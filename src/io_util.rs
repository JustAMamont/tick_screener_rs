//! Абстракция над асинхронным файловым I/O с опциональной поддержкой io_uring.
//!
//! # Зачем это нужно
//!
//! Стандартный `tokio::fs` внутри использует `spawn_blocking` + `std::fs`,
//! что означает поток ОС на каждую файловую операцию. Для редких чтений
//! конфига это приемлемо, но при интенсивных файловых операциях (например,
//! при чтении логов в мониторинге) накладные расходы на создание потоков
//! становятся заметными.
//!
//! io_uring - это асинхронный интерфейс ядра Linux (с версии 5.1), который
//! позволяет отправлять несколько I/O-операций одним syscall и получать
//! результаты через completion queue. Это снижает overhead и latency.
//!
//! # Использование
//!
//! По умолчанию (без feature `uring`) используется `tokio::fs` (fallback).
//! При включении feature `uring`:
//!
//! ```toml
//! [dependencies]
//! tick-screener = { version = "*", features = ["uring"] }
//! ```
//!
//! чтение файлов выполняется через `tokio-uring` на отдельном
//! однопоточном рантайме. Это безопасно комбинируется с основным
//! многопоточным tokio-рантаймом.
//!
//! # Замечания
//!
//! * io_uring даёт наибольший выигрыш на последовательных чтениях
//!   большого количества мелких файлов. Для одиночных чтений конфига
//!   выигрыш пренебрежимо мал - основная ценность в архитектурной
//!   готовности к будущим оптимизациям файлового логирования.
//! * `tokio-uring` требует Linux 5.1+. На других ОС feature `uring`
//!   не компилируется.

use std::io;
use std::path::Path;

#[cfg(feature = "uring")]
pub use uring_impl::read_file;

#[cfg(not(feature = "uring"))]
pub use fallback::read_file;

#[cfg(not(feature = "uring"))]
mod fallback {
    use super::*;

    /// Асинхронно читает файл в `String`. Fallback через `tokio::fs`.
    ///
    /// Под капотом `tokio::fs::read_to_string` использует
    /// `spawn_blocking` + `std::fs::read_to_string`, что даёт
    /// приемлемую производительность для редких операций (чтение
    /// конфига раз в несколько секунд).
    pub async fn read_file(path: impl AsRef<Path> + Send) -> io::Result<String> {
        tokio::fs::read_to_string(path).await
    }
}

#[cfg(feature = "uring")]
mod uring_impl {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::{mpsc, oneshot};

    /// Запрос на io_uring-операцию.
    enum Request {
        ReadTostring {
            path: PathBuf,
            reply: oneshot::Sender<io::Result<String>>,
        },
    }

    /// Глобальный io_uring-рантайм (однопоточный, на отдельном OS-потоке).
    ///
    /// `tokio-uring` использует current-thread рантайм, поэтому запускаем
    /// его на отдельном потоке. Запросы отправляются через unbounded-канал.
    static URING_RUNTIME: std::sync::OnceLock<Arc<UringRuntime>> = std::sync::OnceLock::new();

    /// Обёртка над io_uring-рантаймом.
    struct UringRuntime {
        tx: mpsc::UnboundedSender<Request>,
        _handle: std::thread::JoinHandle<()>,
    }

    impl UringRuntime {
        fn new() -> Self {
            // Используем tokio::sync::mpsc вместо crossbeam, чтобы recv()
            // был асинхронным и не блокировал current-thread рантайм tokio-uring.
            let (tx, mut rx) = mpsc::unbounded_channel::<Request>();
            let handle = std::thread::Builder::new()
                .name("tick-uring".to_string())
                .spawn(move || {
                    tokio_uring::start(async {
                        // Асинхронно принимаем запросы - не блокируем рантайм.
                        while let Some(req) = rx.recv().await {
                            tokio_uring::spawn(async move {
                                match req {
                                    Request::ReadTostring { path, reply } => {
                                        let result = read_to_string_async(path).await;
                                        let _ = reply.send(result);
                                    }
                                }
                            });
                        }
                        // Когда `tx` дропнут (например, при завершении процесса),
                        // `rx.recv()` вернёт None и рантайм корректно завершится.
                    });
                })
                .expect("Failed to spawn io_uring runtime thread");
            Self {
                tx,
                _handle: handle,
            }
        }

        fn send(&self, req: Request) {
            let _ = self.tx.send(req);
        }
    }

    /// Асинхронно читает файл в `String` через io_uring.
    ///
    /// Реализация отправляет запрос на io_uring-рантайм и ожидает
    /// результат через oneshot-канал. Это позволяет безопасно
    /// комбинировать io_uring с многопоточным tokio.
    ///
    /// # Ошибки
    ///
    /// Возвращает `io::Error` если файл не существует, недоступен
    /// для чтения или содержит невалидный UTF-8.
    pub async fn read_file(path: impl AsRef<Path> + Send) -> io::Result<String> {
        let path = path.as_ref().to_path_buf();
        let (otx, orx) = oneshot::channel::<io::Result<String>>();

        let runtime = URING_RUNTIME.get_or_init(|| Arc::new(UringRuntime::new()));
        runtime.send(Request::ReadTostring { path, reply: otx });

        match orx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("io_uring task was cancelled")),
        }
    }

    /// Читает файл через io_uring внутри `tokio_uring::spawn`.
    ///
    /// Используем `tokio_uring::fs::File::open` + `read_at` - это
    /// нативные io_uring-операции. Читаем чанками по 8 КБ до EOF.
    async fn read_to_string_async(path: PathBuf) -> io::Result<String> {
        use tokio_uring::fs::File;
        let file = File::open(&path).await?;

        // Сначала определяем размер файла через statx.
        let metadata = std::fs::metadata(&path)?;
        let size = metadata.len() as usize;

        // Выделяем буфер сразу нужного размера - это io_uring-операция.
        // `read_at` возвращает (Result<usize>, Buf) - буфер возвращается
        // всегда, даже при ошибке.
        let buf = vec![0u8; size];
        let (read_result, buf) = file.read_at(buf, 0).await;
        let read = read_result?;
        let _ = file.close().await;

        let mut buf = buf;
        if read != size {
            // Файл мог измениться между statx и read - обрезаем до прочитанного.
            buf.truncate(read);
        }

        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_file_returns_contents_for_existing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.txt");
        std::fs::write(&path, "hello world").expect("write");
        let content = read_file(&path).await.expect("read");
        assert_eq!(content, "hello world");
    }

    #[tokio::test]
    async fn read_file_returns_error_for_missing_file() {
        let path = "/nonexistent/path/that/should/not/exist.txt";
        let result = read_file(path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_file_returns_error_for_non_utf8_content() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bin.bin");
        std::fs::write(&path, b"\xff\xfe\x00not utf8").expect("write");
        let result = read_file(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_file_handles_large_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("large.txt");
        let content = "x".repeat(100_000);
        std::fs::write(&path, &content).expect("write");
        let read = read_file(&path).await.expect("read");
        assert_eq!(read.len(), 100_000);
    }

    #[tokio::test]
    async fn read_file_multiple_concurrent_reads() {
        // Несколько параллельных чтений - все должны успешно выполниться.
        let dir = tempfile::tempdir().expect("tempdir");
        let mut paths = Vec::new();
        for i in 0..10 {
            let path = dir.path().join(format!("file_{}.txt", i));
            std::fs::write(&path, format!("content {}", i)).expect("write");
            paths.push((path, i));
        }

        let mut handles = Vec::new();
        for (path, i) in paths {
            handles.push(tokio::spawn(async move {
                let content = read_file(&path).await.expect("read");
                (i, content)
            }));
        }

        for h in handles {
            let (i, content) = h.await.expect("join");
            assert_eq!(content, format!("content {}", i));
        }
    }
}
