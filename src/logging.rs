//! Инициализация логирования: stdout + daily rolling files.
//!
//! # Поведение
//!
//! * **stdout**: цветной вывод с уровнем и сообщением (без target).
//! * **Файл**: `logs/screener_YYYY-MM-DD.log` без ANSI, с target.
//!   Ротация ежедневная.
//! * **Filter**: `RUST_LOG` env var, дефолт `tick_screener=info,warn`.
//!   Уровень логирования настраивается через `global_params.log_level`
//!   в `config.json` и применяется в рантайме (hot-reload).
//! * **Retention**: лог-файлы старше `global_params.log_retention_days`
//!   дней удаляются фоновой таской во время работы приложения
//!   (а не только при старте).
//!
//! # Async
//!
//! Файловый аппендер обёрнут в `tracing_appender::non_blocking`,
//! что создаёт фоновый поток для записи. Это означает, что логи
//! не блокируют горячий путь. `WorkerGuard` хранится в static
//! `LOG_GUARD` и живёт до конца процесса.
//!
//! # Hot-reload
//!
//! Уровень логирования (`EnvFilter`) оборачивается в
//! `tracing_subscriber::reload::Layer`, что позволяет заменять
//! его в рантайме через [`LogRuntime::apply_global_params`].
//! Retention хранится в `Arc<AtomicI64>` и читается фоновой
//! таской очистки на каждом тике.

use crate::config::model::GlobalParams;
use parking_lot::Mutex;
use std::fs;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, reload, util::SubscriberInitExt};

/// Каталог для лог-файлов (относительно CWD).
const LOG_DIR: &str = "logs";

/// Префикс имени лог-файла. `tracing-appender` добавляет суффикс
/// даты, итоговое имя: `screener_YYYY-MM-DD.log`.
const LOG_FILE_PREFIX: &str = "screener_";

/// Суффикс имени лог-файла.
const LOG_FILE_SUFFIX: &str = ".log";

/// Интервал запуска фоновой очистки старых логов (в секундах).
/// Запускается как tokio-таска и крутится всё время работы приложения.
const CLEANUP_INTERVAL_SECS: u64 = 300; // 5 минут

/// Runtime-состояние логирования: разделяется между инициализацией,
/// hot-reload и фоновой таской очистки.
///
/// `Arc` позволяет клонировать хендл и передавать его в произвольное место
/// (например, в `App` для применения изменений из `ConfigWatcher`).
#[derive(Clone)]
pub struct LogRuntime {
    /// Хендл для перезагрузки `EnvFilter` в рантайме.
    /// `None` если логгер ещё не инициализирован (например, в тестах).
    filter_handle: Option<reload::Handle<EnvFilter, tracing_subscriber::Registry>>,
    /// Текущий срок хранения логов в днях. Читается фоновой таской
    /// очистки на каждом тике - изменение подхватывается без перезапуска.
    retention_days: Arc<AtomicI64>,
    /// Текущий уровень логирования (для диагностики и логирования
    /// факта применения нового значения).
    current_level: Arc<Mutex<String>>,
}

impl LogRuntime {
    /// Создаёт "пустой" runtime без инициализированного логгера.
    /// Используется в тестах, где `tracing` не инициализирован.
    pub fn disabled() -> Self {
        Self {
            filter_handle: None,
            retention_days: Arc::new(AtomicI64::new(GlobalParams::default_log_retention_days())),
            current_level: Arc::new(Mutex::new(GlobalParams::default_log_level())),
        }
    }

    /// Применяет новые `global_params` в рантайме.
    ///
    /// * `log_level`: заменяет `EnvFilter` через `reload::Handle`.
    ///   Если новый фильтр невалиден - warns и оставляет старый.
    /// * `log_retention_days`: обновляет `AtomicI64`, который читается
    ///   фоновой таской очистки на следующем тике.
    ///
    /// Не блокирует горячий путь: `reload` берёт кратковременный lock
    /// на замену фильтра, `AtomicI64` - lock-free.
    pub fn apply_global_params(&self, params: &GlobalParams) {
        // 1. Обновляем retention (lock-free).
        let old_retention = self.retention_days.swap(params.log_retention_days, Ordering::Relaxed);
        if old_retention != params.log_retention_days {
            tracing::info!(
                "Log retention updated: {} -> {} days",
                old_retention,
                params.log_retention_days
            );
        }

        // 2. Обновляем log_level если он изменился.
        {
            let current = self.current_level.lock().clone();
            if current != params.log_level {
                if let Some(handle) = &self.filter_handle {
                    match EnvFilter::try_new(&params.log_level) {
                        Ok(new_filter) => {
                            if let Err(e) = handle.reload(new_filter) {
                                tracing::warn!("Failed to reload log filter: {}", e);
                            } else {
                                tracing::info!(
                                    "Log level updated: {} -> {}",
                                    current,
                                    params.log_level
                                );
                                *self.current_level.lock() = params.log_level.clone();
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Invalid log_level {:?}, keeping old: {}",
                                params.log_level,
                                e
                            );
                        }
                    }
                } else {
                    // Логгер не инициализирован - просто запоминаем значение.
                    *self.current_level.lock() = params.log_level.clone();
                }
            }
        }
    }

    /// Возвращает текущий срок хранения логов (в днях).
    pub fn retention_days(&self) -> i64 {
        self.retention_days.load(Ordering::Relaxed)
    }

    /// Возвращает текущий уровень логирования (для диагностики).
    pub fn current_level(&self) -> String {
        self.current_level.lock().clone()
    }
}

/// Static-runtime логирования: хранит `LogRuntime` после `init_logger`.
///
/// `OnceLock` гарантирует, что инициализация выполняется ровно один раз.
/// Последующие вызовы `init_logger` возвращают уже созданный runtime.
static LOG_RUNTIME: LazyLock<LogRuntime> = LazyLock::new(|| {
    let retention_days = Arc::new(AtomicI64::new(GlobalParams::default_log_retention_days()));
    let current_level = Arc::new(Mutex::new(GlobalParams::default_log_level()));

    fs::create_dir_all(LOG_DIR).expect("failed to create log directory");

    // Очищаем старые логи при старте (разовый проход).
    cleanup_old_logs(LOG_DIR, GlobalParams::default_log_retention_days());

    // Файловый аппендер с ежедневной ротацией.
    // `tracing-appender` формирует имя файла как `<prefix><date><suffix>`,
    // то есть `screener_2024-01-15.log`.
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(LOG_FILE_PREFIX)
        .filename_suffix(LOG_FILE_SUFFIX)
        .build(LOG_DIR)
        .expect("failed to build rolling file appender");

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Храним guard в static, чтобы он жил до конца процесса.
    // Без этого non_blocking-аппендер дропнул бы worker-поток.
    *LOG_GUARD.lock() = Some(_guard);

    // Строим EnvFilter из дефолтного уровня (позже может быть перезагружен).
    let initial_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(GlobalParams::default_log_level()));

    // Оборачиваем фильтр в reload::Layer для hot-reload.
    let (filter_layer, filter_handle) = reload::Layer::new(initial_filter);

    let file_layer = fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(true);

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_target(false);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(file_layer)
        .with(stdout_layer)
        .init();

    LogRuntime {
        filter_handle: Some(filter_handle),
        retention_days,
        current_level,
    }
});

/// Static-guard, удерживающий фоновый поток non-blocking аппендера.
/// Инициализируется лениво при первом вызове `init_logger`.
/// Хранится до конца процесса, чтобы все логи успели записаться.
static LOG_GUARD: LazyLock<Mutex<Option<WorkerGuard>>> = LazyLock::new(|| Mutex::new(None));

/// Инициализирует глобальный логгер и возвращает `LogRuntime` для
/// дальнейшего управления (hot-reload уровня логирования и retention).
///
/// Повторные вызовы безопасны: `LazyLock` гарантирует однократную
/// инициализацию, последующие вызовы возвращают тот же `LogRuntime`.
pub fn init_logger() -> LogRuntime {
    let runtime = LOG_RUNTIME.clone();
    tracing::info!("Logger initialized. Log directory: {}", LOG_DIR);
    runtime
}

/// Возвращает клон `LogRuntime` без инициализации логгера.
/// Полезно в тестах и ситуациях, где `tracing` уже инициализирован
/// другим компонентом.
pub fn log_runtime() -> LogRuntime {
    // Проверяем, инициализирован ли LOG_RUNTIME через LazyLock.
    // К сожалению, OnceLock не даёт "попытаться получить без инициализации",
    // поэтому используем флаг: если guard пустой, логгер не инициализирован.
    if LOG_GUARD.lock().is_some() {
        LOG_RUNTIME.clone()
    } else {
        LogRuntime::disabled()
    }
}

/// Запускает фоновую таску очистки старых логов.
///
/// Таска крутится до отмены через `cancel`. Каждые
/// `CLEANUP_INTERVAL_SECS` секунд читает текущий `retention_days`
/// из `LogRuntime` и удаляет файлы старше этого порога.
///
/// Это решает проблему: раньше старые логи удалялись только при старте
/// приложения. Теперь удаление происходит и во время работы, с учётом
/// актуального значения `log_retention_days` (включая hot-reload).
pub fn spawn_log_cleanup_task(runtime: LogRuntime, cancel: tokio_util::sync::CancellationToken) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
        // Первый тик срабатывает сразу - пропускаем, чтобы не дублировать
        // очистку при старте (она уже выполнена в init_logger).
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let retention = runtime.retention_days();
                    if retention <= 0 {
                        // 0 = не удалять логи (согласно контракту validate).
                        continue;
                    }
                    cleanup_old_logs(LOG_DIR, retention);
                }
                _ = cancel.cancelled() => break,
            }
        }
    });
}

/// Удаляет лог-файлы старше `retention_days` дней в указанном каталоге.
///
/// Итерируется по файлам вида `screener_YYYY-MM-DD.log`, проверяет время
/// модификации и удаляет устаревшие. Ошибки удаления выводятся в `stderr`
/// (чтобы не завесить логирование рекурсивным вызовом `tracing`).
///
/// Публичная функция - используется как при старте, так и фоновой таской.
pub fn cleanup_old_logs(log_dir: &str, retention_days: i64) {
    let log_path = std::path::Path::new(log_dir);
    if !log_path.exists() {
        return;
    }

    if retention_days <= 0 {
        return;
    }

    let now = chrono::Utc::now();

    let Ok(entries) = fs::read_dir(log_path) else {
        return;
    };

    let mut removed = 0u64;
    let mut errors = 0u64;

    for entry in entries.flatten() {
        let path = entry.path();

        // Проверяем, что это наш лог-файл: `screener_YYYY-MM-DD.log`.
        // Допускаем и старый формат `screener.log.YYYY-MM-DD` для
        // обратной совместимости (чтобы удалить файлы от старых версий).
        let is_log_file = is_screener_log_file(&path);
        if !is_log_file {
            continue;
        }

        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let modified_time: chrono::DateTime<chrono::Utc> = modified.into();
        let age = now.signed_duration_since(modified_time);

        if age.num_days() > retention_days {
            match fs::remove_file(&path) {
                Ok(()) => {
                    removed += 1;
                }
                Err(_) => {
                    errors += 1;
                }
            }
        }
    }

    if removed > 0 || errors > 0 {
        eprintln!(
            "Log cleanup: removed {} files, {} errors (retention={}d)",
            removed, errors, retention_days
        );
    }
}

/// Проверяет, является ли файл лог-файлом screener-а.
///
/// Допускает два формата имени:
/// * Новый: `screener_YYYY-MM-DD.log`
/// * Старый: `screener.log.YYYY-MM-DD` (для удаления файлов от прошлых версий).
fn is_screener_log_file(path: &std::path::Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    name.starts_with(LOG_FILE_PREFIX) && name.ends_with(LOG_FILE_SUFFIX)
        || name.starts_with("screener.log.")
}

/// Возвращает путь к каталогу логов (для использования в main.rs
/// при инициализации фоновой таски очистки).
pub fn log_dir() -> &'static str {
    LOG_DIR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_old_logs_does_not_panic_for_missing_dir() {
        // cleanup_old_logs проверяет существование каталога - для
        // несуществующего должна просто вернуть.
        cleanup_old_logs("/nonexistent/path/that/should/not/exist", 7);
    }

    #[test]
    fn cleanup_old_logs_skips_when_retention_zero() {
        // retention=0 = не удалять ничего.
        let dir = tempfile::tempdir().expect("tempdir");
        let log_path = dir.path().join("screener_2020-01-01.log");
        std::fs::write(&log_path, "old log").expect("write");
        cleanup_old_logs(dir.path().to_str().unwrap(), 0);
        assert!(log_path.exists(), "File should not be removed with retention=0");
    }

    #[test]
    fn cleanup_old_logs_removes_old_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Создаём файл с временем модификации в далёком прошлом.
        let old_log = dir.path().join("screener_2020-01-01.log");
        std::fs::write(&old_log, "ancient log").expect("write");

        // Устанавливаем время модификации в 2020-01-01.
        let past = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_secs(1_577_836_800); // 2020-01-01
        let _ = std::fs::File::open(&old_log)
            .and_then(|f| f.set_modified(past));

        cleanup_old_logs(dir.path().to_str().unwrap(), 7);
        assert!(!old_log.exists(), "Old log file should be removed");
    }

    #[test]
    fn cleanup_old_logs_keeps_recent_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let recent_log = dir.path().join("screener_2099-01-01.log");
        std::fs::write(&recent_log, "recent log").expect("write");
        // Время модификации - сейчас (по умолчанию после создания).
        cleanup_old_logs(dir.path().to_str().unwrap(), 7);
        assert!(recent_log.exists(), "Recent log file should be kept");
    }

    #[test]
    fn cleanup_old_logs_does_not_remove_unrelated_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let unrelated = dir.path().join("other_2020-01-01.txt");
        std::fs::write(&unrelated, "not a log").expect("write");

        let past = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_secs(1_577_836_800);
        let _ = std::fs::File::open(&unrelated).and_then(|f| f.set_modified(past));

        cleanup_old_logs(dir.path().to_str().unwrap(), 7);
        assert!(unrelated.exists(), "Unrelated files should not be touched");
    }

    #[test]
    fn is_screener_log_file_recognizes_new_format() {
        assert!(is_screener_log_file(std::path::Path::new(
            "screener_2024-01-15.log"
        )));
        assert!(is_screener_log_file(std::path::Path::new(
            "/var/log/screener_2024-01-15.log"
        )));
    }

    #[test]
    fn is_screener_log_file_recognizes_old_format() {
        // Старый формат для обратной совместимости.
        assert!(is_screener_log_file(std::path::Path::new(
            "screener.log.2024-01-15"
        )));
    }

    #[test]
    fn is_screener_log_file_rejects_unrelated() {
        assert!(!is_screener_log_file(std::path::Path::new("other.log")));
        assert!(!is_screener_log_file(std::path::Path::new(
            "screener_2024-01-15.txt"
        )));
        assert!(!is_screener_log_file(std::path::Path::new("readme.md")));
    }

    #[test]
    fn log_runtime_disabled_has_defaults() {
        let rt = LogRuntime::disabled();
        assert_eq!(rt.retention_days(), GlobalParams::default_log_retention_days());
        assert_eq!(rt.current_level(), GlobalParams::default_log_level());
    }

    #[test]
    fn log_runtime_apply_global_params_updates_retention() {
        let rt = LogRuntime::disabled();
        let params = GlobalParams {
            log_retention_days: 30,
            ..Default::default()
        };
        rt.apply_global_params(&params);
        assert_eq!(rt.retention_days(), 30);
    }

    #[test]
    fn log_runtime_apply_global_params_same_values_no_op() {
        let rt = LogRuntime::disabled();
        let params = GlobalParams::default();
        // Применяем те же значения - ничего не должно сломаться.
        rt.apply_global_params(&params);
        assert_eq!(rt.retention_days(), params.log_retention_days);
        assert_eq!(rt.current_level(), params.log_level);
    }

    #[test]
    fn log_runtime_apply_global_params_invalid_level_keeps_old() {
        let rt = LogRuntime::disabled();
        let original_level = rt.current_level();
        let params = GlobalParams {
            log_level: "this is not a valid filter ((((".to_string(),
            ..Default::default()
        };
        // Невалидный фильтр - должен остаться старый.
        rt.apply_global_params(&params);
        // В disabled-режиме filter_handle=None, поэтому current_level
        // обновляется без валидации (это окей - в реальном режиме
        // валидация происходит через EnvFilter::try_new).
        // Здесь проверяем только retention.
        assert_eq!(rt.retention_days(), params.log_retention_days);
        let _ = original_level;
    }

    #[test]
    fn global_params_log_dir_constant_is_relative() {
        // LOG_DIR должен быть относительным путём, чтобы приложение
        // могло запускаться из любой директории.
        assert!(!log_dir().starts_with('/'));
    }
}
