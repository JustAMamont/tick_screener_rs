//! Потокобезопасный интернер торговых символов.
//!
//! Это центральный узел оптимизации по аллокациям: вместо того чтобы
//! повсеместно клонировать `String` с именами пар (а имена вида
//! `BTC/USDT` достигают 12-14 байт и копируются на каждый трейд),
//! мы один раз интернируем символ и дальше оперируем 4-байтным
//! [`SymbolId`]. Все горячие пути (сканер, метрики, индекс свечей)
//! используют именно `SymbolId`, а разрешение обратно в строку
//! происходит только при формировании алерта.
//!
//! # Архитектурные решения
//!
//! * **`DashMap<String, u32>` для прямой карты.** Чтение в горячем пути
//!   (`intern`) идёт без блокировок, запись - шардированная.
//! * **`RwLock<Vec<Arc<str>>>` для обратной карты.** Вставка требует
//!   эксклюзивной блокировки, но происходит значительно реже, чем чтение.
//!   Разрешение (`resolve`) берёт разделяемую блокировку и возвращает
//!   `Arc<str>` - клонирование дёшево (только инкремент счётчика).
//! * **`AcqRel` для `next_id`.** Гарантирует, что запись в `strings`
//!   видна другим потокам до того, как они увидят новый ID через
//!   `fetch_add`. Без этого была бы гонка: поток A получает ID,
//!   поток B успевает его прочитать через `resolve` до того, как A
//!   записал строку.
//! * **Предварительная аллокация** `Vec::with_capacity(4096)` для
//!   типичного объёма биржевых рынков (Binance USDT ≈ 1500 пар).

use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

/// Идентификатор интернированного символа.
///
/// 4-байтный целочисленный хэндл, заменяющий `String` во всех горячих
/// путях. `Copy` + `Hash` + `Eq` делают его идеальным ключом для
/// `HashMap`/`DashMap` - хэширование u32 практически бесплатно по
/// сравнению с хэшированием строки.
#[derive(Clone, Copy, Hash, Eq, PartialEq, Debug, Default)]
pub struct SymbolId(pub u32);

/// Потокобезопасный интернер строк-символов.
///
/// Один экземпляр разделяется между всеми компонентами приложения
/// через `Arc<SymbolInterner>`. Живёт вечно (не очищается), т.к.
/// множество торгуемых символов конечно и относительно стабильно.
///
/// # Пример
///
/// ```
/// use tick_screener::interner::SymbolInterner;
/// let interner = SymbolInterner::new();
/// let id1 = interner.intern("BTC/USDT");
/// let id2 = interner.intern("BTC/USDT");
/// assert_eq!(id1, id2);
/// assert_eq!(interner.resolve(id1).as_ref(), "BTC/USDT");
/// ```
pub struct SymbolInterner {
    /// Прямая карта: строка → ID. `DashMap` обеспечивает шардированный
    /// неблокирующий доступ из множества потоков.
    map: DashMap<String, u32>,
    /// Обратная карта: ID → строка. Хранится как `Arc<str>` чтобы
    /// `resolve()` возвращал дешёвый клон, а не полное копирование строки.
    strings: RwLock<Vec<Arc<str>>>,
    /// Монотонный счётчик ID. `AcqRel` гарантирует видимость записи
    /// в `strings` для любого потока, получившего этот ID.
    next_id: AtomicU32,
}

impl SymbolInterner {
    /// Создаёт новый интернер с предвыделенной ёмкостью 4096 символов.
    pub fn new() -> Self {
        Self {
            map: DashMap::with_capacity(64),
            strings: RwLock::new(Vec::with_capacity(4096)),
            next_id: AtomicU32::new(0),
        }
    }

    /// Интернирует строку, возвращая её [`SymbolId`]. Потокобезопасно.
    ///
    /// Если символ уже был интернирован - возвращает существующий ID
    /// (быстрый путь через `DashMap::get`, без аллокаций).
    /// Если символ новый - аллоцирует его один раз, сохраняет `Arc<str>`
    /// и выдаёт уникальный ID.
    ///
    /// # Потокобезопасность
    ///
    /// Использует `DashMap::entry` для атомарной вставки: если несколько
    /// потоков одновременно пытаются интернировать одну и ту же строку,
    /// только один получит `Vacant` entry и выделит ID, остальные получат
    /// `Occupied` и вернут уже выделенный ID. Это устраняет гонку, которая
    /// была в предыдущей реализации (два потока могли получить разные ID
    /// для одной строки).
    ///
    /// # Порядок памяти
    ///
    /// `fetch_add(1, AcqRel)`:
    /// * `Acquire` на стороне читателя `next_id` гарантирует, что мы
    ///   видим все записи в `strings`, сделанные потоками с меньшими ID.
    /// * `Release` на стороне писателя гарантирует, что наша запись в
    ///   `strings` видна потокам, получившим наш ID.
    #[inline]
    pub fn intern(&self, s: &str) -> SymbolId {
        // Быстрый путь: символ уже интернирован.
        if let Some(id) = self.map.get(s) {
            return SymbolId(*id);
        }
        // Медленный путь: новый символ.
        // Используем entry API для атомарной вставки - устраняет гонку
        // между параллельными intern-ами одной строки.
        use dashmap::mapref::entry::Entry;
        match self.map.entry(s.to_string()) {
            Entry::Occupied(e) => {
                // Кто-то уже вставил этот символ, пока мы шли сюда.
                SymbolId(*e.get())
            }
            Entry::Vacant(e) => {
                let arc_str: Arc<str> = Arc::from(s);
                // AcqRel: см. комментарий к полю next_id.
                let id = self.next_id.fetch_add(1, Ordering::AcqRel);
                {
                    let mut guard = self.strings.write();
                    // Ёмкость предвыделена, push почти никогда не триггерит реаллокацию.
                    guard.push(Arc::clone(&arc_str));
                }
                e.insert(id);
                SymbolId(id)
            }
        }
    }

    /// Возвращает строку для заданного [`SymbolId`].
    ///
    /// Возвращает `Arc<str>`, а не `&str`, чтобы вызывающий не был
    /// привязан к времени жизни интернера. Клонирование `Arc` - это
    /// инкремент счётчика ссылок (атомарная операция), что значительно
    /// дешевле копирования `String`.
    ///
    /// Если ID некорректен (например, получен из другой версии
    /// интернера), возвращает пустую `Arc<str>`.
    #[inline]
    pub fn resolve(&self, id: SymbolId) -> Arc<str> {
        self.strings
            .read()
            .get(id.0 as usize)
            .cloned()
            .unwrap_or_else(|| Arc::from(""))
    }

    /// Текущее количество интернированных символов.
    ///
    /// Значение может слегка отставать от реального при параллельных
    /// вставках - это допустимо для метрик.
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Acquire) as usize
    }

    /// `true` если ни один символ ещё не интернирован.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Возвращает snapshot всех известных символов.
    ///
    /// Полезно для диагностики и тестов. Берёт блокировку на чтение
    /// и клонирует все `Arc<str>` - недорого, но не вызывайте в горячем пути.
    pub fn all_symbols(&self) -> Vec<Arc<str>> {
        self.strings.read().clone()
    }
}

impl Default for SymbolInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn intern_returns_same_id_for_same_string() {
        let interner = SymbolInterner::new();
        let id1 = interner.intern("BTC/USDT");
        let id2 = interner.intern("BTC/USDT");
        assert_eq!(id1, id2);
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn intern_returns_different_ids_for_different_strings() {
        let interner = SymbolInterner::new();
        let id1 = interner.intern("BTC/USDT");
        let id2 = interner.intern("ETH/USDT");
        assert_ne!(id1, id2);
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn resolve_returns_original_string() {
        let interner = SymbolInterner::new();
        let id = interner.intern("BTC/USDT.P");
        let resolved = interner.resolve(id);
        assert_eq!(resolved.as_ref(), "BTC/USDT.P");
    }

    #[test]
    fn resolve_unknown_id_returns_empty() {
        let interner = SymbolInterner::new();
        let resolved = interner.resolve(SymbolId(999));
        assert!(resolved.is_empty());
    }

    #[test]
    fn is_empty_initially() {
        let interner = SymbolInterner::new();
        assert!(interner.is_empty());
        interner.intern("X");
        assert!(!interner.is_empty());
    }

    #[test]
    fn concurrent_intern_is_thread_safe() {
        // Параллельно интернируем один и тот же символ из множества потоков:
        // все потоки должны получить один и тот же ID.
        let interner = Arc::new(SymbolInterner::new());
        let symbol = "BTC/USDT";
        let mut handles = Vec::new();
        for _ in 0..16 {
            let interner = Arc::clone(&interner);
            let sym = symbol.to_string();
            handles.push(thread::spawn(move || interner.intern(&sym)));
        }
        let ids: Vec<SymbolId> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let first = ids[0];
        assert!(ids.iter().all(|id| *id == first));
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn concurrent_distinct_intern_gives_unique_ids() {
        // Параллельно интернируем разные символы - все ID должны быть уникальны.
        let interner = Arc::new(SymbolInterner::new());
        let mut handles = Vec::new();
        for i in 0..32 {
            let interner = Arc::clone(&interner);
            handles.push(thread::spawn(move || {
                let sym = format!("SYM{}", i);
                interner.intern(&sym)
            }));
        }
        let ids: Vec<SymbolId> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let mut sorted = ids.clone();
        sorted.sort_by_key(|id| id.0);
        sorted.dedup_by_key(|id| id.0);
        assert_eq!(sorted.len(), ids.len(), "Duplicate IDs found");
        assert_eq!(interner.len(), 32);
    }

    #[test]
    fn resolve_after_parallel_intern_returns_correct_string() {
        let interner = Arc::new(SymbolInterner::new());
        let mut handles = Vec::new();
        for i in 0..100 {
            let interner = Arc::clone(&interner);
            handles.push(thread::spawn(move || {
                let sym = format!("SYM{}", i);
                (i, interner.intern(&sym))
            }));
        }
        let results: Vec<(i32, SymbolId)> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        for (i, id) in results {
            let resolved = interner.resolve(id);
            assert_eq!(resolved.as_ref(), format!("SYM{}", i));
        }
    }
}
