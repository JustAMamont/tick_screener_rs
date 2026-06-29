# tick-screener

[![Rust](https://img.shields.io/badge/Rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![Edition](https://img.shields.io/badge/Edition-2024-blue.svg)](https://doc.rust-lang.org/edition-guide/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green.svg)](#лицензия)
[![Tests](https://img.shields.io/badge/tests-127%20passing-brightgreen.svg)](#тестирование)

**Высокопроизводительный сканер аномалий торговых пар** для криптовалютных
бирж. Подписывается на WebSocket-стримы сделок (trades) с Binance, Bybit,
Kucoin, Bitget, Gate, MEXC, агрегирует их в свечи заданного таймфрейма и
отправляет Telegram-уведомление при превышении порогов по объёму и
изменению цены.

## Содержание

- [Возможности](#возможности)
- [Архитектура](#архитектура)
- [Оптимизации](#оптимизации)
  - [Аллокация символов](#аллокация-символов)
  - [Атомарные операции](#атомарные-операции)
  - [Потоковая безопасность](#потоковая-безопасность)
  - [io_uring](#io_uring)
- [Установка](#установка)
- [Конфигурация](#конфигурация)
- [Запуск](#запуск)
- [Тестирование](#тестирование)
- [Бенчмарки](#бенчмарки)
- [Структура проекта](#структура-проекта)
- [Расширение](#расширение)
- [Устранение неисправностей](#устранение-неисправностей)
- [Лицензия](#лицензия)

---

## Возможности

- **6 бирж × 2 типа рынка (spot/perp)** - Binance, Bybit, Kucoin, Bitget, Gate, MEXC (с возможностью добавления других)
- **Hot-reload конфигурации** - изменения применяются без перезапуска,
  с точечным diff (добавление/удаление/модификация сканеров +
  `global_params`: log_level, log_retention_days)
- **Шаринг фидов** - несколько сканеров на одной бирже используют
  один WebSocket-стрим, экономя подключения
- **Rate limit aware** - Telegram-бот уважает `429 retry_after`,
  буферизует алерты и отправляет раздельными сообщениями: листинги
  (с закреплением) отдельно от volatility-алертов
- **Network resilience** - экспоненциальный backoff при сетевых сбоях,
  автоматическая проверка восстановления через `getMe`
- **Listing detection** - отдельная фоновый таск обнаруживает новые
  пары на бирже и шлёт мгновенный алерт
- **Параллельная обработка** - `rayon` для CPU-bound работы (агрегация
  свечей по символам), `tokio` для I/O
- **Thread-local метрики** - запись без атомарных конфликтов на горячем
  пути, агрегация раз в 60 секунд
- **Опциональный io_uring** - feature-флаг `uring` активирует io_uring
  для файлового I/O (требует Linux 5.1+)
- **Runtime-configurable logging** - уровень логирования и срок
  хранения логов настраиваются в `global_params` и применяются в
  рантайме (`reload::Layer` для фильтра, `AtomicI64` для retention,
  фоновая таска очистки каждые 5 минут)
- **Daily log rotation** - автоматическая ротация лог-файлов
  (`screener_YYYY-MM-DD.log`) с фоновой очисткой устаревших

---

## Архитектура

```text
    ┌────────────────────────────────────────┐
    │        config.json (hot-reload)        │
    └────────────┬───────────────────────────┘
                 │ watch
                 ▼
    ┌────────────────────────────────────────┐
    │              ConfigWatcher             │ broadcast
    └────────┬───────────────────────────────┘
             ▼
    ┌────────────────────────────────────────────────┐
    │                     App                        │
    │  ┌──────────────────────────────────────────┐  │
    │  │ ConfigRegistry (snapshot + diff)         │  │
    │  └──────────────────────────────────────────┘  │
    └────────┬───────────────────────────────────────┘
             │ build_topology / apply_diff
             ▼
    ┌───────────────────────────────────────────────────────┐
    │                FeedManager                            │
    │  ┌───────────────┐  ┌───────────────┐                 │
    │  │ SharedFeed    │  │ SharedFeed    │  ... per (ex,m) │
    │  │ (binance,spot)│  │ (bybit,perp)  │                 │
    │  └──────┬────────┘  └──────┬────────┘                 │
    │         │ broadcast        │ broadcast                │
    └─────────┼──────────────────┼──────────────────────────┘
              │                  │
              ▼                  ▼
    ┌────────────────┐   ┌────────────────┐
    │ TradeProcessor │   │ TradeProcessor │   ... per scanner
    │  (binance_s)   │   │  (bybit_p)     │
    └────────┬───────┘   └────────┬───────┘
             │                    │
             ▼                    ▼
    ┌────────────────────────────────────────┐
    │           ScannerCore (rayon)          │
    │  ┌──────────────────────────────────┐  │
    │  │ DashMap<SymbolId, Candle>        │  │
    │  │ ThreadLocal<Metrics>             │  │
    │  └──────────────────────────────────┘  │
    └────────────────┬───────────────────────┘
                     │ alerts (mpsc)
                     ▼
    ┌────────────────────────────────────────┐
    │            AlertRouter                 │
    └────────────────┬───────────────────────┘
                     │
                     ▼
    ┌────────────────────────────────────────┐
    │  BotPool → TgBot → Telegram API        │
    └────────────────────────────────────────┘
```

### Поток данных

1. `FeedManager` создаёт один `SharedFeed` на каждую пару
   `(exchange, market_type)`, запускает WebSocket-таски с батчингом
   символов.
2. Каждый сканер (`ScannerRuntimeConfig`) создаёт свой
   `TradeProcessor`, который подписывается на нужный `SharedFeed`.
3. `TradeProcessor` батчит трейды (до 2048 за раз), фильтрует по
   котировке и blacklist-у, передаёт в `ScannerCore::process_trades`.
4. `ScannerCore` интернирует символы, группирует трейды по `SymbolId`,
   обрабатывает каждый символ параллельно (rayon), генерирует алерты
   при закрытии свечи с превышением порогов.
5. Алерты идут через `mpsc`-канал в `AlertRouter`, который находит
   Telegram-настройки сканера и отправляет сообщение через
   `BotPool` → `TgBot`.

---

## Оптимизации

### Аллокация символов

**Проблема**: имена пар вроде `BTC/USDT.P` - 10-12 байт, клонируются
на каждый трейде в горячем пути. При 10 000 трейдов/сек это
~120 КБ/сек аллокаций - узкое место для GC-less языка.

**Решение** ([`src/interner/mod.rs`](src/interner/mod.rs)):

- `SymbolInterner` один раз интернирует строку в `SymbolId(u32)` —
  4 байта, `Copy`, идеальный `Hash`/`Eq`.
- Все горячие пути (`ScannerCore::candles`, `last_alert_ts`,
  `last_active`) используют `SymbolId` как ключ — `HashMap<SymbolId, _>`
  хэширует `u32` практически бесплатно.
- Строки хранятся как `Arc<str>` в `Vec<Arc<str>>` — `resolve()`
  возвращает дешёвый клон `Arc` (только инкремент счётчика).
- Предварительная аллокация `Vec::with_capacity(4096)` покрывает
  типичный объём биржевых рынков (Binance USDT ≈ 1500 пар).
- `DashMap` для прямой карты — шардированный неблокирующий доступ.

**Эффект**: на батче 1000 символов × 10 трейдов — ~0 аллокаций
после первого интернирования. До оптимизации — ~10 000 аллокаций
строк.

### Атомарные операции

**Проблема**: наивная реализация использовала бы `AtomicU64` для
каждого счётчика метрик, что приводит к cache-line bouncing между
ядрами (десятки наносекунд на конфликт).

**Решение** ([`src/scanner/metrics.rs`](src/scanner/metrics.rs)):

- `ThreadLocal<SyncCell>` для записи в горячем пути — каждый
  rayon-воркер пишет в свою ячейку, никаких атомиков.
- `AtomicU64` только для глобальных кумулятивных счётчиков
  (`global_trades`, `global_alerts`), обновляются раз в 60 секунд
  при `flush()`.
- `Ordering::Relaxed` для счётчиков — нам не нужна строгая
  синхронизация с другими операциями, только атомарность самого
  сложения.
- `Ordering::AcqRel` для `next_id` в интернере — гарантирует
  видимость записи в `strings` для потоков, получивших новый ID.

**Эффект**: при 1000 параллельных rayon-воркеров — 0 конфликтов
на запись метрик. До оптимизации — каждый `fetch_add` триггерил
cache-line bounce между всеми ядрами.

### Потоковая безопасность

**Архитектура**:

- `ScannerCore` — все поля `DashMap`/`RwLock`/`ThreadLocal`,
  все методы принимают `&self`. Полностью потокобезопасен.
- `SyncCell` (`UnsafeCell<LocalMetrics>`) — `unsafe impl Sync`
  корректен благодаря `ThreadLocal`, который гарантирует
  потоковую изоляцию доступа.
- `DashMap` для `candles`, `last_alert_ts`, `pairlists` —
  шардированная блокировка, разные символы обновляются без
  конфликтов.
- `parking_lot::RwLock` для `last_active` — короткие критические
  секции, low overhead.
- `tokio::sync::RwLock` для `ScannerRuntimeConfig` — допускает
  `.await` внутри критической секции (нужно для hot-reload).
- `BotPool` — `DashMap<String, Arc<TgBot>>`, `Arc<TgBot>` для
  shared ownership между тасками.

**Потенциальные проблемы**:

- `last_alert_ts.get` + `insert` в `process_trades` —
  потенциальная гонка при параллельной обработке одного символа
  (что не происходит в текущем коде, т.к. трейды одного символа
  идут в одну rayon-таску). Даже если бы произошло — худший
  исход: один дубль алерта в пределах миллисекунды, что не
  критично для UX.
- `next_id.fetch_add(1, AcqRel)` + `strings.write()` —
  потенциальная гонка: поток A получает ID, поток B успевает
  запросить `resolve(id)` до того, как A записал строку.
  Решено через `AcqRel`: B увидит запись A благодаря release/
  acquire семантике.

### io_uring

**Опциональная интеграция** через feature-флаг `uring`
([`src/io_util.rs`](src/io_util.rs)):

- **Без feature** (по умолчанию): `tokio::fs::read_to_string`
  использует `spawn_blocking` + `std::fs`. Приемлемо для редких
  чтений конфига.
- **С feature `uring`**: чтение конфига через `tokio-uring`
  на отдельном однопоточном рантайме. Запросы отправляются через
  `tokio::sync::mpsc`, ответы — через `oneshot`.

**Почему отдельный рантайм?**

`tokio-uring` требует current-thread runtime (`tokio_uring::start`),
что несовместимо с многопоточным tokio. Решение: запускаем
io_uring-runtime на отдельном OS-потоке, общаемся через каналы.
Это безопасно и не блокирует основной рантайм.

**Когда включать?**

- Linux 5.1+ (для io_uring syscall)
- Много файловых операций (логирование в файл с высокой частотой)
- Чувствительность к latency файлового I/O

Для типичного использования (чтение конфига раз в несколько секунд)
выигрыш пренебрежимо мал. Feature добавлен как архитектурная
готовность к будущим оптимизациям файлового логирования.

### Hot-reload логирования

**Архитектура** ([`src/logging.rs`](src/logging.rs)):

- `EnvFilter` оборачивается в `tracing_subscriber::reload::Layer` -
  позволяет заменять фильтр в рантайме без пересоздания subscriber-а.
- `log_retention_days` хранится в `Arc<AtomicI64>` - обновление
  lock-free, читается фоновой таской очистки на каждом тике.
- Фоновая таска `spawn_log_cleanup_task` запускается при старте и
  крутится до завершения приложения, удаляя старые логи каждые
  5 минут (а не только при старте, как раньше).

**Не блокирует горячий путь**:

- `apply_global_params` берёт кратковременный lock на замену фильтра
  (микросекунды) и `AtomicI64::swap` для retention (наносекунды).
- Запись логов идёт через `non_blocking` аппендер - отдельный поток,
  горячий путь не ждёт I/O.

**Имена лог-файлов**: `screener_YYYY-MM-DD.log` (вместо старого
`screener.log.YYYY-MM-DD`). Старый формат распознаётся при очистке
для удаления файлов от прошлых версий.

---

## Установка

### Требования

- **Rust 1.85+** (edition 2024)
- **Linux** (тестировалось на Ubuntu 22.04+, Debian 12+)
  - для feature `uring`: ядро 5.1+
- **~500 МБ** диска для сборки (зависимости)
- **~100 МБ** RAM в runtime (зависит от количества отслеживаемых пар)

### Сборка

```bash
# Клонировать репозиторий
git clone https://github.com/JustAMamont/tick_screener_rs
cd tick_screener_rs

# Debug-сборка (быстрая компиляция, медленный runtime)
cargo build

# Release-сборка (медленная компиляция, быстрый runtime)
cargo build --release

# С io_uring
cargo build --release --features uring
```

Бинарник появится в `target/debug/tick-screener` или
`target/release/tick-screener`.

---

## Конфигурация

Конфиг — JSON-объект с двумя полями: `global_params` (настройки уровня
приложения) и `scanners` (массив scan-записей). Путь по умолчанию:
`./config.json` (можно изменить в `main.rs`).

Для обратной совместимости также принимается старый формат — голый
массив scan-записей (тогда `global_params` берётся по умолчанию).

См. [`config.json.example`](config.json.example):

```json
{
    "global_params": {
        "log_level": "tick_screener=info,warn",
        "log_retention_days": 7
    },
    "scanners": [
        {
            "scan": "mexc_fut",
            "blacklist": [],
            "currency_type": "perp",
            "quote": "*USD",
            "alert_settings": {
                "return_limit": 1,
                "volume_limit": 2000,
                "trange": 1,
                "telegram": {
                    "bot_token": "your_bot_token",
                    "chat_id": -100123456789
                },
                "delimiter": ""
            },
            "process_settings": {
                "pairs_batch_size": 1000,
                "launch_delay": 1.0
            }
        }
    ]
}
```

### Поля `global_params`

| Поле | Тип | По умолчанию | Описание |
|------|-----|-------------|----------|
| `log_level` | string | `"tick_screener=info,warn"` | Уровень логирования в формате `tracing` `EnvFilter`. Применяется в рантайме через `reload::Layer` при hot-reload. |
| `log_retention_days` | int | `7` | Сколько дней хранить лог-файлы. `0` = не удалять. Применяется фоновой таской очистки (каждые 5 мин) при hot-reload. |

Оба поля поддерживают hot-reload: изменение в `config.json` применяется
без перезапуска приложения.

### Поля scan-записи

| Поле | Тип | Описание |
|------|-----|----------|
| `scan` | string | ID сканера. Формат: `<exchange>_<market>` (например, `bybit_spot`). Определяет биржу по префиксу. |
| `blacklist` | string[] | Список исключённых символов в unified-формате (`BTC/USDT`). |
| `currency_type` | `"spot"` \| `"perp"` | Тип рынка. |
| `quote` | string | Котировка-фильтр. `*USD` - все стейблкоины (USDT, USDC, BUSD, FDUSD, TUSD, USDP, DAI). `*BTC` - BTC+WBTC. Любое другое - точное совпадение. |
| `alert_settings.return_limit` | float | Порог изменения цены (%) для алерта. |
| `alert_settings.volume_limit` | float | Порог объёма ($) для алерта. |
| `alert_settings.trange` | int | Таймфрейм свечи в секундах. |
| `alert_settings.telegram.bot_token` | string | Токен Telegram-бота (от @BotFather). Пустая строка - алерты отключены. |
| `alert_settings.telegram.chat_id` | int | ID чата для доставки (отрицательный для групп). |
| `alert_settings.delimiter` | string | Разделитель для отображения пары. `""` -> `BTCUSDT`, `"/"` -> `BTC/USDT`. |
| `process_settings.pairs_batch_size` | int | Количество символов в одном WS-SUBSCRIBE. Ограничено лимитами биржи. |
| `process_settings.launch_delay` | float | Задержка между отправкой батчей (сек). |

### Поддерживаемые `scan` префиксы

| Префикс | Биржа |
|---------|-------|
| `binance` | Binance |
| `bybit` | Bybit |
| `kucoin` | Kucoin |
| `bitget` | Bitget |
| `gate` | Gate |
| `mexc` | MEXC |

Пример: `binance_perp`, `bybit_spot`, `kucoin_fut`, `mexc_perp`.

### Hot-reload

При изменении `config.json` файловый вотчер (debounce 200мс, cooldown
2с) перезагружает конфиг. Изменения применяются точечно:

- **Добавлен сканер** → создаётся `ScannerCore` + `TradeProcessor`,
  подписывается на существующий или новый фид.
- **Удалён сканер** → его `TradeProcessor` abort-ится, `ScannerCore`
  удаляется, при необходимости закрывается фид (если нет подписчиков).
- **Изменён сканер** → обновляется `Arc<RwLock<ScannerRuntimeConfig>>`,
  `TradeProcessor` подхватит новые значения на следующем батче.
- **Добавлен/удалён фид** → создаётся/закрывается `SharedFeed`.
- **Изменён `global_params.log_level`** → `EnvFilter` заменяется в
  рантайме через `tracing_subscriber::reload::Layer`. Не блокирует
  горячий путь.
- **Изменён `global_params.log_retention_days`** → `AtomicI64`
  обновляется lock-free, фоновая таска очистки подхватывает новое
  значение на следующем тике (каждые 5 минут).

---

## Запуск

```bash
# 1. Создать config.json из примера
cp config.json.example config.json
# Отредактировать: указать токен бота, chat_id, биржи

# 2. Запустить
./target/release/tick-screener

# С io_uring
./target/release/tick-screener  # собрано с --features uring

# С настраиваемым уровнем логирования
RUST_LOG=debug ./target/release/tick-screener
RUST_LOG=tick_screener=info,warn ./target/release/tick-screener  # дефолт
```

### Логирование

- **stdout**: цветной вывод с уровнем и сообщением.
- **Файл**: `logs/screener_YYYY-MM-DD.log` (ежедневная ротация,
  очистка файлов старше `global_params.log_retention_days` дней
  фоновой таской каждые 5 минут - не только при старте).
- **Filter**: `RUST_LOG` env var при старте, затем
  `global_params.log_level` из `config.json` (hot-reload через
  `tracing_subscriber::reload::Layer`). Дефолт `tick_screener=info,warn`.

### Завершение

`Ctrl+C` инициирует graceful shutdown:

1. Отменяется config-вотчер.
2. Отменяются все фоновые таски (monitor, refresh, reload, log-cleanup).
3. Закрываются все фиды (`shutdown_all`).
4. Abort-ятся все `TradeProcessor`-ы.
5. Дропается `alert_tx`, `AlertRouter` досылает оставшиеся алерты
   (с таймаутом 3 сек).

---

## Тестирование

```bash
# Все тесты
cargo test

# Только библиотечные тесты
cargo test --lib

# С io_uring feature
cargo test --features uring

# Конкретный модуль
cargo test --lib scanner::core
cargo test --lib interner
cargo test --lib config::watcher

# С выводом println! из тестов
cargo test -- --nocapture

# Clippy (без предупреждений)
cargo clippy --all-targets --features uring -- -D warnings
```

### Покрытие тестами

| Модуль | Что тестируется |
|--------|-----------------|
| `interner` | интернирование, разрешение, конкурентный доступ из 16+ потоков (атомарная вставка через `entry` API) |
| `scanner::core` | пустые батчи, blacklist, открытие/закрытие свечей, пороги, подавление дублей, cleanup, параллельная обработка 500 символов |
| `scanner::metrics` | инкремент счётчиков, batch-операции, flush + reset, параллельная запись из 1000 rayon-воркеров |
| `scanner::processor` | хэширование blacklist |
| `config::watcher` | парсинг object/legacy-array форматов, `global_params` (defaults, partial, validation), разрешение алиасов котировок, обработка неизвестных бирж, чтение файлов |
| `config::registry` | diff-логика: добавление/удаление/модификация сканеров, добавление/удаление фидов, детекция изменений `global_params` (log_level, log_retention_days), комбинированные изменения |
| `feed::manager` | создание пустого менеджера, get_pairlist, compute_batch_size |
| `alert::router` | обработка алертов, пропуск пустых токенов, неизвестные scanner_id |
| `alert::telegram` | пул ботов (дедупликация, cleanup), extract_retry_after, is_network_error, compute_network_cooldown |
| `exchanges::normalized` | from_scan_name (включая регрессионный тест для bybit->Bybit), as_str, Display, Hash/Eq |
| `exchanges::connector` | factory для всех бирж |
| `io_util` | чтение существующих/несуществующих файлов, не-UTF8, большие файлы, параллельные чтения |
| `logging` | cleanup_old_logs (несуществующая директория, retention=0, удаление старых файлов, сохранение свежих, нерелевантные файлы), is_screener_log_file (новый/старый/нерелевантный форматы), LogRuntime (disabled, apply_global_params, обновление retention, no-op при тех же значениях, невалидный log_level) |

Всего **127 unit-тестов** + 1 doctest, все проходят за <1 сек.

---

## Бенчмарки

```bash
cargo bench --features bench
```

Доступные бенчмарки:

- `benches/interner.rs`:
  - `intern_single_new_symbol` — интернирование нового символа
  - `intern_existing_symbol` — повторное интернирование (быстрый путь)
  - `resolve` — разрешение SymbolId → Arc<str>
  - `concurrent_intern_distinct` — параллельное интернирование из 8 потоков

- `benches/scanner_core.rs`:
  - `process_50_symbols_1_trade_each` — небольшой батч
  - `process_1000_symbols_10_trades_each` — большой батч (10 000 трейдов)

Результаты сравнивайте между коммитами с помощью
[criterion](https://bheisler.github.io/criterion.rs/book/user_guide/html_reports.html).

---

## Структура проекта

```
tick_screener_rs/
├── Cargo.toml              # манифест пакета + features (uring, bench)
├── Cargo.lock              # зафиксированные версии зависимостей
├── config.json.example     # пример конфигурации
├── README.md               # этот файл
├── src/
│   ├── main.rs             # точка входа, App, hot-reload loop
│   ├── lib.rs              # публичный API крейта
│   ├── logging.rs          # tracing + file rotation + hot-reload (LogRuntime, cleanup task)
│   ├── io_util.rs          # io_uring абстракция (feature-gated)
│   ├── interner/
│   │   └── mod.rs          # SymbolInterner (Arc<str>, DashMap)
│   ├── config/
│   │   ├── mod.rs
│   │   ├── model.rs        # типы конфига (RawConfig, ScannerRuntimeConfig, FeedKey)
│   │   ├── watcher.rs      # файловый вотчер + парсинг JSON
│   │   └── registry.rs     # текущий снэпшот + diff
│   ├── exchanges/
│   │   ├── mod.rs          # rand_int helper
│   │   ├── normalized.rs   # NormalizedTrade, Exchange, MarketInfo
│   │   ├── connector.rs    # ExchangeConnector trait + factory
│   │   ├── binance.rs      # BinanceConnector (spot + perp)
│   │   ├── bybit.rs        # BybitConnector
│   │   ├── kucoin.rs       # KucoinConnector
│   │   ├── bitget.rs       # BitgetConnector
│   │   ├── gate.rs         # GateConnector
│   │   └── mexc.rs         # MexcConnector
│   ├── feed/
│   │   ├── mod.rs
│   │   └── manager.rs      # FeedManager (SharedFeed per exchange+market)
│   ├── scanner/
│   │   ├── mod.rs
│   │   ├── core.rs         # ScannerCore (rayon, thread-local metrics)
│   │   ├── metrics.rs      # Metrics (ThreadLocal + AtomicU64)
│   │   └── processor.rs    # TradeProcessor (broadcast → core → mpsc)
│   └── alert/
│       ├── mod.rs
│       ├── router.rs       # AlertRouter (mpsc → TgBot)
│       └── telegram.rs     # TgBot + BotPool (rate limit, buffering)
└── benches/
    ├── interner.rs         # бенчмарки SymbolInterner
    └── scanner_core.rs     # бенчмарки ScannerCore
```

---

## Расширение

### Добавление новой биржи

1. Создать `src/exchanges/<name>.rs` с `<Name>Connector`,
   реализующим `ExchangeConnector`.
2. Добавить вариант в `Exchange` enum в `normalized.rs`.
3. Добавить матч в `Exchange::from_scan_name` и `Exchange::as_str`.
4. Добавить фабрику в `get_connector_factory` в `connector.rs`.
5. Добавить `pub mod <name>;` в `exchanges/mod.rs`.
6. Написать тесты на парсинг символов и `from_scan_name`.

### Добавление нового типа алерта

1. Расширить `Alert` struct в `scanner/core.rs` (или создать новый
   тип, если структура сильно отличается).
2. Добавить генерацию в `ScannerCore::process_trades` (или в новой
   фоновой таске, как `run_pairlist_refresher` для listings).
3. Обновить `AlertRouter` если нужно разное поведение по `alert_type`.

### Изменение таймфрейма

`trange` (в секундах) настраивается в `alert_settings` конфига.
Применяется на следующем батче после hot-reload — без перезапуска
приложения. Свечи в памяти сохраняются, но их границы могут
сдвинуться, что приведёт к досрочному закрытию (с проверкой порогов).

---

## Устранение неисправностей

### `Failed to load initial config`

- Проверьте что `config.json` существует в CWD.
- Проверьте что JSON валидный (`cat config.json | jq .`).
- Проверьте что все обязательные поля присутствуют (см. [Конфигурация](#конфигурация)).

### `Unknown exchange in scan name: <name>`

Префикс `scan` не распознан. Поддерживаемые: `binance`, `bybit`,
`kucoin`, `bitget`, `gate`, `mexc`. Регистр не важен.

### `Failed to send alert: channel closed`

`AlertRouter` остановился (вероятно, при graceful shutdown).
Сообщение: `alert_tx` дропнут, `alert_rx` закрыт. Не ошибка в
нормальном сценарии shutdown.

### TG 429 (rate limit)

Telegram-бот получил 429. Это нормально — бот автоматически
буферизует алерты на время `retry_after` и отправляет их одним
сообщением после истечения. Если 429 частые — уменьшите
чувствительность сканеров (повысьте `return_limit` / `volume_limit`).

### TG network error

Сетевые проблемы между вашим сервером и `api.telegram.org`. Бот
использует экспоненциальный backoff (10с → 20с → 40с → ... → 300с).
Автоматическая проверка восстановления через `getMe`.

### WebSocket disconnects / reconnects

Нормальное поведение — биржи периодически сбрасывают соединения
(Binance каждые 24 часа принудительно). Коннекторы автоматически
переподключаются с экспоненциальным backoff.

### Высокая память

- Уменьшите `pairs_batch_size` (меньше одновременных подписок).
- Проверьте `blacklist` — исключите неинтересующие пары.
- Используйте более узкий `quote` фильтр (например, `USDT` вместо
  `*USD`).

### Логи не появляются

- Проверьте права на запись в `./logs/`.
- Проверьте `RUST_LOG` env var (по умолчанию `tick_screener=info,warn`).
- Старые логи (>7 дней) удаляются при старте.

---

## Лицензия

MIT OR Apache-2.0 — выберите удобную для вашего случая.

## Авторство

Оригинальный автор: [JustAMamont](https://github.com/JustAMamont).


