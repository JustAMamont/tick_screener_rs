use std::fs;
use std::path::Path;
use std::sync::LazyLock;
use tracing_appender::rolling::RollingFileAppender;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const LOG_DIR: &str = "logs";
const MAX_LOG_AGE_DAYS: i64 = 7;

static LOG_GUARD: LazyLock<tracing_appender::non_blocking::WorkerGuard> = LazyLock::new(|| {
    fs::create_dir_all(LOG_DIR).expect("failed to create log directory");
    cleanup_old_logs();

    let file_appender = RollingFileAppender::new(
        tracing_appender::rolling::Rotation::DAILY,
        LOG_DIR,
        "screener.log",
    );

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(true);

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_target(false);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("tick_screener=info,warn"));

    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(stdout_layer)
        .init();

    guard
});

pub fn init_logger() {
    // Force initialization of the static lazy lock
    LazyLock::force(&LOG_GUARD);
    tracing::info!("Logger initialized. Log directory: {}", LOG_DIR);
}

fn cleanup_old_logs() {
    let log_path = Path::new(LOG_DIR);
    if !log_path.exists() {
        return;
    }

    let now = chrono::Utc::now();

    if let Ok(entries) = fs::read_dir(log_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "log" {
                    if let Ok(metadata) = fs::metadata(&path) {
                        if let Ok(modified) = metadata.modified() {
                            let modified_time: chrono::DateTime<chrono::Utc> = modified.into();
                            let age = now.signed_duration_since(modified_time);

                            if age.num_days() > MAX_LOG_AGE_DAYS {
                                match fs::remove_file(&path) {
                                    Ok(()) => eprintln!("Removed old log: {:?}", path),
                                    Err(e) => eprintln!("Failed to remove {:?}: {}", path, e),
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
