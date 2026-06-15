//! Command-line entry point for the ansible-worker binary.
//!
//! Loads configuration, wires up the configured transport, queue, executor,
//! and status publisher, then runs until interrupted by `Ctrl+C` or `SIGTERM`.

use anyhow::{Context, Result};
use ansible_worker::{
    config::{Config, TransportType},
    executor::{Executor, ExecutorStatusUpdate},
    task_queue::TaskQueue,
    transport::{
        http::{HttpStatusPublisher, HttpTransport},
        mqtt::{MqttStatusPublisher, MqttTransport},
        StatusPublisher, TaskReceiver,
    },
    VERSION,
};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "ansible-worker")]
#[command(version = VERSION)]
#[command(about = "MQTT-triggered Ansible playbook execution worker")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,

    /// Validate configuration and exit
    #[arg(long)]
    validate: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Load configuration
    let config = Config::load(&args.config)
        .with_context(|| format!("Failed to load config from {:?}", args.config))?;

    // If validate flag is set, just validate and exit
    if args.validate {
        println!("Configuration is valid");
        return Ok(());
    }

    // Setup logging
    setup_logging(&config.log_level)?;

    info!("Starting ansible-worker v{}", VERSION);
    info!("Loaded configuration from {:?}", args.config);

    // Run the worker
    let worker = AnsibleWorker::new(config)?;
    worker.run().await
}

fn setup_logging(level: &str) -> Result<()> {
    let level = match level.to_uppercase().as_str() {
        "TRACE" => Level::TRACE,
        "DEBUG" => Level::DEBUG,
        "INFO" => Level::INFO,
        "WARN" => Level::WARN,
        "ERROR" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .context("Failed to set tracing subscriber")?;

    Ok(())
}

struct AnsibleWorker {
    config: Arc<Config>,
    task_queue: TaskQueue,
}

impl AnsibleWorker {
    fn new(config: Config) -> Result<Self> {
        let config = Arc::new(config);
        let task_queue = TaskQueue::new(config.worker.max_queue_size);

        Ok(Self { config, task_queue })
    }

    async fn run(self) -> Result<()> {
        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Create status update channel
        let (status_tx, mut status_rx) = mpsc::channel::<ExecutorStatusUpdate>(100);

        // Create transport based on configuration
        let transport_type = &self.config.transport;
        info!("Using {:?} transport", transport_type);

        // Spawn task receiver based on transport type
        let receiver_config = Arc::clone(&self.config);
        let receiver_queue = self.task_queue.clone();
        let receiver_shutdown_rx = shutdown_rx.clone();
        let receiver_handle: tokio::task::JoinHandle<()> = match transport_type {
            TransportType::Mqtt => {
                let transport = MqttTransport::new(receiver_config);
                tokio::spawn(async move {
                    if let Err(e) = transport.run(receiver_queue, receiver_shutdown_rx).await {
                        error!("MQTT receiver error: {}", e);
                    }
                })
            }
            TransportType::Http => {
                let transport = match HttpTransport::new(receiver_config) {
                    Ok(t) => t,
                    Err(e) => {
                        error!("Failed to create HTTP transport: {}", e);
                        return Err(e);
                    }
                };
                tokio::spawn(async move {
                    if let Err(e) = transport.run(receiver_queue, receiver_shutdown_rx).await {
                        error!("HTTP receiver error: {}", e);
                    }
                })
            }
        };

        // Spawn executor
        let executor_config = Arc::clone(&self.config);
        let executor_queue = self.task_queue.clone();
        let executor_shutdown_rx = shutdown_rx.clone();
        let executor_handle = tokio::spawn(async move {
            let mut executor = Executor::new(
                executor_config,
                executor_queue,
                status_tx,
                executor_shutdown_rx,
            );
            executor.run().await;
        });

        // Create status publisher based on transport type
        let pub_config = Arc::clone(&self.config);
        let publisher_handle: tokio::task::JoinHandle<()> = match transport_type {
            TransportType::Mqtt => {
                let mqtt_transport = MqttTransport::new(Arc::clone(&pub_config));
                let publisher = match MqttStatusPublisher::new(Arc::clone(&pub_config), &mqtt_transport) {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Failed to create MQTT publisher: {}", e);
                        return Err(e);
                    }
                };
                let eventloop_handle = publisher.start_event_loop();

                tokio::spawn(async move {
                    while let Some(update) = status_rx.recv().await {
                        if let Err(e) = publisher.publish(&update.status).await {
                            error!("Failed to publish status: {}", e);
                        }
                    }
                    eventloop_handle.abort();
                })
            }
            TransportType::Http => {
                let publisher = match HttpStatusPublisher::new(pub_config) {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Failed to create HTTP publisher: {}", e);
                        return Err(e);
                    }
                };

                tokio::spawn(async move {
                    while let Some(update) = status_rx.recv().await {
                        if let Err(e) = publisher.publish(&update.status).await {
                            error!("Failed to publish status: {}", e);
                        }
                    }
                })
            }
        };

        // Wait for shutdown signal
        info!("Worker running, waiting for shutdown signal...");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl+C, shutting down...");
            }
            _ = async {
                #[cfg(unix)]
                {
                    let mut sigterm = tokio::signal::unix::signal(
                        tokio::signal::unix::SignalKind::terminate()
                    ).expect("Failed to register SIGTERM handler");
                    sigterm.recv().await;
                }
                #[cfg(not(unix))]
                {
                    std::future::pending::<()>().await;
                }
            } => {
                info!("Received SIGTERM, shutting down...");
            }
        }

        // Signal shutdown
        let _ = shutdown_tx.send(true);
        self.task_queue.shutdown().await;

        // Cancel remaining tasks in queue and publish cancellation status
        let remaining = self.task_queue.clear().await;
        if !remaining.is_empty() {
            info!("Cancelling {} remaining tasks in queue", remaining.len());

            // Create a publisher for cancellation status based on transport type
            match &self.config.transport {
                TransportType::Mqtt => {
                    let mqtt_transport = MqttTransport::new(Arc::clone(&self.config));
                    if let Ok(publisher) = MqttStatusPublisher::new(Arc::clone(&self.config), &mqtt_transport) {
                        let eventloop_handle = publisher.start_event_loop();
                        // Give it a moment to connect
                        tokio::time::sleep(Duration::from_millis(500)).await;

                        for mut task in remaining {
                            task.cancel();
                            let _ = publisher.publish(&task.status).await;
                        }
                        eventloop_handle.abort();
                    }
                }
                TransportType::Http => {
                    if let Ok(publisher) = HttpStatusPublisher::new(Arc::clone(&self.config)) {
                        for mut task in remaining {
                            task.cancel();
                            let _ = publisher.publish(&task.status).await;
                        }
                    }
                }
            }
        }

        // Wait for tasks to complete (with timeout)
        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            let _ = receiver_handle.await;
            let _ = executor_handle.await;
        })
        .await;

        publisher_handle.abort();

        info!("Shutdown complete");
        Ok(())
    }
}
