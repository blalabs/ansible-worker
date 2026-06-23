//! MQTT transport: subscribes to a shared task topic and publishes per-task
//! status, with optional TLS and mutual TLS.

use super::{StatusPublisher, TaskReceiver};
use crate::config::{Config, MqttConfig};
use crate::models::{Task, TaskRequest, TaskStatus};
use crate::task_queue::{QueueError, TaskQueue};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS, Transport};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tracing::{debug, error, info, warn};

/// MQTT-based transport for task communication
pub struct MqttTransport {
    config: Arc<Config>,
}

impl MqttTransport {
    /// Create a transport from shared configuration.
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    /// Configure TLS transport for MQTT connection
    fn configure_tls(mqtt_config: &MqttConfig) -> Result<Transport> {
        use std::fs::File;
        use std::io::BufReader;

        let mut root_cert_store = rustls::RootCertStore::empty();

        // Load CA certificate if provided
        if let Some(ca_path) = &mqtt_config.tls_ca_cert {
            let ca_file = File::open(ca_path)
                .with_context(|| format!("Failed to open CA certificate: {}", ca_path))?;
            let mut ca_reader = BufReader::new(ca_file);
            let certs: Vec<_> = rustls_pemfile::certs(&mut ca_reader)
                .collect::<Result<Vec<_>, _>>()
                .with_context(|| format!("Failed to parse CA certificate: {}", ca_path))?;
            for cert in certs {
                root_cert_store
                    .add(cert)
                    .context("Failed to add CA certificate to root store")?;
            }
        } else {
            // Use system root certificates
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        let tls_config = rustls::ClientConfig::builder().with_root_certificates(root_cert_store);

        // Configure client certificate if provided (mutual TLS)
        let tls_config = if let (Some(cert_path), Some(key_path)) =
            (&mqtt_config.tls_client_cert, &mqtt_config.tls_client_key)
        {
            let cert_file = File::open(cert_path)
                .with_context(|| format!("Failed to open client certificate: {}", cert_path))?;
            let mut cert_reader = BufReader::new(cert_file);
            let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .with_context(|| format!("Failed to parse client certificate: {}", cert_path))?;

            let key_file = File::open(key_path)
                .with_context(|| format!("Failed to open client key: {}", key_path))?;
            let mut key_reader = BufReader::new(key_file);

            let key = rustls_pemfile::private_key(&mut key_reader)
                .context("Failed to read private key")?
                .ok_or_else(|| anyhow::anyhow!("No private key found in {}", key_path))?;

            tls_config
                .with_client_auth_cert(certs, key)
                .context("Failed to configure client authentication")?
        } else {
            tls_config.with_no_client_auth()
        };

        Ok(Transport::tls_with_config(
            rumqttc::TlsConfiguration::Rustls(Arc::new(tls_config)),
        ))
    }

    /// Create MQTT options with optional TLS
    pub fn create_mqtt_options(&self, client_id: &str) -> Result<MqttOptions> {
        let mqtt_config = &self.config.mqtt;
        let mqtt = mqtt_config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MQTT configuration required for MQTT transport"))?;

        let mut mqtt_options = MqttOptions::new(client_id, &mqtt.host, mqtt.port);
        mqtt_options.set_keep_alive(Duration::from_secs(mqtt.keepalive));

        if let (Some(username), Some(password)) = (&mqtt.username, &mqtt.password) {
            mqtt_options.set_credentials(username, password);
        }

        mqtt_options.set_clean_session(true);

        // Configure TLS if enabled
        if mqtt.tls_enabled {
            let transport =
                Self::configure_tls(mqtt).context("Failed to configure TLS for MQTT")?;
            mqtt_options.set_transport(transport);
            info!("TLS enabled for MQTT connection");
        }

        Ok(mqtt_options)
    }
}

#[async_trait]
impl TaskReceiver for MqttTransport {
    async fn run(
        &self,
        task_queue: TaskQueue,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        let mqtt = self
            .config
            .mqtt
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("MQTT configuration required"))?;

        let client_id = format!(
            "ansible-worker-{}-{}",
            self.config.worker.group_name,
            uuid::Uuid::new_v4()
        );

        let mqtt_options = self.create_mqtt_options(&client_id)?;
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 100);

        let task_topic = self.config.task_topic();
        info!("Connecting to MQTT broker at {}:{}", mqtt.host, mqtt.port);

        let mut _connected = false;
        let mut _subscribed = false;

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("MQTT event loop received shutdown signal");
                        let _ = client.disconnect().await;
                        break;
                    }
                }

                event = eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(Packet::ConnAck(connack))) => {
                            info!("Connected to MQTT broker: {:?}", connack);
                            _connected = true;

                            if let Err(e) = client.subscribe(&task_topic, QoS::ExactlyOnce).await {
                                error!("Failed to subscribe: {}", e);
                            }
                        }

                        Ok(Event::Incoming(Packet::SubAck(suback))) => {
                            info!("Subscribed to task topic: {:?}", suback);
                            _subscribed = true;
                        }

                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            debug!("Received message on topic: {}", publish.topic);
                            self.handle_task_message(&publish.payload, &task_queue, &client).await;
                        }

                        Ok(Event::Incoming(Packet::Disconnect)) => {
                            warn!("Disconnected from MQTT broker");
                            _connected = false;
                            _subscribed = false;
                        }

                        Ok(Event::Incoming(packet)) => {
                            debug!("Received packet: {:?}", packet);
                        }

                        Ok(Event::Outgoing(_)) => {}

                        Err(e) => {
                            error!("MQTT error: {}", e);
                            _connected = false;
                            _subscribed = false;
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl MqttTransport {
    async fn handle_task_message(
        &self,
        payload: &[u8],
        task_queue: &TaskQueue,
        client: &AsyncClient,
    ) {
        match TaskRequest::from_json(payload) {
            Ok(request) => {
                let task_id = request.task_id.clone();
                let task = Task::new(request);

                info!("Received task: {}", task_id);

                match task_queue.put(task).await {
                    Ok(()) => {
                        debug!("Task {} added to queue", task_id);
                    }
                    Err(QueueError::QueueFull(max)) => {
                        warn!("Queue full (max: {}), rejecting task {}", max, task_id);
                        let mut status = TaskStatus::new(task_id.clone());
                        status.state = crate::models::TaskState::Failed;
                        status.error_message = Some(format!("Queue full (max: {})", max));
                        let topic = self.config.status_topic(&task_id);
                        if let Ok(payload) = status.to_json() {
                            let _ = client
                                .publish(&topic, QoS::AtLeastOnce, true, payload)
                                .await;
                        }
                    }
                    Err(QueueError::Shutdown) => {
                        warn!("Queue shutdown, rejecting task {}", task_id);
                    }
                }
            }
            Err(e) => {
                error!("Failed to parse task request: {}", e);
            }
        }
    }
}

/// MQTT status publisher
pub struct MqttStatusPublisher {
    config: Arc<Config>,
    client: AsyncClient,
    eventloop: Mutex<Option<EventLoop>>,
}

impl MqttStatusPublisher {
    /// Create a publisher with its own MQTT connection, derived from the same
    /// options as `transport`. Call [`start_event_loop`](Self::start_event_loop)
    /// to drive the connection.
    pub fn new(config: Arc<Config>, transport: &MqttTransport) -> Result<Self> {
        let client_id = format!(
            "ansible-worker-pub-{}-{}",
            config.worker.group_name,
            uuid::Uuid::new_v4()
        );

        let mqtt_options = transport.create_mqtt_options(&client_id)?;
        let (client, eventloop) = AsyncClient::new(mqtt_options, 100);

        Ok(Self {
            config,
            client,
            eventloop: Mutex::new(Some(eventloop)),
        })
    }

    /// Start the event loop in a background task
    pub fn start_event_loop(&self) -> tokio::task::JoinHandle<()> {
        let eventloop = {
            let mut guard = self.eventloop.blocking_lock();
            guard.take()
        };

        tokio::spawn(async move {
            if let Some(mut eventloop) = eventloop {
                loop {
                    match eventloop.poll().await {
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            info!("Publisher connected to MQTT broker");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Publisher MQTT error: {}", e);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        })
    }
}

#[async_trait]
impl StatusPublisher for MqttStatusPublisher {
    async fn publish(&self, status: &TaskStatus) -> Result<()> {
        let topic = self.config.status_topic(&status.task_id);
        let payload = status.to_json().context("Failed to serialize status")?;

        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload)
            .await
            .context("Failed to publish status")?;

        debug!("Published status for task {} to {}", status.task_id, topic);
        Ok(())
    }
}
