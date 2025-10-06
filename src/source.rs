use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_std::channel::{self, Receiver, Sender};
use async_std::task::spawn;
use async_trait::async_trait;
use futures::{stream::LocalBoxStream, StreamExt};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS, Transport};
use rustls::ClientConfig;
use url::Url;

use fluvio::Offset;
use fluvio_connector_common::tracing::info;
use fluvio_connector_common::{
    tracing::{error, warn},
    Source,
};

use crate::formatter::{self, Formatter};
use crate::{config::MqttConfig, error::MqttConnectorError, event::MqttEvent};

const MQTT_CLIENT_BUFFER_SIZE: usize = 10;
const MIN_LOG_WARN_TIME: Duration = Duration::from_secs(5 * 60);

pub(crate) struct MqttSource {
    formatter: Box<dyn Formatter + Sync + Send>,
    options: MqttOptions,
    topic: String,
    qos: QoS,
    channel_capacity: usize,
}

impl MqttSource {
    pub(crate) fn new(config: &MqttConfig) -> Result<Self> {
        let mut url = Url::parse(&config.url.resolve()?)
            .context("unable to parse mqtt broker endpoint url")?;

        if !url.query_pairs().any(|(key, _)| key == "client_id") {
            url.query_pairs_mut()
                .append_pair("client_id", &config.client_id);
        }

        {
            let mut url_without_password = url.clone();
            let _ = url_without_password.set_password(None);
            info!(
                timeout=?config.timeout,
                mqtt_url=%url_without_password,
                %config.topic,
                %config.client_id
            );
        }
        let mut options = MqttOptions::try_from(url.clone())?;
        options.set_keep_alive(config.timeout);
        // limit broker→client pressure
        options.set_inflight(config.inflight);
        if url.scheme() == "mqtts" || url.scheme() == "ssl" {
            info!("using tls");
            let mut root_cert_store = rustls::RootCertStore::empty();
            for cert in
                rustls_native_certs::load_native_certs().context("could not load platform certs")?
            {
                root_cert_store.add(cert).context("Failed to parse DER")?;
            }
            let client_config = ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();

            options.set_transport(Transport::tls_with_config(client_config.into()));
        }
        let formatter = formatter::from_output_type(&config.payload_output_type);
        let topic = config.topic.clone();
        let qos = match config.qos {
            crate::config::QosConfig::AtMostOnce => QoS::AtMostOnce,
            crate::config::QosConfig::AtLeastOnce => QoS::AtLeastOnce,
            crate::config::QosConfig::ExactlyOnce => QoS::ExactlyOnce,
        };
        Ok(Self {
            formatter,
            options,
            topic,
            qos,
            channel_capacity: config.channel_capacity,
        })
    }
}

#[async_trait]
impl<'a> Source<'a, String> for MqttSource {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxStream<'a, String>> {
        let (client, event_loop) = AsyncClient::new(self.options, MQTT_CLIENT_BUFFER_SIZE);
        client.subscribe(self.topic, self.qos).await?;
        let (sender, receiver) = channel::bounded(self.channel_capacity);
        spawn(mqtt_loop(
            sender,
            receiver.clone(),
            event_loop,
            self.formatter,
        ));
        Ok(receiver.boxed_local())
    }
}

async fn mqtt_loop(
    tx: Sender<String>,
    rx: Receiver<String>,
    mut event_loop: EventLoop,
    formatter: Box<dyn Formatter + Sync + Send>,
) -> Result<(), MqttConnectorError> {
    let mut last_warn = Instant::now();
    loop {
        // eventloop.poll() docs state "Don't block while iterating"
        let notification = match event_loop.poll().await {
            Ok(notification) => notification,
            Err(e) => {
                error!("Mqtt error {}. Finishing mqtt loop", e);
                tx.close();
                return Err(MqttConnectorError::MqttConnection(e));
            }
        };

        if let Ok(mqtt_event) = MqttEvent::try_from(notification) {
            let formatted = match formatter.to_string(&mqtt_event) {
                Ok(s) => s,
                Err(_) => {
                    let elapsed = last_warn.elapsed();
                    if elapsed > MIN_LOG_WARN_TIME {
                        warn!("Failed to format MQTT message; skipping");
                        last_warn = Instant::now();
                    }
                    continue;
                }
            };
            // Backpressure: await send instead of drop-on-full
            if let Err(_closed) = tx.send(formatted).await {
                error!("Channel closed; finishing mqtt loop");
                return Err(MqttConnectorError::ChannelClosed);
            }
        }
    }
}
