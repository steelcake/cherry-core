use crate::{DataStream, Format, StreamConfig};
use anyhow::{Context, Result};
use futures_lite::StreamExt;
use std::collections::BTreeMap;

use std::sync::Arc;

pub fn start_stream(cfg: StreamConfig) -> Result<DataStream> {
    match cfg.format {
        Format::Evm(evm_query) => {
            let evm_query = evm_query.to_sqd().context("convert to sqd query")?;

            let url = cfg
                .provider
                .url
                .context("url is required when using sqd")?
                .parse()
                .context("parse url")?;

            let mut client_config = sqd_portal_client::ClientConfig::default();

            if let Some(v) = cfg.provider.max_num_retries {
                client_config.max_num_retries = v;
            }
            if let Some(v) = cfg.provider.retry_backoff_ms {
                client_config.retry_backoff_ms = v;
            }
            if let Some(v) = cfg.provider.retry_base_ms {
                client_config.retry_base_ms = v;
            }
            if let Some(v) = cfg.provider.retry_ceiling_ms {
                client_config.retry_ceiling_ms = v;
            }
            if let Some(v) = cfg.provider.http_req_timeout_millis {
                client_config.http_req_timeout_millis = v;
            }

            let client = sqd_portal_client::Client::new(url, client_config);
            let client = Arc::new(client);

            let receiver = client.evm_arrow_finalized_stream(
                evm_query,
                sqd_portal_client::StreamConfig {
                    stop_on_head: true,
                    ..Default::default()
                },
            );

            let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

            let stream = stream.map(|v| {
                v.map(|v| {
                    let mut data = BTreeMap::new();

                    data.insert("blocks".to_owned(), v.blocks);
                    data.insert("transactions".to_owned(), v.transactions);
                    data.insert("logs".to_owned(), v.logs);
                    data.insert("traces".to_owned(), v.traces);

                    data
                })
            });

            Ok(Box::pin(stream))
        }
    }
}
