use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures_lite::{Stream, StreamExt};
use reqwest::Url;

pub mod evm;

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub format: Format,
    pub provider: Provider,
}

#[derive(Debug, Clone)]
pub enum Format {
    Evm(evm::Query),
}

#[derive(Debug, Clone)]
pub enum Provider {
    Sqd {
        client_config: sqd_portal_client::ClientConfig,
        url: Url,
    },
}

#[allow(clippy::type_complexity)]
pub fn start_stream(
    cfg: StreamConfig,
) -> Result<Pin<Box<dyn Stream<Item = Result<BTreeMap<String, RecordBatch>>> + Send + Sync>>> {
    match cfg.provider {
        Provider::Sqd { client_config, url } => match cfg.format {
            Format::Evm(evm_query) => {
                let evm_query = evm_query.to_sqd();

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
        },
    }
}
