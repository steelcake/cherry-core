use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures_lite::{Stream, StreamExt};
use reqwest::Url;

pub mod evm;

#[derive(Debug, Clone)]
pub struct Query {
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

pub fn start_stream(
    query: Query,
) -> Result<Pin<Box<dyn Stream<Item = Result<BTreeMap<String, RecordBatch>>>>>> {
    match query.provider {
        Provider::Sqd { client_config, url } => match query.format {
            Format::Evm(evm_query) => {
                let evm_query = evm_query.to_sqd();

                let client = sqd_portal_client::Client::new(url, client_config);
                let client = Arc::new(client);

                let stream = client.evm_arrow_finalized_stream(
                    evm_query,
                    sqd_portal_client::StreamConfig {
                        stop_on_head: true,
                        head_poll_interval_millis: 10_000,
                    },
                );

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
