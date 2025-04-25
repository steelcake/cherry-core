#![allow(clippy::should_implement_trait)]
#![allow(clippy::field_reassign_with_default)]

use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use futures_lite::{Stream, StreamExt};
use provider::common::{evm_query_to_generic, svm_query_to_generic};
use serde::de::DeserializeOwned;

pub mod evm;
mod provider;
mod rayon_async;
pub mod svm;

#[derive(Debug, Clone)]
pub enum Query {
    Evm(evm::Query),
    Svm(svm::Query),
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for Query {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;

        let kind = ob.getattr("kind").context("get kind attribute")?;
        let kind: &str = kind.extract().context("kind as str")?;

        let query = ob.getattr("params").context("get params attribute")?;

        match kind {
            "evm" => Ok(Self::Evm(query.extract().context("parse query")?)),
            "svm" => Ok(Self::Svm(query.extract().context("parse query")?)),
            _ => Err(anyhow!("unknown query kind: {}", kind).into()),
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct ProviderConfig {
    pub kind: ProviderKind,
    pub url: Option<String>,
    pub bearer_token: Option<String>,
    pub max_num_retries: Option<usize>,
    pub retry_backoff_ms: Option<u64>,
    pub retry_base_ms: Option<u64>,
    pub retry_ceiling_ms: Option<u64>,
    pub req_timeout_millis: Option<u64>,
    pub stop_on_head: bool,
    pub head_poll_interval_millis: Option<u64>,
    pub buffer_size: Option<usize>,
}

impl ProviderConfig {
    pub fn new(kind: ProviderKind) -> Self {
        Self {
            kind,
            url: None,
            bearer_token: None,
            max_num_retries: None,
            retry_backoff_ms: None,
            retry_base_ms: None,
            retry_ceiling_ms: None,
            req_timeout_millis: None,
            stop_on_head: false,
            head_poll_interval_millis: None,
            buffer_size: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ProviderKind {
    Sqd,
    Hypersync,
    YellowstoneGrpc,
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for ProviderKind {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;

        let out: &str = ob.extract().context("read as string")?;

        match out {
            "sqd" => Ok(Self::Sqd),
            "hypersync" => Ok(Self::Hypersync),
            "yellowstone_grpc" => Ok(Self::YellowstoneGrpc),
            _ => Err(anyhow!("unknown provider kind: {}", out).into()),
        }
    }
}

type DataStream = Pin<Box<dyn Stream<Item = Result<BTreeMap<String, RecordBatch>>> + Send + Sync>>;

fn make_req_fields<T: DeserializeOwned>(query: &cherry_query::Query) -> Result<T> {
    let mut req_fields_query = query.clone();
    req_fields_query
        .add_request_and_include_fields()
        .context("add req and include fields")?;

    let fields = req_fields_query
        .fields
        .into_iter()
        .map(|(k, v)| {
            (
                k.strip_suffix('s').unwrap().to_owned(),
                v.into_iter()
                    .map(|v| (v, true))
                    .collect::<BTreeMap<String, bool>>(),
            )
        })
        .collect::<BTreeMap<String, _>>();

    Ok(serde_json::from_value(serde_json::to_value(&fields).unwrap()).unwrap())
}

pub async fn start_stream(provider_config: ProviderConfig, mut query: Query) -> Result<DataStream> {
    let generic_query = match &mut query {
        Query::Evm(evm_query) => {
            let generic_query = evm_query_to_generic(evm_query);

            evm_query.fields = make_req_fields(&generic_query).context("make req fields")?;

            generic_query
        }
        Query::Svm(svm_query) => {
            let generic_query = svm_query_to_generic(svm_query);

            svm_query.fields = make_req_fields(&generic_query).context("make req fields")?;

            generic_query
        }
    };
    let generic_query = Arc::new(generic_query);

    let stream = match provider_config.kind {
        ProviderKind::Sqd => {
            provider::sqd::start_stream(provider_config, query).context("start sqd stream")?
        }
        ProviderKind::Hypersync => provider::hypersync::start_stream(provider_config, query)
            .await
            .context("start hypersync stream")?,
        ProviderKind::YellowstoneGrpc => {
            provider::yellowstone_grpc::start_stream(provider_config, query)
                .await
                .context("start yellowstone_grpc stream")?
        }
    };

    let stream = stream.then(move |res| {
        let generic_query = Arc::clone(&generic_query);
        async {
            rayon_async::spawn(move || {
                res.and_then(move |data| {
                    let data = cherry_query::run_query(&data, &generic_query)
                        .context("run local query")?;
                    Ok(data)
                })
            })
            .await
            .unwrap()
        }
    });

    Ok(Box::pin(stream))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::svm::*;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    #[tokio::test]
    #[ignore]
    async fn simple_svm_start_stream() {
        let mut provider_config = ProviderConfig::new(ProviderKind::Sqd);
        provider_config.url = Some("https://portal.sqd.dev/datasets/solana-mainnet".to_string());

        let program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
        let program_id: [u8; 32] = bs58::decode(program_id)
            .into_vec()
            .unwrap()
            .try_into()
            .unwrap();
        let program_id = Address(program_id);

        let query = crate::Query::Svm(svm::Query {
            from_block: 329443000,
            to_block: Some(329443000),
            include_all_blocks: false,
            fields: Fields {
                instruction: InstructionFields::all(),
                transaction: TransactionFields::default(),
                log: LogFields::default(),
                balance: BalanceFields::default(),
                token_balance: TokenBalanceFields::default(),
                reward: RewardFields::default(),
                block: BlockFields::default(),
            },
            instructions: vec![
                // InstructionRequest::default() ,
                InstructionRequest {
                    program_id: vec![program_id],
                    discriminator: vec![Data(vec![12, 96, 49, 128, 22])],
                    ..Default::default()
                },
            ],
            transactions: vec![],
            logs: vec![],
            balances: vec![],
            token_balances: vec![],
            rewards: vec![],
        });
        let mut stream = start_stream(provider_config, query).await.unwrap();
        let data = stream.next().await.unwrap().unwrap();
        for (k, v) in data.into_iter() {
            let mut file = File::create(format!("{}.parquet", k)).unwrap();
            let mut writer = ArrowWriter::try_new(&mut file, v.schema(), None).unwrap();
            writer.write(&v).unwrap();
            writer.close().unwrap();
        }
    }
}
