#![allow(clippy::should_implement_trait)]
#![allow(clippy::field_reassign_with_default)]

use std::{collections::BTreeMap, pin::Pin};

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use futures_lite::Stream;

pub mod evm;
mod provider;
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
    pub query: Query,
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
    pub fn new(kind: ProviderKind, query: Query) -> Self {
        Self {
            kind,
            query,
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

pub async fn start_stream(provider_config: ProviderConfig) -> Result<DataStream> {
    match provider_config.kind {
        ProviderKind::Sqd => provider::sqd::start_stream(provider_config),
        ProviderKind::Hypersync => provider::hypersync::start_stream(provider_config).await,
        ProviderKind::YellowstoneGrpc => {
            provider::yellowstone_grpc::start_stream(provider_config).await
        }
    }
}
