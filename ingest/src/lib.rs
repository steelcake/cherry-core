use std::{collections::BTreeMap, pin::Pin};

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use futures_lite::Stream;

pub mod evm;
mod provider;

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub format: Format,
    pub provider: ProviderConfig,
}

#[derive(Debug, Clone)]
pub enum Format {
    Evm(evm::Query),
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
    pub http_req_timeout_millis: Option<u64>,
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
            http_req_timeout_millis: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ProviderKind {
    Sqd,
    Hypersync,
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for ProviderKind {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;

        let out: &str = ob.extract().context("read as string")?;

        match out {
            "sqd" => Ok(Self::Sqd),
            "hypersync" => Ok(Self::Hypersync),
            _ => Err(anyhow!("unknown provider kind: {}", out).into()),
        }
    }
}

type DataStream = Pin<Box<dyn Stream<Item = Result<BTreeMap<String, RecordBatch>>> + Send + Sync>>;

pub async fn start_stream(cfg: StreamConfig) -> Result<DataStream> {
    match cfg.provider.kind {
        ProviderKind::Sqd => provider::sqd::start_stream(cfg),
        ProviderKind::Hypersync => provider::hypersync::start_stream(cfg).await,
    }
}
