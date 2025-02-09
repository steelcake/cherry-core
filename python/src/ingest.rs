use std::collections::BTreeMap;
use std::pin::Pin;

use anyhow::{anyhow, Context, Result};
use arrow::{pyarrow::ToPyArrow, record_batch::RecordBatch};
use futures_lite::{Stream, StreamExt};
use pyo3::{intern, prelude::*};

pub fn ingest_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let submodule = PyModule::new(py, "ingest")?;

    submodule.add_function(wrap_pyfunction!(start_stream, m)?)?;

    m.add_submodule(&submodule)?;

    Ok(())
}

#[pyclass]
struct ResponseStream {
    inner: Option<Pin<Box<dyn Stream<Item = Result<BTreeMap<String, RecordBatch>>> + Send + Sync>>>,
}

#[pymethods]
impl ResponseStream {
    pub fn close(&mut self) {
        self.inner.take();
    }

    pub async fn next(&mut self) -> PyResult<Option<BTreeMap<String, PyObject>>> {
        let inner = match self.inner.as_mut() {
            Some(i) => i,
            None => return Ok(None),
        };

        let next: BTreeMap<String, RecordBatch> = match inner.next().await {
            Some(n) => n.context("get next item from inner stream")?,
            None => {
                self.inner = None;
                return Ok(None);
            }
        };

        let mut out = BTreeMap::new();

        for (table_name, batch) in next.into_iter() {
            let batch =
                Python::with_gil(|py| batch.to_pyarrow(py).context("map result to pyarrow"))?;

            out.insert(table_name, batch);
        }

        Ok(Some(out))
    }
}

#[pyfunction]
fn start_stream(query: &Bound<'_, PyAny>) -> PyResult<ResponseStream> {
    let cfg = parse_stream_config(query).context("parse stream config")?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    let inner = runtime
        .block_on(async move { baselib::ingest::start_stream(cfg).context("start stream") })?;
    std::mem::forget(runtime);

    Ok(ResponseStream { inner: Some(inner) })
}

fn parse_stream_config(cfg: &Bound<'_, PyAny>) -> Result<baselib::ingest::StreamConfig> {
    let format = cfg
        .getattr(intern!(cfg.py(), "format"))
        .context("get format attribute")?;
    let format: &str = format.extract().context("read format as string")?;

    let query = cfg
        .getattr(intern!(cfg.py(), "query"))
        .context("get query attribute")?;

    let format = match format {
        "evm" => {
            let evm_query: baselib::ingest::evm::Query =
                query.extract().context("extract evm query")?;
            baselib::ingest::Format::Evm(evm_query)
        }
        _ => {
            return Err(anyhow!("unknown query format: {}", format));
        }
    };

    let provider = cfg
        .getattr(intern!(cfg.py(), "provider"))
        .context("get provider attribute")?;
    let provider_kind = provider
        .getattr(intern!(provider.py(), "kind"))
        .context("get provider.kind attribute")?;
    let provider_config = provider
        .getattr(intern!(provider.py(), "config"))
        .context("get provider.config attribute")?;

    let provider_kind: &str = provider_kind
        .extract()
        .context("read provider.kind as string")?;
    let provider_config: ProviderConfig =
        provider_config.extract().context("read provider.config")?;

    let provider = match provider_kind {
        "sqd" => {
            let mut client_config = sqd_portal_client::ClientConfig::default();

            if let Some(v) = provider_config.max_num_retries {
                client_config.max_num_retries = v;
            }
            if let Some(v) = provider_config.retry_backoff_ms {
                client_config.retry_backoff_ms = v;
            }
            if let Some(v) = provider_config.retry_base_ms {
                client_config.retry_base_ms = v;
            }
            if let Some(v) = provider_config.retry_ceiling_ms {
                client_config.retry_ceiling_ms = v;
            }
            if let Some(v) = provider_config.http_req_timeout_millis {
                client_config.http_req_timeout_millis = v;
            }

            let url = provider_config
                .url
                .context("url is required when using sqd")?
                .parse()
                .context("parse url")?;

            baselib::ingest::Provider::Sqd { client_config, url }
        }
        _ => {
            return Err(anyhow!("unknown provider: {}", provider_kind));
        }
    };

    Ok(baselib::ingest::StreamConfig { format, provider })
}

#[derive(FromPyObject)]
struct ProviderConfig {
    url: Option<String>,
    max_num_retries: Option<usize>,
    retry_backoff_ms: Option<u64>,
    retry_base_ms: Option<u64>,
    retry_ceiling_ms: Option<u64>,
    http_req_timeout_millis: Option<u64>,
}
