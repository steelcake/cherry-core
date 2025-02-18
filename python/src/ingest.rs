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
#[allow(clippy::type_complexity)]
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

    let inner = crate::TOKIO_RUNTIME.block_on(async move {
        baselib::ingest::start_stream(cfg)
            .await
            .context("start stream")
    })?;

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
        .context("get provider attribute")?
        .extract()
        .context("parse provider config")?;

    Ok(baselib::ingest::StreamConfig { format, provider })
}
