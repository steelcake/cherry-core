use std::collections::BTreeMap;
use std::pin::Pin;

use anyhow::{Context, Result};
use arrow::{pyarrow::ToPyArrow, record_batch::RecordBatch};
use baselib::ingest::{ProviderConfig, Query};
use futures_lite::{Stream, StreamExt};
use pyo3::prelude::*;

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
fn start_stream(
    provider_config: &Bound<'_, PyAny>,
    query: &Bound<'_, PyAny>,
) -> PyResult<ResponseStream> {
    let cfg: ProviderConfig = provider_config.extract().context("parse provider config")?;
    let query: Query = query.extract().context("parse query")?;

    let inner = crate::TOKIO_RUNTIME.block_on(async move {
        baselib::ingest::start_stream(cfg, query)
            .await
            .context("start stream")
    })?;

    Ok(ResponseStream { inner: Some(inner) })
}
