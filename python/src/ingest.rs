use std::collections::BTreeMap;
use std::pin::Pin;

use anyhow::{Context, Result};
use arrow::{pyarrow::ToPyArrow, record_batch::RecordBatch};
use futures_lite::{Stream, StreamExt};
use pyo3::prelude::*;

pub fn ingest_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let submodule = PyModule::new(py, "ingest")?;

    m.add_function(wrap_pyfunction!(start_stream, m)?)?;

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
    let query = parse_query(query).context("parse query")?;

    let inner = baselib::ingest::start_stream(query).context("start stream")?;

    Ok(ResponseStream { inner: Some(inner) })
}

#[derive(FromPyObject)]
struct Query {
    format: String,
    provider: String,
}

#[derive(FromPyObject)]
struct Format {}

#[derive(FromPyObject)]
struct Provider {
    kind: String,
}
