use pyo3::prelude::*;

pub fn ingest_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let submodule = PyModule::new(py, "ingest")?;

    submodule.add_class(Query);

    m.add_submodule(&submodule)?;

    Ok(())
}

#[derive(Default, Debug, Clone)]
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    // pub blocks: Vec<BlockRequest>,
    pub transactions: Vec<TransactionRequest>,
    pub logs: Vec<LogRequest>,
    pub traces: Vec<TraceRequest>,
    pub join_mode: JoinMode,
    pub fields: FieldSelection,
}

// impl Query {
//     #[new]
//     #[args(from_block=15)]
//     fn new(from_block: u64, to_block: Option<u64> ) {
//         
//     }
// }
