use alloy_primitives::map::HashMap;
use arrow::array::RecordBatch;
use anyhow::Result;

pub enum DataType {

}

pub enum HexOutput {
    NoEncode,
    Prefixed,
    NonPrefixed,
}

pub struct Config {
    hex_output: HexOutput,
    
}

pub fn map_columns(data: RecordBatch, mapping: HashMap<String, (DataType, DataType)>) -> Result<RecordBatch> {
    todo!()
}
