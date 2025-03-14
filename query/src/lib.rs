use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray};
use arrow::compute;
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

type TableName = String;
type FieldName = String;

pub struct Query {
    pub selection: BTreeMap<TableName, Vec<TableSelection>>,
    pub fields: BTreeMap<TableName, Vec<FieldName>>,
}

pub struct TableSelection {
    pub filters: BTreeMap<FieldName, Arc<dyn Array>>,
    pub include: Vec<Include>,
}

pub struct Include {
    pub other_table_name: TableName,
    pub field_names: Vec<FieldName>,
    pub other_table_field_names: Vec<FieldName>,
}

pub fn run_query(
    data: &BTreeMap<TableName, RecordBatch>,
    query: &Query,
) -> Result<BTreeMap<TableName, RecordBatch>> {
    let filters = query
        .selection
        .par_iter()
        .map(|(table_name, selections)| {
            selections
                .par_iter()
                .enumerate()
                .map(|(i, selection)| {
                    run_table_selection(data, table_name, selection)
                        .with_context(|| format!("run table selection {} for {}", i, table_name))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    let data = select_fields(data, &query.fields).context("select fields")?;

    data.par_iter()
        .filter_map(|(table_name, table_data)| {
            let mut combined_filter: Option<BooleanArray> = None;

            for f in filters.iter() {
                for f in f.iter() {
                    let filter = match f.get(table_name) {
                        Some(f) => f,
                        None => continue,
                    };

                    match combined_filter.as_ref() {
                        Some(e) => {
                            let f = compute::kernels::boolean::or(e, &filter)
                                .with_context(|| format!("combine filters for {}", table_name));
                            let f = match f {
                                Ok(v) => v,
                                Err(err) => return Some(Err(err)),
                            };
                            combined_filter = Some(f);
                        }
                        None => {
                            combined_filter = Some(filter.clone());
                        }
                    }
                }
            }

            let combined_filter = match combined_filter {
                Some(f) => f,
                None => return None,
            };

            let table_data =
                compute::kernels::filter::filter_record_batch(table_data, &combined_filter)
                    .context("filter record batch");
            let table_data = match table_data {
                Ok(v) => v,
                Err(err) => return Some(Err(err)),
            };

            Some(Ok((table_name.to_owned(), table_data)))
        })
        .collect()
}

fn select_fields(
    data: &BTreeMap<TableName, RecordBatch>,
    fields: &BTreeMap<TableName, Vec<FieldName>>,
) -> Result<BTreeMap<TableName, RecordBatch>> {
    let mut out = BTreeMap::new();

    for (table_name, field_names) in fields.iter() {
        let table_data = data
            .get(table_name)
            .with_context(|| format!("get data for table {}", table_name))?;

        let indices = field_names
            .iter()
            .map(|n| {
                table_data
                    .schema_ref()
                    .index_of(n)
                    .with_context(|| format!("find index of field {} in table {}", n, table_name))
            })
            .collect::<Result<Vec<usize>>>()?;

        let table_data = table_data
            .project(&indices)
            .with_context(|| format!("project table {}", table_name))?;
        out.insert(table_name.to_owned(), table_data);
    }

    Ok(out)
}

fn run_table_selection(
    data: &BTreeMap<TableName, RecordBatch>,
    table_name: &str,
    selection: &TableSelection,
) -> Result<BTreeMap<TableName, BooleanArray>> {
    todo!()
}

