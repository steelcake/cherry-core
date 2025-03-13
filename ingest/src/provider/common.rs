use arrow::record_batch::RecordBatch;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

pub fn field_selection_to_set<S: Serialize>(field_selection: &S) -> BTreeSet<String> {
    let json = serde_json::to_value(field_selection).unwrap();
    let json = json.as_object().unwrap();

    let mut output = BTreeSet::new();

    for (key, value) in json.iter() {
        if value.as_bool().unwrap() {
            output.insert(key.clone());
        }
    }

    output
}

pub fn prune_fields<S: Serialize>(data: &mut BTreeMap<String, RecordBatch>, selection: &S) {
    let val = serde_json::to_value(selection).unwrap();
    let obj = val.as_object().unwrap();

    for (key, val) in obj.iter() {
        let data_table_name = format!("{}s", key);
        let mut empty = true;

        let obj = val.as_object().unwrap();

        for (field_name, is_enabled) in obj.iter() {
            let is_enabled = is_enabled.as_bool().unwrap();

            let batch = data.get_mut(&data_table_name).unwrap();
            let index = batch.schema().index_of(field_name).unwrap();
            if !is_enabled {
                batch.remove_column(index);
            } else {
                empty = false;
            }
        }

        if empty {
            data.remove(&data_table_name).unwrap();
        }
    }
}
