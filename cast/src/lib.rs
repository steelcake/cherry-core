use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::RecordBatch,
    compute::CastOptions,
    datatypes::{DataType, Field, Schema},
};

/// Casts columns according to given (column name, target data type) pairs.
///
/// Returns error if casting a row fails and `allow_cast_fail` is set to `false`.
/// Writes `null` to output if casting a row fails and `allow_cast_fail` is set to `true`.
pub fn cast<S: AsRef<str>>(
    map: &[(S, DataType)],
    data: &RecordBatch,
    allow_cast_fail: bool,
) -> Result<RecordBatch> {
    let schema = cast_schema(map, data.schema_ref()).context("cast schema")?;

    let mut arrays = Vec::with_capacity(data.num_columns());

    let cast_opt = CastOptions {
        safe: !allow_cast_fail,
        ..Default::default()
    };

    for (col, field) in data.columns().iter().zip(data.schema_ref().fields().iter()) {
        let cast_target = map.iter().find(|x| x.0.as_ref() == field.name());

        let col = match cast_target {
            Some(tgt) => Arc::new(
                arrow::compute::cast_with_options(col, &tgt.1, &cast_opt)
                    .with_context(|| format!("Failed when casting column '{}'", field.name()))?,
            ),
            None => col.clone(),
        };

        arrays.push(col);
    }

    let batch = RecordBatch::try_new(Arc::new(schema), arrays).context("construct record batch")?;

    Ok(batch)
}

/// Casts column types according to given (column name, target data type) pairs.
pub fn cast_schema<S: AsRef<str>>(map: &[(S, DataType)], schema: &Schema) -> Result<Schema> {
    let mut fields = schema.fields().to_vec();

    for f in fields.iter_mut() {
        let cast_target = map.iter().find(|x| x.0.as_ref() == f.name());

        if let Some(tgt) = cast_target {
            *f = Arc::new(Field::new(f.name(), tgt.1.clone(), f.is_nullable()));
        }
    }

    Ok(Schema::new(fields))
}
