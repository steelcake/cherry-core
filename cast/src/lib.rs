#![allow(clippy::manual_div_ceil)]

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::{builder, Array, BinaryArray, Decimal256Array, RecordBatch, StringArray},
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
        safe: allow_cast_fail,
        ..Default::default()
    };

    for (col, field) in data.columns().iter().zip(data.schema_ref().fields().iter()) {
        let cast_target = map.iter().find(|x| x.0.as_ref() == field.name());

        let col = match cast_target {
            Some(tgt) => {
                // allow precision loss for decimal types into floating point types
                if matches!(
                    col.data_type(),
                    DataType::Decimal256(..) | DataType::Decimal128(..)
                ) && tgt.1.is_floating()
                {
                    let string_col =
                        arrow::compute::cast_with_options(col, &DataType::Utf8, &cast_opt)
                            .with_context(|| {
                                format!(
                            "Failed when casting column '{}' to string as intermediate step",
                            field.name()
                        )
                            })?;
                    Arc::new(
                        arrow::compute::cast_with_options(&string_col, &tgt.1, &cast_opt)
                            .with_context(|| {
                                format!(
                                    "Failed when casting column '{}' to {:?}",
                                    field.name(),
                                    tgt.1
                                )
                            })?,
                    )
                } else {
                    Arc::new(
                        arrow::compute::cast_with_options(col, &tgt.1, &cast_opt).with_context(
                            || {
                                format!(
                                    "Failed when casting column '{}' from {:?} to {:?}",
                                    field.name(),
                                    col.data_type(),
                                    tgt.1
                                )
                            },
                        )?,
                    )
                }
            }
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

/// Casts all columns with from_type to to_type.
///
/// Returns error if casting a row fails and `allow_cast_fail` is set to `false`.
/// Writes `null` to output if casting a row fails and `allow_cast_fail` is set to `true`.
pub fn cast_by_type(
    data: &RecordBatch,
    from_type: &DataType,
    to_type: &DataType,
    allow_cast_fail: bool,
) -> Result<RecordBatch> {
    let schema =
        cast_schema_by_type(data.schema_ref(), from_type, to_type).context("cast schema")?;

    let mut arrays = Vec::with_capacity(data.num_columns());

    let cast_opt = CastOptions {
        safe: allow_cast_fail,
        ..Default::default()
    };

    for (col, field) in data.columns().iter().zip(data.schema_ref().fields().iter()) {
        let col = if col.data_type() == from_type {
            // allow precision loss for decimal types into floating point types
            if matches!(
                col.data_type(),
                DataType::Decimal256(..) | DataType::Decimal128(..)
            ) && to_type.is_floating()
            {
                let string_col = arrow::compute::cast_with_options(col, &DataType::Utf8, &cast_opt)
                    .with_context(|| {
                        format!(
                            "Failed when casting_by_type column '{}' to string as intermediate step",
                            field.name()
                        )
                    })?;
                Arc::new(
                    arrow::compute::cast_with_options(&string_col, to_type, &cast_opt)
                        .with_context(|| {
                            format!(
                                "Failed when casting_by_type column '{}' to {:?}",
                                field.name(),
                                to_type
                            )
                        })?,
                )
            } else {
                Arc::new(
                    arrow::compute::cast_with_options(col, to_type, &cast_opt).with_context(
                        || {
                            format!(
                                "Failed when casting_by_type column '{}' to {:?}",
                                field.name(),
                                to_type
                            )
                        },
                    )?,
                )
            }
        } else {
            col.clone()
        };

        arrays.push(col);
    }

    let batch = RecordBatch::try_new(Arc::new(schema), arrays).context("construct record batch")?;

    Ok(batch)
}

/// Casts columns with from_type to to_type
pub fn cast_schema_by_type(
    schema: &Schema,
    from_type: &DataType,
    to_type: &DataType,
) -> Result<Schema> {
    let mut fields = schema.fields().to_vec();

    for f in fields.iter_mut() {
        if f.data_type() == from_type {
            *f = Arc::new(Field::new(f.name(), to_type.clone(), f.is_nullable()));
        }
    }

    Ok(Schema::new(fields))
}

pub fn base58_encode(data: &RecordBatch) -> Result<RecordBatch> {
    let schema = schema_binary_to_string(data.schema_ref());
    let mut columns = Vec::<Arc<dyn Array>>::with_capacity(data.columns().len());

    for col in data.columns().iter() {
        if col.data_type() == &DataType::Binary {
            columns.push(Arc::new(base58_encode_column(
                col.as_any().downcast_ref::<BinaryArray>().unwrap(),
            )));
        } else {
            columns.push(col.clone());
        }
    }

    RecordBatch::try_new(Arc::new(schema), columns).context("construct arrow batch")
}

pub fn base58_encode_column(col: &BinaryArray) -> StringArray {
    let mut arr =
        builder::StringBuilder::with_capacity(col.len(), (col.value_data().len() + 2) * 2);

    for v in col.iter() {
        match v {
            Some(v) => {
                let v = bs58::encode(v)
                    .with_alphabet(bs58::Alphabet::BITCOIN)
                    .into_string();
                arr.append_value(v);
            }
            None => arr.append_null(),
        }
    }

    arr.finish()
}

pub fn hex_encode<const PREFIXED: bool>(data: &RecordBatch) -> Result<RecordBatch> {
    let schema = schema_binary_to_string(data.schema_ref());
    let mut columns = Vec::<Arc<dyn Array>>::with_capacity(data.columns().len());

    for col in data.columns().iter() {
        if col.data_type() == &DataType::Binary {
            columns.push(Arc::new(hex_encode_column::<PREFIXED>(
                col.as_any().downcast_ref::<BinaryArray>().unwrap(),
            )));
        } else {
            columns.push(col.clone());
        }
    }

    RecordBatch::try_new(Arc::new(schema), columns).context("construct arrow batch")
}

pub fn hex_encode_column<const PREFIXED: bool>(col: &BinaryArray) -> StringArray {
    let mut arr =
        builder::StringBuilder::with_capacity(col.len(), (col.value_data().len() + 2) * 2);

    for v in col.iter() {
        match v {
            Some(v) => {
                // TODO: avoid allocation here and use a scratch buffer to encode hex into or write to arrow buffer
                // directly somehow.
                let v = if PREFIXED {
                    format!("0x{}", faster_hex::hex_string(v))
                } else {
                    faster_hex::hex_string(v)
                };

                arr.append_value(v);
            }
            None => arr.append_null(),
        }
    }

    arr.finish()
}

/// Converts binary fields to string in the schema
///
/// Intended to be used with encode hex functions
pub fn schema_binary_to_string(schema: &Schema) -> Schema {
    let mut fields = Vec::<Arc<Field>>::with_capacity(schema.fields().len());

    for f in schema.fields().iter() {
        if f.data_type() == &DataType::Binary {
            fields.push(Arc::new(Field::new(
                f.name().clone(),
                DataType::Utf8,
                f.is_nullable(),
            )));
        } else {
            fields.push(f.clone());
        }
    }

    Schema::new(fields)
}

/// Converts decimal256 fields to binary in the schema
///
/// Intended to be used with u256_to_binary function
pub fn schema_decimal256_to_binary(schema: &Schema) -> Schema {
    let mut fields = Vec::<Arc<Field>>::with_capacity(schema.fields().len());

    for f in schema.fields().iter() {
        if f.data_type() == &DataType::Decimal256(76, 0) {
            fields.push(Arc::new(Field::new(
                f.name().clone(),
                DataType::Binary,
                f.is_nullable(),
            )));
        } else {
            fields.push(f.clone());
        }
    }

    Schema::new(fields)
}

pub fn base58_decode_column(col: &StringArray) -> Result<BinaryArray> {
    let mut arr = builder::BinaryBuilder::with_capacity(col.len(), col.value_data().len() / 2);

    for v in col.iter() {
        match v {
            // TODO: this should be optimized by removing allocations if needed
            Some(v) => {
                let v = bs58::decode(v)
                    .with_alphabet(bs58::Alphabet::BITCOIN)
                    .into_vec()
                    .context("bs58 decode")?;
                arr.append_value(v);
            }
            None => arr.append_null(),
        }
    }

    Ok(arr.finish())
}

pub fn hex_decode_column<const PREFIXED: bool>(col: &StringArray) -> Result<BinaryArray> {
    let mut arr = builder::BinaryBuilder::with_capacity(col.len(), col.value_data().len() / 2);

    for v in col.iter() {
        match v {
            // TODO: this should be optimized by removing allocations if needed
            Some(v) => {
                let v = v.as_bytes();
                let v = if PREFIXED {
                    v.get(2..).context("index into prefix hex encoded value")?
                } else {
                    v
                };

                let len = v.len();
                let mut dst = vec![0; (len + 1) / 2];

                faster_hex::hex_decode(v, &mut dst).context("hex decode")?;

                arr.append_value(dst);
            }
            None => arr.append_null(),
        }
    }

    Ok(arr.finish())
}

pub fn u256_column_from_binary(col: &BinaryArray) -> Result<Decimal256Array> {
    let mut arr = builder::Decimal256Builder::with_capacity(col.len());

    for v in col.iter() {
        match v {
            Some(v) => {
                let num = ruint::aliases::U256::try_from_be_slice(v).context("parse ruint u256")?;
                let num = alloy_primitives::I256::try_from(num)
                    .with_context(|| format!("u256 to i256. val was {}", num))?;

                let val = arrow::datatypes::i256::from_be_bytes(num.to_be_bytes::<32>());
                arr.append_value(val);
            }
            None => arr.append_null(),
        }
    }

    Ok(arr.with_precision_and_scale(76, 0).unwrap().finish())
}

pub fn u256_column_to_binary(col: &Decimal256Array) -> Result<BinaryArray> {
    let mut arr = builder::BinaryBuilder::with_capacity(col.len(), col.len() * 32);

    for v in col.iter() {
        match v {
            Some(v) => {
                let num = alloy_primitives::I256::from_be_bytes::<32>(v.to_be_bytes());
                let num = ruint::aliases::U256::try_from(num).context("convert i256 to u256")?;
                arr.append_value(num.to_be_bytes_trimmed_vec());
            }
            None => {
                arr.append_null();
            }
        }
    }

    Ok(arr.finish())
}

/// Converts all Decimal256 (U256) columns in the batch to big endian binary values
pub fn u256_to_binary(data: &RecordBatch) -> Result<RecordBatch> {
    let schema = schema_decimal256_to_binary(data.schema_ref());
    let mut columns = Vec::<Arc<dyn Array>>::with_capacity(data.columns().len());

    for (i, col) in data.columns().iter().enumerate() {
        if col.data_type() == &DataType::Decimal256(76, 0) {
            let col = col.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let x = u256_column_to_binary(col)
                .with_context(|| format!("col {} to binary", data.schema().fields()[i].name()))?;
            columns.push(Arc::new(x));
        } else {
            columns.push(col.clone());
        }
    }

    RecordBatch::try_new(Arc::new(schema), columns).context("construct arrow batch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use std::fs::File;

    #[test]
    #[ignore]
    fn test_cast() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open("data.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let table = reader.next().unwrap().unwrap();

        let type_mappings = vec![
            ("amount0In", DataType::Decimal128(15, 0)),
            ("amount1In", DataType::Float32),
            ("amount0Out", DataType::Float64),
            ("amount1Out", DataType::Decimal128(38, 0)),
            ("timestamp", DataType::Int64),
        ];

        let result = cast(&type_mappings, &table, true).unwrap();

        // Save the filtered instructions to a new parquet file
        let mut file = File::create("result.parquet").unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, result.schema(), None).unwrap();
        writer.write(&result).unwrap();
        writer.close().unwrap();
    }
}
