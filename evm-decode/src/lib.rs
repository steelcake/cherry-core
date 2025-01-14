use std::sync::Arc;

use alloy_dyn_abi::{DynSolEvent, DynSolType, DynSolValue, Specifier};
use anyhow::{anyhow, Context, Result};
use arrow::{
    array::{builder, Array, BinaryArray, BooleanBuilder, ListArray, RecordBatch, StructArray}, buffer::{NullBuffer, OffsetBuffer}, datatypes::{DataType, Field, Schema}
};

pub fn decode_events(signature: &str, data: &RecordBatch) -> Result<RecordBatch> {
    let event = resolve_event_signature(signature)?;

    let schema = event_signature_to_arrow_schema_impl(&event)
        .context("convert event signature to arrow schema")?;

    let mut arrays: Vec<Arc<dyn Array + 'static>> = Vec::with_capacity(schema.fields().len());

    for (sol_type, topic_name) in event
        .indexed()
        .iter()
        .zip(["topic1", "topic2", "topic3"].iter())
    {
        let col = data
            .column_by_name(topic_name)
            .context("get topic column")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("get topic column as binary")?;

        let decoded: Vec<Option<DynSolValue>> = col
            .iter()
            .map(|blob| match blob {
                Some(blob) => Some(sol_type.abi_decode(blob).unwrap()),
                None => None,
            })
            .collect();

        arrays.push(map_sol_values_to_arrow(&decoded));
    }

    let body_col = data
        .column_by_name("data")
        .context("get data column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get data column as binary")?;
    let body_sol_type = DynSolType::Tuple(event.body().to_vec());

    let body_decoded: Vec<Option<DynSolValue>> = body_col
        .iter()
        .map(|blob| match blob {
            Some(blob) => Some(body_sol_type.abi_decode_sequence(blob).unwrap()),
            None => None,
        })
        .collect();

    let body_array = map_sol_values_to_arrow(&body_decoded);
    match body_array.data_type() {
        DataType::Struct(fields) => {}
        _ => {}
    }

    arrays.push(map_sol_values_to_arrow(&body_decoded));

    RecordBatch::try_new(Arc::new(schema), arrays).context("construct arrow batch")
}

pub fn event_signature_to_arrow_schema(signature: &str) -> Result<Schema> {
    let event = resolve_event_signature(signature)?;
    event_signature_to_arrow_schema_impl(&event)
}

fn event_signature_to_arrow_schema_impl(event: &DynSolEvent) -> Result<Schema> {
    todo!()
}

fn resolve_event_signature(signature: &str) -> Result<DynSolEvent> {
    let event = alloy_json_abi::Event::parse(signature).context("parse event signature")?;
    event.resolve().context("resolve event signature")
}

fn map_sol_values_to_arrow(
    sol_type: &DynSolType,
    sol_values: &[Option<DynSolValue>],
) -> Arc<dyn Array> {
    match sol_type {}
}

fn to_arrow_dtype(sol_type: &DynSolType) -> Result<DataType> {
    todo!()
}

fn to_arrow(sol_type: &DynSolType, sol_values: Vec<Option<DynSolValue>>) -> Result<Arc<dyn Array>> {
    match sol_type {
        DynSolType::Bool => to_bool(&sol_values),
        DynSolType::Bytes => {
            to_binary(&sol_values)
        }
        DynSolType::String => {
            to_string(&sol_values)
        }
        DynSolType::Address => {
            to_binary(&sol_values)
        }
        DynSolType::Int(_) => {
            to_binary(&sol_values)
        }
        DynSolType::Uint(_) => {
            to_binary(&sol_values)
        }
        DynSolType::Array(inner_type) => {
            to_list(&inner_type, sol_values)
        }
        DynSolType::Function => {
            Err(anyhow!("decoding 'Function' typed value in function signature isn't supported."))
        }
        DynSolType::FixedArray(inner_type, _) => {
            to_list(&inner_type, sol_values)
        }
        DynSolType::Tuple(fields) => {
            to_struct(fields, sol_values)
        }
        DynSolType::FixedBytes(_) => {
            to_binary(&sol_values)
        }
    }
}

fn to_list(sol_type: &DynSolType, sol_values: Vec<Option<DynSolValue>>) -> Result<Arc<dyn Array>> {
    let mut lengths = Vec::with_capacity(sol_values.len());
    let mut values = Vec::with_capacity(sol_values.len() * 2);
    let mut validity = Vec::with_capacity(sol_values.len() * 2);

    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::Array(inner_vals) | DynSolValue::FixedArray(inner_vals) => {
                    lengths.push(inner_vals.len());
                    values.extend(inner_vals.into_iter().map(Some));
                    validity.push(true);
                } 
                _ => {
                    return Err(anyhow!("found unexpected value. Expected list type, Found: {:?}", val));
                }
            }
            None => {
                lengths.push(0);
                validity.push(false);
            }
        }
    }

    let values = to_arrow(sol_type, values).context("map inner")?;
    let field = Field::new("", to_arrow_dtype(sol_type).context("construct data type")?, true); 
    let list_arr = ListArray::try_new(
        Arc::new(field),
        OffsetBuffer::from_lengths(lengths),
        values,
        Some(NullBuffer::from(validity)),
    ).context("construct list array")?;
    Ok(Arc::new(list_arr))
}

fn to_struct(fields: &[DynSolType], sol_values: Vec<Option<DynSolValue>>) -> Result<Arc<dyn Array>> {
    let mut values = vec![Vec::with_capacity(sol_values.len()); fields.len()];

    // unpack top layer of sol_values into columnar format
    // since we recurse by calling to_arrow later in the function, this will eventually map to
    // primitive types.
    for val in sol_values.iter() {
        match val {
            Some(val) => match val {
                DynSolValue::Tuple(inner_vals) => {
                    if values.len() != inner_vals.len() {
                        return Err(anyhow!("found unexpected length tuple value. Expected: {}, Found: {}", values.len(), inner_vals.len()));
                    }
                    for (v, inner) in values.iter_mut().zip(inner_vals) {
                        v.push(Some(inner.clone()));
                    }
                }
                _ => {
                    return Err(anyhow!("found unexpected value. Expected: tuple, Found: {:?}", val));
                }
            }
            None => {
                for v in values.iter_mut() {
                    v.push(None);
                }
            }
        }
    }

    let mut arrays = Vec::with_capacity(fields.len()); 

    for (sol_type, arr_vals) in fields.iter().zip(values.into_iter()) {
        arrays.push(to_arrow(sol_type, arr_vals)?);
    }

    let fields = arrays.iter().enumerate().map(|(i, arr)| Field::new(format!("param{}", i), arr.data_type().clone(), true)).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema, arrays).context("construct record batch")?;

    Ok(Arc::new(StructArray::from(
        batch
    )))
}

fn to_bool(sol_values: &[Option<DynSolValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::BooleanBuilder::new();

    for val in sol_values.iter() {
        match val {
            Some(val) => match val {
                DynSolValue::Bool(b) => {
                    builder.append_value(*b);
                }
                _ => {
                    anyhow!("found unexpected value. Expected: bool, Found: {:?}", val);
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn to_binary(sol_values: &[Option<DynSolValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::BinaryBuilder::new();

    for val in sol_values.iter() {
        match val {
            Some(val) => match val {
                DynSolValue::Bytes(data) => {
                    builder.append_value(data);
                }
                DynSolValue::FixedBytes(data, _) => {
                    builder.append_value(data);
                }
                DynSolValue::Address(data) => {
                    builder.append_value(data);
                }
                DynSolValue::Uint(v, _) => {
                    builder.append_value(v.to_be_bytes::<32>());
                }
                DynSolValue::Int(v, _) => {
                    builder.append_value(v.to_be_bytes::<32>());
                }
                _ => {
                    anyhow!("found unexpected value. Expected a binary type, Found: {:?}", val);
                }
            }
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn to_string(sol_values: &[Option<DynSolValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::StringBuilder::new();

    for val in sol_values.iter() {
        match val {
            Some(val) => match val {
                DynSolValue::String(s) => {
                    builder.append_value(s);
                }
                _ => {
                    anyhow!("found unexpected value. Expected string, Found: {:?}", val);
                }
            }
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}
