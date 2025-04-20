use std::sync::Arc;

use alloy_dyn_abi::{DynSolCall, DynSolEvent, DynSolType, DynSolValue, Specifier};
use alloy_primitives::{I256, U256};
use anyhow::{anyhow, Context, Result};
use arrow::{
    array::{builder, Array, ArrowPrimitiveType, BinaryArray, ListArray, RecordBatch, StructArray},
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::{
        DataType, Field, Fields, Int16Type, Int32Type, Int64Type, Int8Type, Schema, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};

/// Returns topic0 based on given event signature
pub fn signature_to_topic0(signature: &str) -> Result<[u8; 32]> {
    let event = alloy_json_abi::Event::parse(signature).context("parse event signature")?;
    Ok(event.selector().into())
}

/// Decodes given call input data in arrow format to arrow format.
/// Output Arrow schema is auto generated based on the function signature.
/// Handles any level of nesting with Lists/Structs.
///
/// Writes `null` for data rows that fail to decode if `allow_decode_fail` is set to `true`.
/// Errors when a row fails to decode if `allow_decode_fail` is set to `false`.
pub fn decode_call_inputs(
    signature: &str,
    data: &BinaryArray,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    decode_call_impl::<true>(signature, data, allow_decode_fail)
}

/// Decodes given call output data in arrow format to arrow format.
/// Output Arrow schema is auto generated based on the function signature.
/// Handles any level of nesting with Lists/Structs.
///
/// Writes `null` for data rows that fail to decode if `allow_decode_fail` is set to `true`.
/// Errors when a row fails to decode if `allow_decode_fail` is set to `false`.
pub fn decode_call_outputs(
    signature: &str,
    data: &BinaryArray,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    decode_call_impl::<false>(signature, data, allow_decode_fail)
}

// IS_INPUT: true means we are decoding inputs
// false means we are decoding outputs
fn decode_call_impl<const IS_INPUT: bool>(
    signature: &str,
    data: &BinaryArray,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    let (call, resolved) = resolve_function_signature(signature)?;

    let schema = function_signature_to_arrow_schemas_impl(&call, &resolved)
        .context("convert event signature to arrow schema")?;
    let schema = if IS_INPUT { schema.0 } else { schema.1 };

    let mut arrays: Vec<Arc<dyn Array + 'static>> = Vec::with_capacity(schema.fields().len());

    let mut decoded = Vec::<Option<DynSolValue>>::with_capacity(data.len());

    for blob in data.iter() {
        match blob {
            Some(blob) => {
                let decode_res = if IS_INPUT {
                    resolved.abi_decode_input(blob, false)
                } else {
                    resolved.abi_decode_output(blob, false)
                };
                match decode_res {
                    Ok(data) => decoded.push(Some(DynSolValue::Tuple(data))),
                    Err(e) if allow_decode_fail => {
                        log::debug!("failed to decode function call data: {}", e);
                        decoded.push(None);
                    }
                    Err(e) => {
                        return Err(anyhow!("failed to decode function call data: {}", e));
                    }
                }
            }
            None => decoded.push(None),
        }
    }

    let sol_type = if IS_INPUT {
        DynSolType::Tuple(resolved.types().to_vec())
    } else {
        DynSolType::Tuple(resolved.returns().types().to_vec())
    };

    let array = to_arrow(&sol_type, decoded, allow_decode_fail).context("map params to arrow")?;
    match array.data_type() {
        DataType::Struct(_) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();

            for f in arr.columns().iter() {
                arrays.push(f.clone());
            }
        }
        _ => unreachable!(),
    }

    RecordBatch::try_new(Arc::new(schema), arrays).context("construct arrow batch")
}

/// Returns (input schema, output schema)
pub fn function_signature_to_arrow_schemas(signature: &str) -> Result<(Schema, Schema)> {
    let (func, resolved) = resolve_function_signature(signature)?;
    function_signature_to_arrow_schemas_impl(&func, &resolved)
}

fn function_signature_to_arrow_schemas_impl(
    func: &alloy_json_abi::Function,
    call: &DynSolCall,
) -> Result<(Schema, Schema)> {
    let mut input_fields = Vec::with_capacity(call.types().len());
    let mut output_fields = Vec::with_capacity(call.returns().types().len());

    for (i, (sol_t, param)) in call.types().iter().zip(func.inputs.iter()).enumerate() {
        let dtype = to_arrow_dtype(sol_t).context("map to arrow type")?;
        let name = if param.name() == "" {
            format!("param{}", i)
        } else {
            param.name().to_owned()
        };
        input_fields.push(Arc::new(Field::new(name, dtype, true)));
    }

    for (i, (sol_t, param)) in call
        .returns()
        .types()
        .iter()
        .zip(func.outputs.iter())
        .enumerate()
    {
        let dtype = to_arrow_dtype(sol_t).context("map to arrow type")?;
        let name = if param.name() == "" {
            format!("param{}", i)
        } else {
            param.name().to_owned()
        };
        output_fields.push(Arc::new(Field::new(name, dtype, true)));
    }

    Ok((Schema::new(input_fields), Schema::new(output_fields)))
}

fn resolve_function_signature(signature: &str) -> Result<(alloy_json_abi::Function, DynSolCall)> {
    let event = alloy_json_abi::Function::parse(signature).context("parse function signature")?;
    let resolved = event.resolve().context("resolve function signature")?;

    Ok((event, resolved))
}

/// Decodes given event data in arrow format to arrow format.
/// Output Arrow schema is auto generated based on the event signature.
/// Handles any level of nesting with Lists/Structs.
///
/// Writes `null` for event data rows that fail to decode if `allow_decode_fail` is set to `true`.
/// Errors when a row fails to decode if `allow_decode_fail` is set to `false`.
pub fn decode_events(
    signature: &str,
    data: &RecordBatch,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    let (event, resolved) = resolve_event_signature(signature)?;

    let schema = event_signature_to_arrow_schema_impl(&event, &resolved)
        .context("convert event signature to arrow schema")?;

    let mut arrays: Vec<Arc<dyn Array + 'static>> = Vec::with_capacity(schema.fields().len());

    for (sol_type, topic_name) in resolved
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

        let mut decoded = Vec::<Option<DynSolValue>>::with_capacity(col.len());

        for blob in col.iter() {
            match blob {
                Some(blob) => match sol_type.abi_decode(blob) {
                    Ok(data) => decoded.push(Some(data)),
                    Err(e) if allow_decode_fail => {
                        log::debug!("failed to decode a topic: {}", e);
                        decoded.push(None);
                    }
                    Err(e) => {
                        return Err(anyhow!("failed to decode a topic: {}", e));
                    }
                },
                None => decoded.push(None),
            }
        }

        arrays.push(to_arrow(sol_type, decoded, allow_decode_fail).context("map topic to arrow")?);
    }

    let body_col = data
        .column_by_name("data")
        .context("get data column")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("get data column as binary")?;
    let body_sol_type = DynSolType::Tuple(resolved.body().to_vec());

    let mut body_decoded = Vec::<Option<DynSolValue>>::with_capacity(body_col.len());

    for blob in body_col.iter() {
        match blob {
            Some(blob) => match body_sol_type.abi_decode_sequence(blob) {
                Ok(data) => body_decoded.push(Some(data)),
                Err(e) if allow_decode_fail => {
                    log::debug!("failed to decode body: {}", e);
                    body_decoded.push(None);
                }
                Err(e) => {
                    return Err(anyhow!("failed to decode body: {}", e));
                }
            },
            None => body_decoded.push(None),
        }
    }

    let body_array =
        to_arrow(&body_sol_type, body_decoded, allow_decode_fail).context("map body to arrow")?;
    match body_array.data_type() {
        DataType::Struct(_) => {
            let arr = body_array.as_any().downcast_ref::<StructArray>().unwrap();

            for f in arr.columns().iter() {
                arrays.push(f.clone());
            }
        }
        _ => unreachable!(),
    }

    RecordBatch::try_new(Arc::new(schema), arrays).context("construct arrow batch")
}

/// Generates Arrow schema based on given event signature
pub fn event_signature_to_arrow_schema(signature: &str) -> Result<Schema> {
    let (resolved, event) = resolve_event_signature(signature)?;
    event_signature_to_arrow_schema_impl(&resolved, &event)
}

fn event_signature_to_arrow_schema_impl(
    sig: &alloy_json_abi::Event,
    event: &DynSolEvent,
) -> Result<Schema> {
    let num_fields = event.indexed().len() + event.body().len();
    let mut fields = Vec::<Arc<Field>>::with_capacity(num_fields);
    let mut names = Vec::with_capacity(num_fields);

    for (i, input) in sig.inputs.iter().enumerate() {
        if input.indexed {
            let name = if input.name.is_empty() {
                format!("param{}", i)
            } else {
                input.name.clone()
            };
            names.push(name);
        }
    }
    for (i, input) in sig.inputs.iter().enumerate() {
        if !input.indexed {
            let name = if input.name.is_empty() {
                format!("param{}", i)
            } else {
                input.name.clone()
            };
            names.push(name);
        }
    }

    for (sol_t, name) in event.indexed().iter().chain(event.body()).zip(names) {
        let dtype = to_arrow_dtype(sol_t).context("map to arrow type")?;
        fields.push(Arc::new(Field::new(name, dtype, true)));
    }

    Ok(Schema::new(fields))
}

fn resolve_event_signature(signature: &str) -> Result<(alloy_json_abi::Event, DynSolEvent)> {
    let event = alloy_json_abi::Event::parse(signature).context("parse event signature")?;
    let resolved = event.resolve().context("resolve event signature")?;

    Ok((event, resolved))
}

fn to_arrow_dtype(sol_type: &DynSolType) -> Result<DataType> {
    match sol_type {
        DynSolType::Bool => Ok(DataType::Boolean),
        DynSolType::Bytes => Ok(DataType::Binary),
        DynSolType::String => Ok(DataType::Utf8),
        DynSolType::Address => Ok(DataType::Binary),
        DynSolType::Int(num_bits) => Ok(num_bits_to_int_type(*num_bits)),
        DynSolType::Uint(num_bits) => Ok(num_bits_to_uint_type(*num_bits)),
        DynSolType::Array(inner_type) => {
            let inner_type = to_arrow_dtype(inner_type).context("map inner")?;
            Ok(DataType::List(Arc::new(Field::new("", inner_type, true))))
        }
        DynSolType::Function => Err(anyhow!(
            "decoding 'Function' typed value in function signature isn't supported."
        )),
        DynSolType::FixedArray(inner_type, _) => {
            let inner_type = to_arrow_dtype(inner_type).context("map inner")?;
            Ok(DataType::List(Arc::new(Field::new("", inner_type, true))))
        }
        DynSolType::Tuple(fields) => {
            let mut arrow_fields = Vec::<Arc<Field>>::with_capacity(fields.len());

            for (i, f) in fields.iter().enumerate() {
                let inner_dt = to_arrow_dtype(f).context("map field dt")?;
                arrow_fields.push(Arc::new(Field::new(format!("param{}", i), inner_dt, true)));
            }

            Ok(DataType::Struct(Fields::from(arrow_fields)))
        }
        DynSolType::FixedBytes(_) => Ok(DataType::Binary),
    }
}

fn num_bits_to_uint_type(num_bits: usize) -> DataType {
    if num_bits <= 8 {
        DataType::UInt8
    } else if num_bits <= 16 {
        DataType::UInt16
    } else if num_bits <= 32 {
        DataType::UInt32
    } else if num_bits <= 64 {
        DataType::UInt64
    } else if num_bits <= 128 {
        DataType::Decimal128(38, 0)
    } else if num_bits <= 256 {
        DataType::Decimal256(76, 0)
    } else {
        unreachable!()
    }
}

fn num_bits_to_int_type(num_bits: usize) -> DataType {
    if num_bits <= 8 {
        DataType::Int8
    } else if num_bits <= 16 {
        DataType::Int16
    } else if num_bits <= 32 {
        DataType::Int32
    } else if num_bits <= 64 {
        DataType::Int64
    } else if num_bits <= 128 {
        DataType::Decimal128(38, 0)
    } else if num_bits <= 256 {
        DataType::Decimal256(76, 0)
    } else {
        unreachable!()
    }
}

fn to_arrow(
    sol_type: &DynSolType,
    sol_values: Vec<Option<DynSolValue>>,
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    match sol_type {
        DynSolType::Bool => to_bool(&sol_values),
        DynSolType::Bytes => to_binary(&sol_values),
        DynSolType::String => to_string(&sol_values),
        DynSolType::Address => to_binary(&sol_values),
        DynSolType::Int(num_bits) => to_int(*num_bits, &sol_values, allow_decode_fail),
        DynSolType::Uint(num_bits) => to_uint(*num_bits, &sol_values, allow_decode_fail),
        DynSolType::Array(inner_type) => to_list(inner_type, sol_values, allow_decode_fail),
        DynSolType::Function => Err(anyhow!(
            "decoding 'Function' typed value in function signature isn't supported."
        )),
        DynSolType::FixedArray(inner_type, _) => to_list(inner_type, sol_values, allow_decode_fail),
        DynSolType::Tuple(fields) => to_struct(fields, sol_values, allow_decode_fail),
        DynSolType::FixedBytes(_) => to_binary(&sol_values),
    }
}

fn to_int(
    num_bits: usize,
    sol_values: &[Option<DynSolValue>],
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    match num_bits_to_int_type(num_bits) {
        DataType::Int8 => to_int_impl::<Int8Type>(num_bits, sol_values),
        DataType::Int16 => to_int_impl::<Int16Type>(num_bits, sol_values),
        DataType::Int32 => to_int_impl::<Int32Type>(num_bits, sol_values),
        DataType::Int64 => to_int_impl::<Int64Type>(num_bits, sol_values),
        DataType::Decimal128(_, _) => to_decimal128(num_bits, sol_values),
        DataType::Decimal256(_, _) => to_decimal256(num_bits, sol_values, allow_decode_fail),
        _ => unreachable!(),
    }
}

fn to_uint(
    num_bits: usize,
    sol_values: &[Option<DynSolValue>],
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    match num_bits_to_int_type(num_bits) {
        DataType::UInt8 => to_int_impl::<UInt8Type>(num_bits, sol_values),
        DataType::UInt16 => to_int_impl::<UInt16Type>(num_bits, sol_values),
        DataType::UInt32 => to_int_impl::<UInt32Type>(num_bits, sol_values),
        DataType::UInt64 => to_int_impl::<UInt64Type>(num_bits, sol_values),
        DataType::Decimal128(_, _) => to_decimal128(num_bits, sol_values),
        DataType::Decimal256(_, _) => to_decimal256(num_bits, sol_values, allow_decode_fail),
        _ => unreachable!(),
    }
}

fn to_decimal128(num_bits: usize, sol_values: &[Option<DynSolValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::Decimal128Builder::new();

    for val in sol_values.iter() {
        match val {
            Some(val) => match val {
                DynSolValue::Int(v, nb) => {
                    assert_eq!(num_bits, *nb);

                    let v = i128::try_from(*v).context("convert to i128")?;

                    builder.append_value(v);
                }
                DynSolValue::Uint(v, nb) => {
                    assert_eq!(num_bits, *nb);

                    let v = i128::try_from(*v).context("convert to i128")?;

                    builder.append_value(v);
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: bool, Found: {:?}",
                        val
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    builder = builder.with_data_type(DataType::Decimal128(38, 0));

    Ok(Arc::new(builder.finish()))
}

fn to_decimal256(
    num_bits: usize,
    sol_values: &[Option<DynSolValue>],
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    let mut builder = builder::Decimal256Builder::new();

    for val in sol_values.iter() {
        match val {
            Some(val) => match val {
                DynSolValue::Int(v, nb) => {
                    assert_eq!(num_bits, *nb);

                    let v = arrow::datatypes::i256::from_be_bytes(v.to_be_bytes::<32>());

                    builder.append_value(v);
                }
                DynSolValue::Uint(v, nb) => {
                    assert_eq!(num_bits, *nb);
                    match I256::try_from(*v).context("try u256 to i256") {
                        Ok(v) => builder.append_value(arrow::datatypes::i256::from_be_bytes(
                            v.to_be_bytes::<32>(),
                        )),
                        Err(e) => {
                            if allow_decode_fail {
                                log::debug!("failed to decode u256: {}", e);
                                builder.append_null();
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: bool, Found: {:?}",
                        val
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    builder = builder.with_data_type(DataType::Decimal256(76, 0));

    Ok(Arc::new(builder.finish()))
}

fn to_int_impl<T>(num_bits: usize, sol_values: &[Option<DynSolValue>]) -> Result<Arc<dyn Array>>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<I256> + TryFrom<U256>,
{
    let mut builder = builder::PrimitiveBuilder::<T>::new();

    for val in sol_values.iter() {
        match val {
            Some(val) => match val {
                DynSolValue::Int(v, nb) => {
                    assert_eq!(num_bits, *nb);
                    builder.append_value(match T::Native::try_from(*v) {
                        Ok(v) => v,
                        Err(_) => unreachable!(),
                    });
                }
                DynSolValue::Uint(v, nb) => {
                    assert_eq!(num_bits, *nb);
                    builder.append_value(match T::Native::try_from(*v) {
                        Ok(v) => v,
                        Err(_) => unreachable!(),
                    });
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: bool, Found: {:?}",
                        val
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn to_list(
    sol_type: &DynSolType,
    sol_values: Vec<Option<DynSolValue>>,
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    let mut lengths = Vec::with_capacity(sol_values.len());
    let mut values = Vec::with_capacity(sol_values.len() * 2);
    let mut validity = Vec::with_capacity(sol_values.len() * 2);

    let mut all_valid = true;

    for val in sol_values {
        match val {
            Some(val) => match val {
                DynSolValue::Array(inner_vals) | DynSolValue::FixedArray(inner_vals) => {
                    lengths.push(inner_vals.len());
                    values.extend(inner_vals.into_iter().map(Some));
                    validity.push(true);
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected list type, Found: {:?}",
                        val
                    ));
                }
            },
            None => {
                lengths.push(0);
                validity.push(false);
                all_valid = false;
            }
        }
    }

    let values = to_arrow(sol_type, values, allow_decode_fail).context("map inner")?;
    let field = Field::new(
        "",
        to_arrow_dtype(sol_type).context("construct data type")?,
        true,
    );
    let list_arr = ListArray::try_new(
        Arc::new(field),
        OffsetBuffer::from_lengths(lengths),
        values,
        if all_valid {
            None
        } else {
            Some(NullBuffer::from(validity))
        },
    )
    .context("construct list array")?;
    Ok(Arc::new(list_arr))
}

fn to_struct(
    fields: &[DynSolType],
    sol_values: Vec<Option<DynSolValue>>,
    allow_decode_fail: bool,
) -> Result<Arc<dyn Array>> {
    let mut values = vec![Vec::with_capacity(sol_values.len()); fields.len()];

    // unpack top layer of sol_values into columnar format
    // since we recurse by calling to_arrow later in the function, this will eventually map to
    // primitive types.
    for val in sol_values.iter() {
        match val {
            Some(val) => match val {
                DynSolValue::Tuple(inner_vals) => {
                    if values.len() != inner_vals.len() {
                        return Err(anyhow!(
                            "found unexpected length tuple value. Expected: {}, Found: {}",
                            values.len(),
                            inner_vals.len()
                        ));
                    }
                    for (v, inner) in values.iter_mut().zip(inner_vals) {
                        v.push(Some(inner.clone()));
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "found unexpected value. Expected: tuple, Found: {:?}",
                        val
                    ));
                }
            },
            None => {
                for v in values.iter_mut() {
                    v.push(None);
                }
            }
        }
    }

    let mut arrays = Vec::with_capacity(fields.len());

    for (sol_type, arr_vals) in fields.iter().zip(values.into_iter()) {
        arrays.push(to_arrow(sol_type, arr_vals, allow_decode_fail)?);
    }

    let fields = arrays
        .iter()
        .enumerate()
        .map(|(i, arr)| Field::new(format!("param{}", i), arr.data_type().clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema, arrays).context("construct record batch")?;

    Ok(Arc::new(StructArray::from(batch)))
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
                    return Err(anyhow!(
                        "found unexpected value. Expected: bool, Found: {:?}",
                        val
                    ));
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
                    return Err(anyhow!(
                        "found unexpected value. Expected a binary type, Found: {:?}",
                        val
                    ));
                }
            },
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
                    return Err(anyhow!(
                        "found unexpected value. Expected string, Found: {:?}",
                        val
                    ));
                }
            },
            None => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn nested_event_signature_to_schema() {
        let sig = "ConfiguredQuests(address editor, uint256[][], address indexed my_addr, (bool,bool[],(bool, uint256[]))[] questDetails)";

        let schema = event_signature_to_arrow_schema(sig).unwrap();

        let expected_schema = Schema::new(vec![
            Arc::new(Field::new("my_addr", DataType::Binary, true)),
            Arc::new(Field::new("editor", DataType::Binary, true)),
            Arc::new(Field::new(
                "param1",
                DataType::List(Arc::new(Field::new(
                    "",
                    DataType::List(Arc::new(Field::new("", DataType::Decimal256(76, 0), true))),
                    true,
                ))),
                true,
            )),
            Arc::new(Field::new(
                "questDetails",
                DataType::List(Arc::new(Field::new(
                    "",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(Field::new("param0", DataType::Boolean, true)),
                        Arc::new(Field::new(
                            "param1",
                            DataType::List(Arc::new(Field::new("", DataType::Boolean, true))),
                            true,
                        )),
                        Arc::new(Field::new(
                            "param2",
                            DataType::Struct(Fields::from(vec![
                                Arc::new(Field::new("param0", DataType::Boolean, true)),
                                Arc::new(Field::new(
                                    "param1",
                                    DataType::List(Arc::new(Field::new(
                                        "",
                                        DataType::Decimal256(76, 0),
                                        true,
                                    ))),
                                    true,
                                )),
                            ])),
                            true,
                        )),
                    ])),
                    true,
                ))),
                true,
            )),
        ]);

        assert_eq!(schema, expected_schema);
    }

    #[test]
    #[ignore]
    fn i256_to_arrow_i256() {
        for val in [
            I256::MIN,
            I256::MAX,
            I256::MAX / I256::try_from(2i32).unwrap(),
        ] {
            let out = arrow::datatypes::i256::from_be_bytes(val.to_be_bytes::<32>());

            assert_eq!(val.to_string(), out.to_string());
        }
    }

    #[test]
    #[ignore]
    fn read_parquet_with_real_data() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open("logs.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let logs = reader.next().unwrap().unwrap();

        let signature =
            "PairCreated(address indexed token0, address indexed token1, address pair,uint256)";

        let decoded = decode_events(signature, &logs, false).unwrap();

        // Save the filtered instructions to a new parquet file
        let mut file = File::create("decoded_logs.parquet").unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, decoded.schema(), None).unwrap();
        writer.write(&decoded).unwrap();
        writer.close().unwrap();
    }
}
