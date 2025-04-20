use crate::deserialize::{DynType, DynValue};
use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, ListArray};
use arrow::{
    array::{builder, ArrowPrimitiveType, RecordBatch, StructArray},
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::*,
};
use std::sync::Arc;

/// Converts a vector of dynamic values into an Arrow array based on the specified type.
///
/// # Arguments
/// * `param_type` - The type information for the values to convert
/// * `values` - Vector of optional dynamic values to convert
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array wrapped in an Arc
pub fn to_arrow(param_type: &DynType, values: Vec<Option<DynValue>>) -> Result<Arc<dyn Array>> {
    match param_type {
        DynType::I8 => to_number::<Int8Type>(&values),
        DynType::I16 => to_number::<Int16Type>(&values),
        DynType::I32 => to_number::<Int32Type>(&values),
        DynType::I64 => to_number::<Int64Type>(&values),
        DynType::I128 => to_decimal128(&values),
        DynType::U8 => to_number::<UInt8Type>(&values),
        DynType::U16 => to_number::<UInt16Type>(&values),
        DynType::U32 => to_number::<UInt32Type>(&values),
        DynType::U64 => to_number::<UInt64Type>(&values),
        DynType::U128 => to_decimal128(&values),
        DynType::Bool => to_bool(&values),
        DynType::FixedArray(inner_type, size) => to_fixed_array(&values, inner_type, *size),
        DynType::Array(inner_type) => to_list(inner_type, values),
        DynType::Struct(inner_type) => to_struct(inner_type, values),
        DynType::Enum(inner_type) => to_enum(inner_type, values),
        DynType::Option(inner_type) => to_option(inner_type, values),
    }
}

/// Converts a svm dynamic type to its corresponding Arrow data type.
///
/// # Arguments
/// * `param_type` - The svm dynamic type to convert
///
/// # Returns
/// * `Result<DataType>` - The corresponding Arrow data type
pub fn to_arrow_dtype(param_type: &DynType) -> Result<DataType> {
    match param_type {
        DynType::Option(inner_type) => to_arrow_dtype(inner_type),
        DynType::I8 => Ok(DataType::Int8),
        DynType::I16 => Ok(DataType::Int16),
        DynType::I32 => Ok(DataType::Int32),
        DynType::I64 => Ok(DataType::Int64),
        DynType::I128 => Ok(DataType::Decimal128(38, 0)),
        DynType::U8 => Ok(DataType::UInt8),
        DynType::U16 => Ok(DataType::UInt16),
        DynType::U32 => Ok(DataType::UInt32),
        DynType::U64 => Ok(DataType::UInt64),
        DynType::U128 => Ok(DataType::Decimal128(38, 0)),
        DynType::Bool => Ok(DataType::Boolean),
        DynType::FixedArray(inner_type, size) => {
            if **inner_type == DynType::U8 {
                Ok(DataType::Binary)
            } else {
                let inner_type = to_arrow_dtype(inner_type)
                    .context("Failed to convert fixed array inner type to arrow type")?;
                Ok(DataType::FixedSizeList(
                    Arc::new(Field::new("", inner_type, true)),
                    *size as i32,
                ))
            }
        }
        DynType::Array(inner_type) => {
            let inner_type = to_arrow_dtype(inner_type)
                .context("Failed to convert list inner type to arrow type")?;
            Ok(DataType::List(Arc::new(Field::new("", inner_type, true))))
        }
        DynType::Enum(variants) => {
            let variant_fields = variants
                .iter()
                .map(|(name, dt)| match dt {
                    Some(dt) => Ok(Arc::new(Field::new(
                        name,
                        to_arrow_dtype(dt)
                            .context("Failed to convert enum inner variant type to arrow type")?,
                        true,
                    ))),
                    None => Ok(Arc::new(Field::new(name, DataType::Boolean, true))),
                })
                .collect::<Result<Vec<_>>>()
                .context("Failed to map enum type to Arrow data type")?;

            let enum_fields = vec![
                Arc::new(Field::new("variant_index", DataType::UInt8, true)),
                Arc::new(Field::new(
                    "variant_data",
                    DataType::Struct(Fields::from(variant_fields)),
                    true,
                )),
            ];
            Ok(DataType::Struct(Fields::from(enum_fields)))
        }
        DynType::Struct(fields) => {
            let arrow_fields = fields
                .iter()
                .map(|(name, field_type)| {
                    let inner_dt = to_arrow_dtype(field_type)
                        .context("Failed to convert struct inner field type to arrow type")?;
                    Ok(Field::new(name, inner_dt, true))
                })
                .collect::<Result<Vec<_>>>()
                .context("Failed to convert struct fields to arrow fields")?;
            Ok(DataType::Struct(Fields::from(arrow_fields)))
        }
    }
}

/// Converts a vector of optional values into an Arrow array for Option types.
///
/// # Arguments
/// * `inner_type` - The type of the inner value
/// * `values` - Vector of optional dynamic values
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array
fn to_option(inner_type: &DynType, values: Vec<Option<DynValue>>) -> Result<Arc<dyn Array>> {
    let mut opt_values = Vec::with_capacity(values.len());

    for value in values {
        match value {
            None => opt_values.push(None),
            Some(DynValue::Option(inner_val)) => {
                opt_values.push(inner_val.map(|v| *v));
            }
            _ => return Err(anyhow!("Expected option type, found: {:?}", value)),
        }
    }

    to_arrow(inner_type, opt_values)
}

/// Converts a vector of numeric values into an Arrow array of the specified type.
///
/// # Type Parameters
/// * `T` - The Arrow primitive type to convert to
///
/// # Arguments
/// * `values` - Slice of optional dynamic values to convert
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array
fn to_number<T>(values: &[Option<DynValue>]) -> Result<Arc<dyn Array>>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i8>
        + TryFrom<u8>
        + TryFrom<i16>
        + TryFrom<u16>
        + TryFrom<i32>
        + TryFrom<u32>
        + TryFrom<i64>
        + TryFrom<u64>
        + TryFrom<i128>
        + TryFrom<u128>,
{
    let mut builder = builder::PrimitiveBuilder::<T>::with_capacity(values.len());

    for v in values.iter() {
        match v {
            Some(v) => match v {
                DynValue::I8(v) => builder.append_value(
                    T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert DynType i8"))?,
                ),
                DynValue::U8(v) => builder.append_value(
                    T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert DynType u8"))?,
                ),
                DynValue::I16(v) => builder.append_value(
                    T::Native::try_from(*v)
                        .map_err(|_| anyhow!("Failed to convert DynType i16"))?,
                ),
                DynValue::U16(v) => builder.append_value(
                    T::Native::try_from(*v)
                        .map_err(|_| anyhow!("Failed to convert DynType u16"))?,
                ),
                DynValue::I32(v) => builder.append_value(
                    T::Native::try_from(*v)
                        .map_err(|_| anyhow!("Failed to convert DynType i32"))?,
                ),
                DynValue::U32(v) => builder.append_value(
                    T::Native::try_from(*v)
                        .map_err(|_| anyhow!("Failed to convert DynType u32"))?,
                ),
                DynValue::I64(v) => builder.append_value(
                    T::Native::try_from(*v)
                        .map_err(|_| anyhow!("Failed to convert DynType i64"))?,
                ),
                DynValue::U64(v) => builder.append_value(
                    T::Native::try_from(*v)
                        .map_err(|_| anyhow!("Failed to convert DynType u64"))?,
                ),
                DynValue::I128(v) => builder.append_value(
                    T::Native::try_from(*v)
                        .map_err(|_| anyhow!("Failed to convert DynType i128"))?,
                ),
                DynValue::U128(v) => builder.append_value(
                    T::Native::try_from(*v)
                        .map_err(|_| anyhow!("Failed to convert DynType u128"))?,
                ),
                _ => {
                    return Err(anyhow!(
                        "Unexpected value type for number conversion: {:?}",
                        v
                    ))
                }
            },
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Converts a vector of numeric values of type i128 or u128 into an Arrow array with precision 38 and scale 0.
///
/// # Type Parameters
/// * `T` - The Arrow primitive type to convert to
///
/// # Arguments
/// * `values` - Slice of optional dynamic values to convert
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array
fn to_decimal128(values: &[Option<DynValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::PrimitiveBuilder::<Decimal128Type>::with_capacity(values.len())
        .with_precision_and_scale(38, 0)
        .map_err(|_| anyhow!("Failed to configure Decimal128 builder"))?;

    for v in values.iter() {
        match v {
            Some(v) => match v {
                DynValue::I128(v) => builder.append_value(*v),
                DynValue::U128(v) => builder.append_value(*v as i128),
                _ => {
                    return Err(anyhow!(
                        "Unexpected value type for decimal conversion: {:?}",
                        v
                    ))
                }
            },
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Converts a vector of boolean values into an Arrow array.
///
/// # Arguments
/// * `values` - Slice of optional dynamic values to convert
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array
fn to_bool(values: &[Option<DynValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::BooleanBuilder::new();
    for val in values {
        match val {
            Some(DynValue::Bool(b)) => builder.append_value(*b),
            Some(v) => {
                return Err(anyhow!(
                    "found unexpected value. Expected: bool, Found: {:?}",
                    v
                ))
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn to_fixed_array(
    values: &[Option<DynValue>],
    inner_type: &DynType,
    _size: usize,
) -> Result<Arc<dyn Array>> {
    if inner_type == &DynType::U8 {
        to_binary(values)
    } else {
        Err(anyhow!("Unsupported fixed array type: {:?}", inner_type))
    }
}

/// Converts a vector of binary values (pubkeys) into an Arrow array.
///
/// # Arguments
/// * `values` - Slice of optional dynamic values to convert
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array
fn to_binary(value: &[Option<DynValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::BinaryBuilder::new();
    for val in value {
        match val {
            Some(DynValue::Array(data)) => {
                // Collect all u8 values into a Vec<u8>
                let bytes: Result<Vec<u8>> = data
                    .iter()
                    .map(|v| match v {
                        DynValue::U8(v) => Ok(*v),
                        _ => Err(anyhow!("Expected binary type, found: {:?}", v)),
                    })
                    .collect();

                // Append the collected bytes as a single binary value
                builder.append_value(&bytes?);
            }
            Some(val) => return Err(anyhow!("Expected binary type, found: {:?}", val)),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Converts a vector of list values into an Arrow ListArray.
///
/// # Arguments
/// * `param_type` - The type of the list elements
/// * `param_values` - Vector of optional dynamic values to convert
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array
fn to_list(param_type: &DynType, param_values: Vec<Option<DynValue>>) -> Result<Arc<dyn Array>> {
    let mut lengths = Vec::with_capacity(param_values.len());
    let mut inner_values = Vec::with_capacity(param_values.len() * 2);
    let mut validity = Vec::with_capacity(param_values.len());

    let mut all_valid = true;

    for val in param_values {
        match val {
            Some(val) => match val {
                DynValue::Array(inner_vals) => {
                    lengths.push(inner_vals.len());
                    inner_values.extend(inner_vals.into_iter().map(Some));
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

    let arrow_data_type = to_arrow_dtype(param_type)?;
    let list_array_values = to_arrow(param_type, inner_values)
        .context("Failed to convert list elements to arrow array")?;
    let field = Field::new("", arrow_data_type, true);
    let list_arr = ListArray::try_new(
        Arc::new(field),
        OffsetBuffer::from_lengths(lengths),
        list_array_values,
        if all_valid {
            None
        } else {
            Some(NullBuffer::from(validity))
        },
    )
    .context("Failed to construct ListArray from list array values")?;

    Ok(Arc::new(list_arr))
}

/// Converts a vector of enum values into an Arrow struct array.
/// Enum are mapped to a struct type where the enum variant is Vec<(String, DynValue)>, then call to_struct
///
/// # Arguments
/// * `variants` - Vector of (name, type) pairs defining the enum variants
/// * `param_values` - Vector of optional dynamic values to convert
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array
fn to_enum(
    variants: &[(String, Option<DynType>)],
    param_values: Vec<Option<DynValue>>,
) -> Result<Arc<dyn Array>> {
    let mut values = Vec::with_capacity(param_values.len());

    // Helper closure that creates a struct representation for an enum variant
    let make_struct = |variant_name, inner_val: Option<Box<DynValue>>| {
        let mut selected_variant_idx = 0;
        let struct_inner = variants
            .iter()
            .enumerate()
            .map(|(i, (name, dt))| {
                let is_selected_variant = name == &variant_name;
                selected_variant_idx = i as u8;
                match dt {
                    Some(_) => {
                        let data_value = if is_selected_variant {
                            // Only unwrap when we know inner_val is Some
                            inner_val
                                .as_ref()
                                .map(|boxed| DynValue::Option(Some(Box::new((**boxed).clone()))))
                                .unwrap_or(DynValue::Option(None))
                        } else {
                            DynValue::Option(None)
                        };

                        (name.clone(), data_value)
                    }
                    None => (name.clone(), DynValue::Option(None)),
                }
            })
            .collect::<Vec<_>>();

        DynValue::Struct(vec![
            (
                "variant_index".to_string(),
                DynValue::U8(selected_variant_idx),
            ),
            ("variant_data".to_string(), DynValue::Struct(struct_inner)),
        ])
    };

    for val in param_values {
        match val {
            None => {
                values.push(None);
            }
            Some(DynValue::Enum(variant_name, inner_val)) => {
                values.push(Some(make_struct(variant_name, inner_val)));
            }
            Some(_) => return Err(anyhow!("type mismatch")),
        }
    }

    // Convert variants to struct type
    let variants_struct = variants
        .iter()
        .map(|(name, param_type)| match param_type {
            Some(param_type) => (name.clone(), DynType::Option(Box::new(param_type.clone()))),
            None => (name.clone(), DynType::Option(Box::new(DynType::Bool))),
        })
        .collect::<Vec<_>>();

    let enum_struct = vec![
        ("variant_index".to_string(), DynType::U8),
        ("variant_data".to_string(), DynType::Struct(variants_struct)),
    ];

    to_struct(&enum_struct, values)
}

/// Converts a vector of struct values into an Arrow struct array.
///
/// # Arguments
/// * `fields` - Vector of (name, type) pairs defining the struct fields
/// * `param_values` - Vector of optional dynamic values to convert
///
/// # Returns
/// * `Result<Arc<dyn Array>>` - The converted Arrow array
fn to_struct(
    fields: &[(String, DynType)],
    param_values: Vec<Option<DynValue>>,
) -> Result<Arc<dyn Array>> {
    let mut inner_values = vec![Vec::with_capacity(param_values.len()); fields.len()];

    for val in param_values.iter() {
        match val {
            Some(DynValue::Struct(inner_vals)) => {
                if inner_values.len() != inner_vals.len() {
                    return Err(anyhow!(
                        "found unexpected struct length value. Expected: {}, Found: {}",
                        inner_values.len(),
                        inner_vals.len()
                    ));
                }
                for (v, (_, inner)) in inner_values.iter_mut().zip(inner_vals) {
                    v.push(Some(inner.clone()));
                }
            }
            Some(val) => {
                return Err(anyhow!(
                    "found unexpected value. Expected: Struct, Found: {:?}",
                    val
                ))
            }
            None => inner_values.iter_mut().for_each(|v| v.push(None)),
        }
    }

    let mut arrays = Vec::with_capacity(fields.len());

    for ((_, param_type), arr_vals) in fields.iter().zip(inner_values.into_iter()) {
        arrays.push(
            to_arrow(param_type, arr_vals)
                .context("Failed to convert struct inner values to arrow")?,
        );
    }

    let fields = arrays
        .iter()
        .zip(fields.iter())
        .map(|(arr, (name, _))| Field::new(name, arr.data_type().clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema, arrays)
        .context("Failed to create record batch from struct arrays")?;
    Ok(Arc::new(StructArray::from(batch)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deserialize::DynType;
    use std::fs::File;

    #[test]
    #[ignore]
    fn test_nested_dyntypes() {
        // Create a complex nested structure that tests all DynType cases
        let nested_type = DynType::Struct(vec![
            ("i8_value".to_string(), DynType::I8),
            ("bool_value".to_string(), DynType::Bool),
            (
                "pubkey_value".to_string(),
                DynType::FixedArray(Box::new(DynType::U8), 32),
            ),
            (
                "vec_value".to_string(),
                DynType::Array(Box::new(DynType::I8)),
            ),
            (
                "nested_struct".to_string(),
                DynType::Struct(vec![
                    ("inner_i8".to_string(), DynType::I8),
                    ("inner_bool".to_string(), DynType::Bool),
                ]),
            ),
            (
                "nested_enum".to_string(),
                DynType::Enum(vec![
                    ("Variant1".to_string(), Some(DynType::I8)),
                    ("Variant2".to_string(), Some(DynType::Bool)),
                    ("Variant3".to_string(), None),
                ]),
            ),
            (
                "optional_value".to_string(),
                DynType::Option(Box::new(DynType::I8)),
            ),
        ]);

        // Create test data
        let test_data = DynValue::Struct(vec![
            ("i8_value".to_string(), DynValue::I8(42)),
            ("bool_value".to_string(), DynValue::Bool(true)),
            (
                "pubkey_value".to_string(),
                DynValue::Array([0u8; 32].into_iter().map(DynValue::U8).collect()),
            ),
            (
                "vec_value".to_string(),
                DynValue::Array(vec![DynValue::I8(1), DynValue::I8(2), DynValue::I8(3)]),
            ),
            (
                "nested_struct".to_string(),
                DynValue::Struct(vec![
                    ("inner_i8".to_string(), DynValue::I8(100)),
                    ("inner_bool".to_string(), DynValue::Bool(false)),
                ]),
            ),
            (
                "nested_enum".to_string(),
                DynValue::Enum("Variant3".to_string(), None),
            ),
            (
                "optional_value".to_string(),
                DynValue::Option(Some(Box::new(DynValue::I8(127)))),
            ),
        ]);

        // Create a vector of test values
        let test_values = vec![Some(test_data)];

        // Convert to Arrow array
        let arrow_array = to_arrow(&nested_type, test_values).unwrap();
        let arrow_array_clone = arrow_array.clone();

        // Create a schema and record batch
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested_data",
            to_arrow_dtype(&nested_type).unwrap(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![arrow_array]).unwrap();

        // Save to parquet for verification
        let mut file = File::create("nested_dyntypes.parquet").unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Verify the conversion worked by checking the array type
        assert!(arrow_array_clone.as_any().is::<StructArray>());
    }
}
