use anyhow::{anyhow, Context, Result};

/// Represents a parameter input with a name and dynamic type
#[derive(Debug, Clone)]
pub struct ParamInput {
    pub name: String,
    pub param_type: DynType,
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for ParamInput {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;

        let name = ob.getattr("name")?.extract::<String>()?;
        let param_type = ob.getattr("param_type")?.extract::<DynType>()?;
        Ok(ParamInput { name, param_type })
    }
}

/// Represents a dynamic type that can be deserialized from binary data
#[derive(Debug, Clone, PartialEq)]
pub enum DynType {
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    Bool,
    /// Complex types
    FixedArray(Box<DynType>, usize),
    Array(Box<DynType>),
    Struct(Vec<(String, DynType)>),
    Enum(Vec<(String, Option<DynType>)>),
    Option(Box<DynType>),
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for DynType {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;
        
        let out: &str = ob.extract().context("Failed to extract DynType as string in the python binding")?;
        println!("DynType binding: {}", out);
        match out {
            "i8" => Ok(DynType::I8),
            "i16" => Ok(DynType::I16),
            "i32" => Ok(DynType::I32),
            "i64" => Ok(DynType::I64),
            "i128" => Ok(DynType::I128),
            "u8" => Ok(DynType::U8),
            "u16" => Ok(DynType::U16),
            "u32" => Ok(DynType::U32),
            "u64" => Ok(DynType::U64),
            "u128" => Ok(DynType::U128),
            "bool" => Ok(DynType::Bool),            
            _ => Err(anyhow!("Not yet implemented type: {}", out).into()),
        }
    }
}

/// Represents a dynamically deserialized value
#[derive(Debug, Clone)]
pub enum DynValue {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    Bool(bool),
    /// Complex values
    Array(Vec<DynValue>),
    Struct(Vec<(String, DynValue)>),
    Enum(String, Option<Box<DynValue>>),
    Option(Option<Box<DynValue>>),
}

/// Deserializes binary data into a vector of dynamic values based on the provided parameter types
///
/// # Arguments
/// * `data` - The binary data to deserialize
/// * `params` - The parameter types that define the structure of the data
///
/// # Returns
/// A vector of deserialized values matching the parameter types
///
/// # Errors
/// Returns an error if:
/// * There is not enough data to deserialize all parameters
/// * The data format doesn't match the expected parameter types
/// * There is remaining data after deserializing all parameters
pub fn deserialize_data(data: &[u8], params: &[ParamInput]) -> Result<Vec<DynValue>> {
    let mut ix_values = Vec::with_capacity(params.len());
    let mut remaining_data = data;

    for param in params {
        // Deserialize value based on type
        let (value, new_data) = deserialize_value(&param.param_type, remaining_data)?;
        ix_values.push(value);
        remaining_data = new_data;
    }

    if !remaining_data.is_empty() {
        return Err(anyhow!(
            "Remaining data after deserialization: {:?}",
            remaining_data
        ));
    }

    Ok(ix_values)
}

/// Deserializes a single value of the specified type from binary data
///
/// # Arguments
/// * `param_type` - The type of value to deserialize
/// * `data` - The binary data to deserialize from
///
/// # Returns
/// A tuple containing:
/// * The deserialized value
/// * The remaining data after deserialization
///
/// # Errors
/// Returns an error if:
/// * There is not enough data to deserialize the value
/// * The data format doesn't match the expected type
fn deserialize_value<'a>(param_type: &DynType, data: &'a [u8]) -> Result<(DynValue, &'a [u8])> {
    match param_type {
        DynType::Option(inner_type) => {
            let value = data.first().context("Not enough data for option")?;
            match value {
                0 => Ok((DynValue::Option(None), &data[1..])),
                1 => {
                    let (value, new_data) = deserialize_value(inner_type, &data[1..])?;
                    Ok((DynValue::Option(Some(Box::new(value))), new_data))
                }
                _ => Err(anyhow!("Invalid option value: {}", value)),
            }
        }
        DynType::I8 => {
            if data.is_empty() {
                return Err(anyhow!(
                    "Not enough data for i8: expected 1 byte, got {}",
                    data.len()
                ));
            }
            let value = i8::from_le_bytes(data[..1].try_into().unwrap());
            Ok((DynValue::I8(value), &data[1..]))
        }
        DynType::I16 => {
            if data.len() < 2 {
                return Err(anyhow!(
                    "Not enough data for i16: expected 2 bytes, got {}",
                    data.len()
                ));
            }
            let value = i16::from_le_bytes(data[..2].try_into().unwrap());
            Ok((DynValue::I16(value), &data[2..]))
        }
        DynType::I32 => {
            if data.len() < 4 {
                return Err(anyhow!(
                    "Not enough data for i32: expected 4 bytes, got {}",
                    data.len()
                ));
            }
            let value = i32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((DynValue::I32(value), &data[4..]))
        }
        DynType::I64 => {
            if data.len() < 8 {
                return Err(anyhow!(
                    "Not enough data for i64: expected 8 bytes, got {}",
                    data.len()
                ));
            }
            let value = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((DynValue::I64(value), &data[8..]))
        }
        DynType::I128 => {
            if data.len() < 16 {
                return Err(anyhow!(
                    "Not enough data for i128: expected 16 bytes, got {}",
                    data.len()
                ));
            }
            let value = i128::from_le_bytes(data[..16].try_into().unwrap());
            Ok((DynValue::I128(value), &data[16..]))
        }
        DynType::U8 => {
            if data.is_empty() {
                return Err(anyhow!("Not enough data for u8: expected 1 byte, got 0"));
            }
            let value = data[0];
            Ok((DynValue::U8(value), &data[1..]))
        }
        DynType::U16 => {
            if data.len() < 2 {
                return Err(anyhow!(
                    "Not enough data for u16: expected 2 bytes, got {}",
                    data.len()
                ));
            }
            let value = u16::from_le_bytes(data[..2].try_into().unwrap());
            Ok((DynValue::U16(value), &data[2..]))
        }
        DynType::U32 => {
            if data.len() < 4 {
                return Err(anyhow!(
                    "Not enough data for u32: expected 4 bytes, got {}",
                    data.len()
                ));
            }
            let value = u32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((DynValue::U32(value), &data[4..]))
        }
        DynType::U64 => {
            if data.len() < 8 {
                return Err(anyhow!(
                    "Not enough data for u64: expected 8 bytes, got {}",
                    data.len()
                ));
            }
            let value = u64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((DynValue::U64(value), &data[8..]))
        }
        DynType::U128 => {
            if data.len() < 16 {
                return Err(anyhow!(
                    "Not enough data for u128: expected 16 bytes, got {}",
                    data.len()
                ));
            }
            let value = u128::from_le_bytes(data[..16].try_into().unwrap());
            Ok((DynValue::U128(value), &data[16..]))
        }
        DynType::Bool => {
            if data.is_empty() {
                return Err(anyhow!("Not enough data for bool: expected 1 byte, got 0"));
            }
            let value = data[0] != 0;
            Ok((DynValue::Bool(value), &data[1..]))
        }
        DynType::FixedArray(inner_type, size) => {
            let inner_type_size = check_type_size(inner_type)?;
            let total_size = inner_type_size * size;

            if data.len() < total_size {
                return Err(anyhow!(
                    "Not enough data for fixed array: expected {} bytes, got {}",
                    total_size,
                    data.len()
                ));
            }
            let value = data[..total_size]
                .to_vec()
                .chunks(inner_type_size)
                .map(|chunk| {
                    let (value, _) = deserialize_value(inner_type, chunk)?;
                    Ok(value)
                })
                .collect::<Result<Vec<DynValue>>>()?;
            Ok((DynValue::Array(value), &data[total_size..]))
        }
        DynType::Array(inner_type) => {
            if data.len() < 4 {
                return Err(anyhow!(
                    "Not enough data for vector length: expected 4 bytes, got {}",
                    data.len()
                ));
            }
            let length = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            let mut remaining_data = &data[4..];

            let mut values = Vec::with_capacity(length);
            for _ in 0..length {
                let (value, new_data) = deserialize_value(inner_type, remaining_data)?;
                values.push(value);
                remaining_data = new_data;
            }

            Ok((DynValue::Array(values), remaining_data))
        }
        DynType::Struct(fields) => {
            let mut values = Vec::new();
            let mut remaining_data = data;
            for field in fields {
                let (value, new_data) = deserialize_value(&field.1, remaining_data)?;
                values.push((field.0.clone(), value));
                remaining_data = new_data;
            }
            Ok((DynValue::Struct(values), remaining_data))
        }
        DynType::Enum(variants) => {
            if data.is_empty() {
                return Err(anyhow!(
                    "Not enough data for enum: expected at least 1 byte for variant index"
                ));
            }
            let variant_index = data[0] as usize;
            let remaining_data = &data[1..];

            if variant_index >= variants.len() {
                return Err(anyhow!("Invalid enum variant index: {}", variant_index));
            }

            let (variant_name, variant_type) = &variants[variant_index];

            if let Some(variant_type) = variant_type {
                let (variant_value, new_data) = deserialize_value(variant_type, remaining_data)?;
                Ok((
                    DynValue::Enum(variant_name.clone(), Some(Box::new(variant_value))),
                    new_data,
                ))
            } else {
                Ok((DynValue::Enum(variant_name.clone(), None), remaining_data))
            }
        }
    }
}

fn check_type_size(param_type: &DynType) -> Result<usize> {
    match param_type {
        DynType::U8 => Ok(1),
        DynType::U16 => Ok(2),
        DynType::U32 => Ok(4),
        DynType::U64 => Ok(8),
        DynType::U128 => Ok(16),
        DynType::I8 => Ok(1),
        DynType::I16 => Ok(2),
        DynType::I32 => Ok(4),
        DynType::I64 => Ok(8),
        DynType::I128 => Ok(16),
        DynType::Bool => Ok(1),
        _ => Err(anyhow!("Unsupported primitive type for fixed array")),
    }
}
