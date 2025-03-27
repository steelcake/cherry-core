use anyhow::{anyhow, Result};
use anchor_lang::prelude::Pubkey;

/// Represents a parameter input with a name and dynamic type
pub struct ParamInput {
    pub name: String,
    pub param_type: DynType,
}

/// Represents a dynamic type that can be deserialized from binary data
#[derive(Debug, Clone, PartialEq)]
pub enum DynType {
    /// A defined type that doesn't require deserialization
    Defined,
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
    F32,
    F64,
    Bool,
    Char,
    /// Solana specific types
    Pubkey,
    /// Complex types
    Vec(Box<DynType>),
    Struct(Vec<(String, DynType)>),
    Enum(Vec<(String, DynType)>),
}

/// Represents a dynamically deserialized value
#[derive(Debug, Clone)]
pub enum DynValue {
    /// A defined value that doesn't require deserialization
    Defined,
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
    F32(f32),
    F64(f64),
    Bool(bool),
    Char(char),
    /// Solana specific values
    Pubkey(Pubkey),
    /// Complex values
    Vec(Vec<DynValue>),
    Struct(Vec<(String, DynValue)>),
    Enum(String, Box<DynValue>),
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
pub fn deserialize_data(data: &mut [u8], params: &[ParamInput]) -> Result<Vec<DynValue>> {
    let mut ix_values = Vec::with_capacity(params.len());
    let mut remaining_data = data;
    
    for param in params {           
        // Deserialize value based on type
        let (value, new_data) = deserialize_value(&param.param_type, remaining_data)?;
        ix_values.push(value);
        remaining_data = new_data;
    }
    
    if !remaining_data.is_empty() {
        return Err(anyhow!("Remaining data after deserialization: {} bytes", remaining_data.len()));
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
fn deserialize_value<'a>(param_type: &DynType, data: &'a mut [u8]) -> Result<(DynValue, &'a mut [u8])> {
    match param_type {
        DynType::Defined => Ok((DynValue::Defined, data)),
        DynType::I8 => {
            if data.is_empty() {
                return Err(anyhow!("Not enough data for i8: expected 1 byte, got {}", data.len()));
            }
            let value = i8::from_le_bytes(data[..1].try_into().unwrap());
            Ok((DynValue::I8(value), &mut data[1..]))
        }
        DynType::I16 => {
            if data.len() < 2 {
                return Err(anyhow!("Not enough data for i16: expected 2 bytes, got {}", data.len()));
            }
            let value = i16::from_le_bytes(data[..2].try_into().unwrap());
            Ok((DynValue::I16(value), &mut data[2..]))
        }
        DynType::I32 => {
            if data.len() < 4 {
                return Err(anyhow!("Not enough data for i32: expected 4 bytes, got {}", data.len()));
            }
            let value = i32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((DynValue::I32(value), &mut data[4..]))
        }
        DynType::I64 => {
            if data.len() < 8 {
                return Err(anyhow!("Not enough data for i64: expected 8 bytes, got {}", data.len()));
            }
            let value = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((DynValue::I64(value), &mut data[8..]))
        }
        DynType::I128 => {
            if data.len() < 16 {
                return Err(anyhow!("Not enough data for i128: expected 16 bytes, got {}", data.len()));
            }
            let value = i128::from_le_bytes(data[..16].try_into().unwrap());
            Ok((DynValue::I128(value), &mut data[16..]))
        }
        DynType::U8 => {
            if data.is_empty() {
                return Err(anyhow!("Not enough data for u8: expected 1 byte, got 0"));
            }
            let value = data[0];
            Ok((DynValue::U8(value), &mut data[1..]))
        }
        DynType::U16 => {
            if data.len() < 2 {
                return Err(anyhow!("Not enough data for u16: expected 2 bytes, got {}", data.len()));
            }
            let value = u16::from_le_bytes(data[..2].try_into().unwrap());
            Ok((DynValue::U16(value), &mut data[2..]))
        }
        DynType::U32 => {
            if data.len() < 4 {
                return Err(anyhow!("Not enough data for u32: expected 4 bytes, got {}", data.len()));
            }
            let value = u32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((DynValue::U32(value), &mut data[4..]))
        }
        DynType::U64 => {
            if data.len() < 8 {
                return Err(anyhow!("Not enough data for u64: expected 8 bytes, got {}", data.len()));
            }
            let value = u64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((DynValue::U64(value), &mut data[8..]))
        }
        DynType::U128 => {
            if data.len() < 16 {
                return Err(anyhow!("Not enough data for u128: expected 16 bytes, got {}", data.len()));
            }
            let value = u128::from_le_bytes(data[..16].try_into().unwrap());
            Ok((DynValue::U128(value), &mut data[16..]))
        }
        DynType::F32 => {
            if data.len() < 4 {
                return Err(anyhow!("Not enough data for f32: expected 4 bytes, got {}", data.len()));
            }
            let value = f32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((DynValue::F32(value), &mut data[4..]))
        }
        DynType::F64 => {
            if data.len() < 8 {
                return Err(anyhow!("Not enough data for f64: expected 8 bytes, got {}", data.len()));
            }
            let value = f64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((DynValue::F64(value), &mut data[8..]))
        }
        DynType::Bool => {
            if data.is_empty() {
                return Err(anyhow!("Not enough data for bool: expected 1 byte, got 0"));
            }
            let value = data[0] != 0;
            Ok((DynValue::Bool(value), &mut data[1..]))
        }
        DynType::Char => {
            if data.len() < 4 {
                return Err(anyhow!("Not enough data for char: expected 4 bytes, got {}", data.len()));
            }
            let value = u32::from_le_bytes(data[..4].try_into().unwrap());
            let char_value = char::from_u32(value)
                .ok_or_else(|| anyhow!("Invalid Unicode scalar value: {}", value))?;
            Ok((DynValue::Char(char_value), &mut data[4..]))
        }
        DynType::Pubkey => {
            if data.len() < 32 {
                return Err(anyhow!("Not enough data for pubkey: expected 32 bytes, got {}", data.len()));
            }
            let value = Pubkey::new_from_array(data[..32].try_into().unwrap());
            Ok((DynValue::Pubkey(value), &mut data[32..]))
        }
        DynType::Vec(inner_type) => {
            if data.len() < 4 {
                return Err(anyhow!("Not enough data for vector length: expected 4 bytes, got {}", data.len()));
            }
            let length = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            let mut remaining_data = &mut data[4..];
            
            let mut values = Vec::with_capacity(length);
            for _ in 0..length {
                let (value, new_data) = deserialize_value(inner_type, remaining_data)?;
                values.push(value);
                remaining_data = new_data;
            }
            
            Ok((DynValue::Vec(values), remaining_data))
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
                return Err(anyhow!("Not enough data for enum: expected at least 1 byte for variant index"));
            }
            let data_len = data.len();
            let variant_index = data[0] as usize;
            let mut remaining_data = &mut data[1..];

            if variant_index >= variants.len() {
                return Err(anyhow!("Invalid enum variant index: {}", variant_index));
            }

            let (variant_name, variant_type) = &variants[variant_index];
            
            let variant_value = if data_len == 1 && variant_type == &DynType::Bool {
                DynValue::Defined
            } else {
                let (value, new_data) = deserialize_value(variant_type, remaining_data)?;
                remaining_data = new_data;
                value
            };

            Ok((DynValue::Enum(variant_name.clone(), Box::new(variant_value)), remaining_data))
        }
    }
}