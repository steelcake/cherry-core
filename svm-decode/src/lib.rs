use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::{array::{builder, ArrowPrimitiveType, NullArray, RecordBatch, StructArray}, buffer::{NullBuffer, OffsetBuffer}, compute::filter_record_batch, datatypes::*};
use std::{fs::File, ops::DerefMut, sync::Arc};
use anchor_lang::prelude::Pubkey;
use arrow::array::{Array, BinaryArray, ListArray, UInt64Array};
use anyhow::{anyhow, Result};
mod deserialize;
use deserialize::{deserialize_data, DynType, DynValue, ParamInput};

pub struct InstructionSignature {
    pub program_id: Pubkey,
    pub name: String,
    pub discriminator: &'static [u8],
    pub sec_discriminator: Option<&'static [u8]>,
    pub params: Vec<ParamInput>,
    pub accounts: Vec<String>,
}

pub fn decode_batch(batch: &RecordBatch, signature: InstructionSignature) -> Result<RecordBatch> {
    let program_id_col = batch.column_by_name("program_id").unwrap();
    let program_id_array = program_id_col.as_any().downcast_ref::<BinaryArray>().unwrap();

    let data_col = batch.column_by_name("data").unwrap();
    let data_array = data_col.as_any().downcast_ref::<BinaryArray>().unwrap();

    let rest_of_accounts_col = batch.column_by_name("rest_of_accounts").unwrap();
    let rest_of_accounts_array = rest_of_accounts_col.as_any().downcast_ref::<ListArray>().unwrap();
    let account_arrays: Vec<&BinaryArray> = (0..10).map(|i| {
        let col_name = format!("a{}", i);
        let col = batch.column_by_name(&col_name).unwrap();
        col.as_any().downcast_ref::<BinaryArray>().unwrap()
    }).collect();

    let num_params = signature.params.len();
    let mut decoded_instructions:Vec<Vec<DynValue>>  = Vec::new();
    let mut exploded_values: Vec<Vec<Option<DynValue>>> = (0..num_params)
        .map(|_| Vec::new())
        .collect();
    
    for row_idx in 0..batch.num_rows() {
        let instr_program_id: [u8; 32] = program_id_array.value(row_idx).try_into().unwrap();
        let instr_program_id = Pubkey::new_from_array(instr_program_id);
        if instr_program_id != signature.program_id {
            println!("Instruction program id doesn't match signature program id");
            exploded_values.iter_mut().for_each(|v| v.push(None));
            continue;
        }

        if data_array.is_null(row_idx) {
            println!("Instruction data is null");
            exploded_values.iter_mut().for_each(|v| v.push(None));
            continue;
        }

        let instruction_data = data_array.value(row_idx);
        let data_result = match_discriminators(&instruction_data, signature.discriminator, signature.sec_discriminator);
        let mut data = match data_result {
            Ok(data) => data,
            Err(e) => {
                println!("Error matching discriminators: {:?}", e);
                exploded_values.iter_mut().for_each(|v| v.push(None));
                continue;
            }
        };

        let decoded_ix_result = deserialize_data(&mut data, &signature.params);
        let decoded_ix = match decoded_ix_result {
            Ok(ix) => ix,
            Err(e) => {
                println!("Error deserializing instruction: {:?}", e);
                exploded_values.iter_mut().for_each(|v| v.push(None));
                continue;
            }
        };
        
        println!("Decoded instruction: {:?}", decoded_ix);
        for (i, value) in decoded_ix.into_iter().enumerate() {
            exploded_values[i].push(Some(value));
        }
    }

    let data_arrays: Vec<Arc<dyn Array>> = exploded_values.iter().enumerate().map(|(i, v)| 
        to_arrow(&signature.params[i].param_type, v.clone())
    ).collect::<Result<Vec<_>>>().unwrap();

    let schema = Arc::new(Schema::new(signature.params.iter().map(|p| Field::new(p.name.clone(), to_arrow_dtype(&p.param_type).unwrap(), true)).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(schema, data_arrays)?;

    // Save the filtered instructions to a new parquet file
    let mut file = File::create("decoded_instructions.parquet").unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    Ok(batch)
}

fn to_arrow(param_type: &DynType, values: Vec<Option<DynValue>>) -> Result<Arc<dyn Array>> {
    match param_type {
        DynType::Defined => Ok(Arc::new(NullArray::new(values.len()))),
        DynType::I8 => to_number::<Int8Type>(8, &values),
        DynType::I16 => to_number::<Int16Type>(16, &values),
        DynType::I32 => to_number::<Int32Type>(32, &values),
        DynType::I64 => to_number::<Int64Type>(64, &values),
        DynType::I128 => to_number::<Decimal128Type>(128, &values),
        DynType::U8 => to_number::<UInt8Type>(8, &values),
        DynType::U16 => to_number::<UInt16Type>(16, &values),
        DynType::U32 => to_number::<UInt32Type>(32, &values),
        DynType::U64 => to_number::<UInt64Type>(64, &values),
        DynType::U128 => to_number::<Decimal128Type>(128, &values),
        DynType::Bool => to_bool(&values),
        DynType::Pubkey => to_binary(&values),
        DynType::Vec(inner_type) => to_list(inner_type, values),
        DynType::Struct(inner_type) => to_struct(inner_type, values),
        DynType::Enum(inner_type) => to_enum(inner_type, values),
        _ => Err(anyhow!("Unsupported type: {:?}", param_type)),
    }
}

fn to_number<T>(num_bits: usize, values: &[Option<DynValue>]) -> Result<Arc<dyn Array>>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i8> + TryFrom<u8> + TryFrom<i16> + TryFrom<u16> + TryFrom<i32> + TryFrom<u32> + TryFrom<i64> + TryFrom<u64> + TryFrom<i128> + TryFrom<u128>,
{
    let mut builder = builder::PrimitiveBuilder::<T>::with_capacity(values.len());

    for v in values.iter() {
        match v {
            Some(val) => {
                match convert_value::<T>(val, num_bits) {
                    Ok(converted) => builder.append_value(converted),
                    Err(e) => return Err(e),
                }
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

// Helper function to convert a value to T::Native
fn convert_value<T: ArrowPrimitiveType>(val: &DynValue, expected_bits: usize) -> Result<T::Native>
where
    T::Native: TryFrom<i8> + TryFrom<u8> + TryFrom<i16> + TryFrom<u16> + TryFrom<i32> + TryFrom<u32> + TryFrom<i64> + TryFrom<u64> + TryFrom<i128> + TryFrom<u128>,
{
    let (actual_bits, value) = match val {
        DynValue::I8(v) => (8, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i8"))),
        DynValue::U8(v) => (8, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u8"))),
        DynValue::I16(v) => (16, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i16"))),
        DynValue::U16(v) => (16, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u16"))),
        DynValue::I32(v) => (32, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i32"))),
        DynValue::U32(v) => (32, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u32"))),
        DynValue::I64(v) => (64, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i64"))),
        DynValue::U64(v) => (64, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u64"))),
        DynValue::I128(v) => (128, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i128"))),
        DynValue::U128(v) => (128, T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u128"))),
        _ => return Err(anyhow!("Unexpected value type: {:?}", val)),
    };

    if actual_bits != expected_bits {
        return Err(anyhow!(
            "Bit width mismatch: expected {} bits, got {} bits",
            expected_bits,
            actual_bits
        ));
    }

    value.map_err(|_| anyhow!("Failed to convert value to target type"))
}

fn to_bool(values: &[Option<DynValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::BooleanBuilder::new();
    for val in values {
        match val {
            Some(DynValue::Bool(b)) => builder.append_value(*b),
            Some(v) => return Err(anyhow!("found unexpected value. Expected: bool, Found: {:?}", v)),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn to_binary(values: &[Option<DynValue>]) -> Result<Arc<dyn Array>> {
    let mut builder = builder::BinaryBuilder::new();
    for val in values {
        match val {
            Some(DynValue::Pubkey(data)) => builder.append_value(data),
            Some(val) => return Err(anyhow!("Expected binary type, found: {:?}", val)),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn to_list(param_type: &DynType, param_values: Vec<Option<DynValue>>) -> Result<Arc<dyn Array>> {
    let mut lengths = Vec::with_capacity(param_values.len());
    let mut inner_values = Vec::with_capacity(param_values.len() * 2);
    let mut validity = Vec::with_capacity(param_values.len() * 2);

    let mut all_valid = true;

    for val in param_values {
        match val {
            Some(val) => match val {
                DynValue::Vec(inner_vals) => {
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
    let list_array_values = to_arrow(param_type, inner_values)?;
    let field = Field::new(
        "",
        to_arrow_dtype(param_type)?,
        true,
    );
    let list_arr = ListArray::try_new(
        Arc::new(field),
        OffsetBuffer::from_lengths(lengths),
        list_array_values,
        if all_valid {
            None
        } else {
            Some(NullBuffer::from(validity))
        },
    )?;
    Ok(Arc::new(list_arr))
}

fn to_arrow_dtype(param_type: &DynType) -> Result<DataType> {
    match param_type {
        DynType::I8 => Ok(DataType::Int8),
        DynType::I16 => Ok(DataType::Int16),
        DynType::I32 => Ok(DataType::Int32),
        DynType::I64 => Ok(DataType::Int64),
        DynType::I128 => Ok(DataType::Decimal128(128, 0)),
        DynType::U8 => Ok(DataType::UInt8),
        DynType::U16 => Ok(DataType::UInt16),
        DynType::U32 => Ok(DataType::UInt32),
        DynType::U64 => Ok(DataType::UInt64),
        DynType::U128 => Ok(DataType::Decimal128(128, 0)),
        DynType::Bool => Ok(DataType::Boolean),
        DynType::Pubkey => Ok(DataType::Binary),
        DynType::Vec(inner_type) => {
            let inner_type = to_arrow_dtype(inner_type)?;
            Ok(DataType::List(Arc::new(Field::new("", inner_type, true))))
        }
        DynType::Enum(variants) => {
            // For enums, we'll use a dictionary type to store the variant names
            Ok(DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)))
        }
        DynType::Struct(fields) => {
            let arrow_fields = fields.iter()
                .map(|(name, field_type)| {
                    let inner_dt = to_arrow_dtype(field_type)?;
                    Ok(Field::new(name, inner_dt, true))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(Fields::from(arrow_fields)))
        }
        DynType::Defined => Ok(DataType::Null),
    }
}

fn to_enum(
    variants: &Vec<(String, DynType)>,
    param_values: Vec<Option<DynValue>>,
) -> Result<Arc<dyn Array>> {
    let mut values = Vec::with_capacity(param_values.len());

    let make_struct = |variant_name, inner_val| {
        let struct_inner = variants.iter().map(|(name, _)| {
            if name == variant_name {
                (name.clone(), Box::new(inner_val))
            } else {
                (name.clone(), Box::new(DynValue::Defined))
            }
        });

        DynValue::Struct(struct_inner)
    };

    for val in param_values {
        match val {
            None => {
                values.push(None);
            }
            Some(v) => match v {
                DynValue::Enum((variant_name, inner_val)) => {
                    values.push(make_struct(variant_name, inner_val));
                }
                _ => {
                    return Err(anyhow!("type mismatch"));
                }
            }
        }
    }

    to_struct(variants, values)


    let mut struct_type = Vec::<(String, DynType)>::new();
    let mut struct_value = Vec::<(String, DynValue)>::new();


    for field in fields.iter() {
        struct_type.push(field.0.clone(), field.1));
    }

    for (field, val) in fields.iter().zip(param_values) {
        struct_type.push((format!(field.0), ))
    }



    let mut variant_indices = Vec::with_capacity(param_values.len());
    let mut variant_values = Vec::with_capacity(param_values.len());

    for val in param_values {
        match val {
            Some(DynValue::Enum(variant_name, inner_val)) => {
                // Find the index of the variant in the fields list
                let variant_index = fields.iter()
                    .position(|(name, _)| name == &variant_name)
                    .ok_or_else(|| anyhow!("Unknown enum variant: {}", variant_name))?;
                
                variant_indices.push(Some(variant_index as u8));
                variant_values.push(Some(*inner_val));
            }
            None => {
                variant_indices.push(None);
                variant_values.push(None);
            }
            Some(val) => return Err(anyhow!("Expected enum value, found: {:?}", val)),
        }
    }

    // Create a dictionary array for the variant names
    let variant_names: Vec<&str> = fields.iter().map(|(name, _)| name.as_str()).collect();
    let dictionary = Arc::new(arrow::array::StringArray::from(variant_names));
    
    // Create the dictionary array with indices
    let indices = arrow::array::UInt8Array::from(variant_indices);
    let dictionary_array = arrow::array::DictionaryArray::try_new(
        indices,
        dictionary,
    )?;

    Ok(Arc::new(dictionary_array))
}

// fn to_enum(
//     fields: &Vec<(String, DynType)>,
//     param_values: Vec<Option<DynValue>>,
// ) -> Result<Arc<dyn Array>> {
//     println!("param_values: {:?}", param_values);
//     println!("fields: {:?}", fields);
//     let mut variant_names = Vec::with_capacity(param_values.len());
//     let mut variant_indices = Vec::with_capacity(param_values.len());
//     let mut variant_values = Vec::with_capacity(param_values.len());
//     let mut variant_fields = Vec::with_capacity(param_values.len());

//     for val in param_values {
//         match val {
//             Some(DynValue::Enum(variant_name, inner_val)) => {
//                 // Find the index of the variant in the fields list
//                 let variant_index = fields.iter()
//                     .position(|(name, _)| name == &variant_name)
//                     .ok_or_else(|| anyhow!("Unknown enum variant: {}", variant_name))?;
                
//                 variant_names.push(Some(variant_name));
//                 variant_indices.push(Some(variant_index as u8));
//                 variant_values.push(Some(*inner_val));
//                 variant_fields.push(fields[variant_index].clone());
//             }
//             None => {
//                 variant_names.push(None);
//                 variant_indices.push(None);
//                 variant_values.push(None);
//                 variant_fields.push((String::from(""), DynType::Defined));
//             }
//             Some(val) => return Err(anyhow!("Expected enum value, found: {:?}", val)),
//         }
//     };

//     println!("variant_names: {:?}", variant_names);
//     println!("variant_indices: {:?}", variant_indices);
//     println!("variant_values: {:?}", variant_values);
//     println!("variant_fields: {:?}", variant_fields);
//     // Create a dictionary array for the variant names
//     let variant_names: Vec<&str> = fields.iter().map(|(name, _)| name.as_str()).collect();
//     let dictionary = Arc::new(arrow::array::StringArray::from(variant_names));
    
//     // Create the dictionary array with indices
//     let indices = arrow::array::UInt8Array::from(variant_indices);
//     let dictionary_array = arrow::array::DictionaryArray::try_new(
//         indices,
//         dictionary,
//     )?;




//     // Create a struct array for the variant values

//     let mut arrays = Vec::with_capacity(fields.len());

//     for ((_, param_type), arr_vals) in variant_fields.iter().zip(variant_values.into_iter()) {
//         arrays.push(to_arrow(param_type, vec![arr_vals])?);
//     }
//     for arr in arrays.clone() {
//         println!("Array length: {:?}", arr.len());
//         println!("Array data type: {:?}", arr.data_type());
//         println!("Array: {:?}", arr);
//     }

//     let fields = arrays
//         .iter()
//         .zip(variant_fields.iter())
//         .map(|(arr, (name, _))| Field::new(name, arr.data_type().clone(), true))
//         .collect::<Vec<_>>();
//     let schema = Arc::new(Schema::new(fields));

    
//     let batch = RecordBatch::try_new(schema, arrays)?;

//     Ok(Arc::new(StructArray::from(batch)))
// }


fn to_struct(
    fields: &Vec<(String, DynType)>,
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
                for (v, (name, inner)) in inner_values.iter_mut().zip(inner_vals) {
                    v.push(Some(inner.clone()));
                }
            },
            Some(val) => return Err(anyhow!("found unexpected value. Expected: Struct, Found: {:?}", val)),
            None => inner_values.iter_mut().for_each(|v| v.push(None)),
        }
    }

    let mut arrays = Vec::with_capacity(fields.len());

    for ((param_name,param_type), arr_vals) in fields.iter().zip(inner_values.into_iter()) {
        arrays.push(to_arrow(param_type, arr_vals)?);
    }

    let fields = arrays
        .iter()
        .zip(fields.iter())
        .map(|(arr, (name, _))| Field::new(name, arr.data_type().clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema, arrays)?;

    Ok(Arc::new(StructArray::from(batch)))
}

pub fn match_discriminators(
    instr_data: &[u8],
    discriminator: &[u8],
    sec_discriminator: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let discriminator_len = discriminator.len();
    let disc = &instr_data[..discriminator_len];
    let mut ix_data = &instr_data[discriminator_len..];
    if !disc.eq(discriminator) {
        return Err(anyhow::anyhow!("Instruction data discriminator doesn't match signature discriminator"));
    }
    if let Some(sec_discriminator) = sec_discriminator {
        let sec_discriminator_len = sec_discriminator.len();
        let sec_disc = &ix_data[..sec_discriminator_len];
        if !sec_disc.eq(sec_discriminator) {
            return Err(anyhow::anyhow!("Instruction data sec_discriminator doesn't match signature sec_discriminator"));
        }
        ix_data = &ix_data[sec_discriminator_len..];
    }
    Ok(ix_data.to_vec())
}



mod tests {
    use super::*;

    #[test]
    // #[ignore]
    fn read_parquet_files() {
        // let builder = ParquetRecordBatchReaderBuilder::try_new(File::open("../core/reports/instruction.parquet").unwrap()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open("filtered_instructions.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let instructions = reader.next().unwrap().unwrap();

        // Filter instructions by program id
        let jup_program_id = Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").to_bytes();
        let spl_token_program_id = Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").to_bytes();        
        let spl_token_2022_program_id = Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").to_bytes();

        // Get the index of the program_id column
        let program_id_idx = instructions.schema().index_of("program_id").unwrap();

        // Get the program_id column as a BinaryArray
        let program_id_col = instructions.column(program_id_idx);
        let program_id_col = program_id_col.as_any().downcast_ref::<BinaryArray>().unwrap();

        // Create a boolean mask for filtering
        let mut mask = Vec::with_capacity(instructions.num_rows());
        for i in 0..instructions.num_rows() {
            if program_id_col.is_null(i) {
                mask.push(false);
            } else {
                let value = program_id_col.value(i);
                mask.push(
                    // value == spl_token_program_id ||
                    value == jup_program_id
                    // value == spl_token_2022_program_id
                );
            }
        }

        // Convert mask to BooleanArray and filter the RecordBatch
        let mask_array = arrow::array::BooleanArray::from(mask);
        let filtered_instructions = filter_record_batch(&instructions, &mask_array).unwrap();

        let ix_signature = InstructionSignature {
            // // SPL Token Transfer
            // program_id: Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
            // name: "Transfer".to_string(),
            // discriminator: &[3],
            // sec_discriminator: None,
            // params: vec![
            //     ParamInput {
            //         name: "Amount".to_string(),
            //         param_type: DynType::U64,
            //     },
                
            // ],
            // accounts: vec![
            //     "Source".to_string(),
            //     "Destination".to_string(),
            //     "Authority".to_string(),
            // ],

            // // JUP SwapEvent
            // program_id: Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"),
            // name: "SwapEvent".to_string(),
            // discriminator: &[228, 69, 165, 46, 81, 203, 154, 29],
            // sec_discriminator: Some(&[64, 198, 205, 232, 38, 8, 113, 226]),
            // params: vec![
            //     ParamInput {
            //         name: "Amm".to_string(),
            //         param_type: DynType::Pubkey,
            //     },
            //     ParamInput {
            //         name: "InputMint".to_string(),
            //         param_type: DynType::Pubkey,
            //     },
            //     ParamInput {
            //         name: "InputAmount".to_string(),
            //         param_type: DynType::U64,
            //     },
            //     ParamInput {
            //         name: "OutputMint".to_string(),
            //         param_type: DynType::Pubkey,
            //     },
            //     ParamInput {
            //         name: "OutputAmount".to_string(),
            //         param_type: DynType::U64,
            //     },
            // ],
            // accounts: vec![
            // ],

            // JUP Route
            program_id: Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"),
            name: "Route".to_string(),
            discriminator: &[229, 23, 203, 151, 122, 227, 173, 42],
            sec_discriminator: None,
            params: vec![
                ParamInput {
                    name: "RoutePlan".to_string(),
                    param_type: DynType::Vec(Box::new(
                        DynType::Struct(vec![
                            ("RoutePlanStep".to_string(), 
                            DynType::Enum(vec![
                                ("Saber".to_string(), DynType::Defined),
                                ("SaberAddDecimalsDeposit".to_string(), DynType::Defined),
                                ("SaberAddDecimalsWithdraw".to_string(), DynType::Defined),
                                ("TokenSwap".to_string(), DynType::Defined),
                                ("Sencha".to_string(), DynType::Defined),
                                ("Step".to_string(), DynType::Defined),
                                ("Cropper".to_string(), DynType::Defined),
                                ("Raydium".to_string(), DynType::Defined),
                                ("Crema".to_string(), DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool)])),
                                ("Lifinity".to_string(), DynType::Defined),
                                ("Mercurial".to_string(), DynType::Defined),
                                ("Cykura".to_string(), DynType::Defined),
                                ("Serum".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
                                ("MarinadeDeposit".to_string(), DynType::Defined),
                                ("MarinadeUnstake".to_string(), DynType::Defined),
                                ("Aldrin".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
                                ("AldrinV2".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
                                ("Whirlpool".to_string(), DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool)])),
                                ("Invariant".to_string(), DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)])),
                                ("Meteora".to_string(), DynType::Defined),
                                ("GooseFX".to_string(), DynType::Defined),
                                ("DeltaFi".to_string(), DynType::Struct(vec![("stable".to_string(), DynType::Bool)])),
                                ("Balansol".to_string(), DynType::Defined),
                                ("MarcoPolo".to_string(), DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)])),
                                ("Dradex".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
                                ("LifinityV2".to_string(), DynType::Defined),
                                ("RaydiumClmm".to_string(), DynType::Defined),
                                ("Openbook".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
                                ("Phoenix".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
                                ("Symmetry".to_string(), DynType::Struct(vec![("from_token_id".to_string(), DynType::U64), ("to_token_id".to_string(), DynType::U64)])),
                                ("TokenSwapV2".to_string(), DynType::Defined),
                                ("HeliumTreasuryManagementRedeemV0".to_string(), DynType::Defined),
                                ("StakeDexStakeWrappedSol".to_string(), DynType::Defined),
                                ("StakeDexSwapViaStake".to_string(), DynType::Struct(vec![("bridge_stake_seed".to_string(), DynType::U32)])),
                                ("GooseFXV2".to_string(), DynType::Defined),
                                ("Perps".to_string(), DynType::Defined),
                                ("PerpsAddLiquidity".to_string(), DynType::Defined),
                                ("PerpsRemoveLiquidity".to_string(), DynType::Defined),
                                ("MeteoraDlmm".to_string(), DynType::Defined),
                                ("OpenBookV2".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
                                ("RaydiumClmmV2".to_string(), DynType::Defined),
                                ("StakeDexPrefundWithdrawStakeAndDepositStake".to_string(), DynType::Struct(vec![("bridge_stake_seed".to_string(), DynType::U32)])),
                                ("Clone".to_string(), DynType::Struct(vec![("pool_index".to_string(), DynType::U8), ("quantity_is_input".to_string(), DynType::Bool), ("quantity_is_collateral".to_string(), DynType::Bool)])),
                                ("SanctumS".to_string(), DynType::Struct(vec![("src_lst_value_calc_accs".to_string(), DynType::U8), ("dst_lst_value_calc_accs".to_string(), DynType::U8), ("src_lst_index".to_string(), DynType::U32), ("dst_lst_index".to_string(), DynType::U32)])),
                                ("SanctumSAddLiquidity".to_string(), DynType::Struct(vec![("lst_value_calc_accs".to_string(), DynType::U8), ("lst_index".to_string(), DynType::U32)])),
                                ("SanctumSRemoveLiquidity".to_string(), DynType::Struct(vec![("lst_value_calc_accs".to_string(), DynType::U8), ("lst_index".to_string(), DynType::U32)])),
                                ("RaydiumCP".to_string(), DynType::Defined),
                                ("WhirlpoolSwapV2".to_string(), DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool), ("remaining_accounts_info".to_string(), DynType::Defined)])),
                                ("OneIntro".to_string(), DynType::Defined),
                                ("PumpdotfunWrappedBuy".to_string(), DynType::Defined),
                                ("PumpdotfunWrappedSell".to_string(), DynType::Defined),
                                ("PerpsV2".to_string(), DynType::Defined),
                                ("PerpsV2AddLiquidity".to_string(), DynType::Defined),
                                ("PerpsV2RemoveLiquidity".to_string(), DynType::Defined),
                                ("MoonshotWrappedBuy".to_string(), DynType::Defined),
                                ("MoonshotWrappedSell".to_string(), DynType::Defined),
                                ("StabbleStableSwap".to_string(), DynType::Defined),
                                ("StabbleWeightedSwap".to_string(), DynType::Defined),
                                ("Obric".to_string(), DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)])),
                                ("FoxBuyFromEstimatedCost".to_string(), DynType::Defined),
                                ("FoxClaimPartial".to_string(), DynType::Struct(vec![("is_y".to_string(), DynType::Bool)])),
                                ("SolFi".to_string(), DynType::Struct(vec![("is_quote_to_base".to_string(), DynType::Bool)])),
                                ("SolayerDelegateNoInit".to_string(), DynType::Defined),
                                ("SolayerUndelegateNoInit".to_string(), DynType::Defined),
                                ("TokenMill".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
                                ("DaosFunBuy".to_string(), DynType::Defined),
                                ("DaosFunSell".to_string(), DynType::Defined),
                                ("ZeroFi".to_string(), DynType::Defined),
                                ("StakeDexWithdrawWrappedSol".to_string(), DynType::Defined),
                                ("VirtualsBuy".to_string(), DynType::Defined),
                                ("VirtualsSell".to_string(), DynType::Defined),
                                ("Peren".to_string(), DynType::Struct(vec![("in_index".to_string(), DynType::U8), ("out_index".to_string(), DynType::U8)])),
                                ("PumpdotfunAmmBuy".to_string(), DynType::Defined),
                                ("PumpdotfunAmmSell".to_string(), DynType::Defined),
                                ("Gamma".to_string(), DynType::Defined),
                            ])),
                            ("Percent".to_string(), DynType::U8),
                            ("InputIndex".to_string(), DynType::U8),
                            ("OutputIndex".to_string(), DynType::U8),
                        ])
                    )),
                },
                ParamInput {
                    name: "InAmount".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "QuotedOutAmount".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "SlippageBps".to_string(),
                    param_type: DynType::U16,
                },
                ParamInput {
                    name: "PlatformFeeBps".to_string(),
                    param_type: DynType::U8,
                },          
            ],
            accounts: vec![
                "TokenProgram".to_string(),
                "UserTransferAuthority".to_string(),
                "UserSourceTokenAccount".to_string(),
                "UserDestinationTokenAccount".to_string(),
                "DestinationTokenAccount".to_string(),
                "PlatformFeeAccount".to_string(),
                "EventAuthority".to_string(),
                "Program".to_string(),
            ],





        };

        let result = decode_batch(&filtered_instructions, ix_signature);
    }
}

