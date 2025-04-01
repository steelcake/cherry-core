use anchor_lang::prelude::Pubkey;
use anyhow::{anyhow, Result, Context};
use arrow::array::{Array, BinaryArray, ListArray};
use arrow::{
    array::{builder, ArrowPrimitiveType, NullArray, RecordBatch, StructArray},
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::*,
};
use std::{fs::File, sync::Arc};
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
    let program_id_array = program_id_col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();

    let data_col = batch.column_by_name("data").unwrap();
    let data_array = data_col.as_any().downcast_ref::<BinaryArray>().unwrap();

    let rest_of_accounts_col = batch.column_by_name("rest_of_accounts").unwrap();
    let _rest_of_accounts_array = rest_of_accounts_col
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let _account_arrays: Vec<&BinaryArray> = (0..10)
        .map(|i| {
            let col_name = format!("a{}", i);
            let col = batch.column_by_name(&col_name).unwrap();
            col.as_any().downcast_ref::<BinaryArray>().unwrap()
        })
        .collect();

    let num_params = signature.params.len();
    let mut _decoded_instructions: Vec<Vec<DynValue>> = Vec::new();
    let mut exploded_values: Vec<Vec<Option<DynValue>>> =
        (0..num_params).map(|_| Vec::new()).collect();

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
        let data_result = match_discriminators(
            &instruction_data,
            signature.discriminator,
            signature.sec_discriminator,
        );
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

    let data_arrays: Vec<Arc<dyn Array>> = exploded_values
        .iter()
        .enumerate()
        .map(|(i, v)| to_arrow(&signature.params[i].param_type, v.clone()).unwrap())
        .collect::<Vec<_>>();

    let schema = Arc::new(Schema::new(
        signature
            .params
            .iter()
            .map(|p| Field::new(p.name.clone(), to_arrow_dtype(&p.param_type).unwrap(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, data_arrays).context("Failed to create record batch from data arrays")?;
    // Save the filtered instructions to a new parquet file
    let mut file = File::create("decoded_instructions.parquet").unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    Ok(batch)
}

fn to_arrow(param_type: &DynType, values: Vec<Option<DynValue>>) -> Result<Arc<dyn Array>> {
    match param_type {
        DynType::NoData => Ok(Arc::new(NullArray::new(values.len()))),
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
        DynType::Option(inner_type) => to_option(inner_type, values),
    }
}

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

fn to_number<T>(num_bits: usize, values: &[Option<DynValue>]) -> Result<Arc<dyn Array>>
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
            Some(val) => match convert_value::<T>(val, num_bits) {
                Ok(converted) => builder.append_value(converted),
                Err(e) => return Err(e),
            },
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

// Helper function to convert a value to T::Native
fn convert_value<T: ArrowPrimitiveType>(val: &DynValue, expected_bits: usize) -> Result<T::Native>
where
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
    let (actual_bits, value) = match val {
        DynValue::I8(v) => (
            8,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i8")),
        ),
        DynValue::U8(v) => (
            8,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u8")),
        ),
        DynValue::I16(v) => (
            16,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i16")),
        ),
        DynValue::U16(v) => (
            16,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u16")),
        ),
        DynValue::I32(v) => (
            32,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i32")),
        ),
        DynValue::U32(v) => (
            32,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u32")),
        ),
        DynValue::I64(v) => (
            64,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i64")),
        ),
        DynValue::U64(v) => (
            64,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u64")),
        ),
        DynValue::I128(v) => (
            128,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert i128")),
        ),
        DynValue::U128(v) => (
            128,
            T::Native::try_from(*v).map_err(|_| anyhow!("Failed to convert u128")),
        ),
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
    let mut validity = Vec::with_capacity(param_values.len());

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

    // First get the correct Arrow DataType for the inner type
    let arrow_data_type = to_arrow_dtype(param_type)?;
    
    // Convert inner values to Arrow array
    let list_array_values = to_arrow(param_type, inner_values)
        .context("Failed to convert list elements to arrow array")?;
    
    // Create field with the correct data type
    let field = Field::new("", arrow_data_type, true);
    
    // Create list array using the converted values
    let list_arr = ListArray::try_new(
        Arc::new(field),
        OffsetBuffer::from_lengths(lengths),
        list_array_values,
        if all_valid { None } else { Some(NullBuffer::from(validity)) },
    ).context("Failed to construct ListArray from list array values")?;
    
    Ok(Arc::new(list_arr))
}

fn to_arrow_dtype(param_type: &DynType) -> Result<DataType> {
    match param_type {
        DynType::Option(inner_type) => to_arrow_dtype(inner_type),
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
            let inner_type = to_arrow_dtype(inner_type).context("Failed to convert list inner type to arrow type")?;
            Ok(DataType::List(Arc::new(Field::new("", inner_type, true))))
        }
        DynType::Enum(variants) => {
            let fields = variants.iter().map(|(name, dt)| {
                let struct_fields = vec![
                    Field::new("variant_chosen", DataType::Boolean, true),
                    Field::new("Data", to_arrow_dtype(&DynType::Option(Box::new(dt.clone())))?, true),
                ];
                
                Ok(Field::new(name, DataType::Struct(Fields::from(struct_fields)), true))
            }).collect::<Result<Vec<_>>>().context("Failed to map enum type to Arrow data type")?;

            Ok(DataType::Struct(Fields::from(fields)))
        }
        DynType::Struct(fields) => {
            let arrow_fields = fields
                .iter()
                .map(|(name, field_type)| {
                    let inner_dt = to_arrow_dtype(field_type).context("Failed to convert struct inner field type to arrow type")?;
                    Ok(Field::new(name, inner_dt, true))
                })
                .collect::<Result<Vec<_>>>().context("Failed to convert struct fields to arrow fields")?;
            Ok(DataType::Struct(Fields::from(arrow_fields)))
        }
        DynType::NoData => Ok(DataType::Null),
    }
}

fn to_enum(
    variants: &Vec<(String, DynType)>,
    param_values: Vec<Option<DynValue>>,
) -> Result<Arc<dyn Array>> {
    // Converts the enum variants into a struct type as Vec<(String, DynValue)>, and then call to_struct
    let mut values = Vec::with_capacity(param_values.len());

    // Helper closure that, for each variant in the Enum, create a tuple (name, value) and collect them into a Vec
    let make_struct = |variant_name, inner_val: DynValue| {
        // Create a struct where only the chosen variant has a value, others are None
        let struct_inner = variants.iter().map(|(name, _)| {
            if name == &variant_name {
                println!("variant_name: {}", variant_name);
                (name.clone(), DynValue::Struct(vec![
                    ("variant_chosen".to_string(), DynValue::Bool(true)),
                    ("Data".to_string(), DynValue::Option(Some(Box::new(inner_val.clone()))))
                ]))
            } else {
                // This variant was not chosen - set to None
                (name.clone(), DynValue::Struct(vec![
                    ("variant_chosen".to_string(), DynValue::Bool(false)),
                    ("Data".to_string(), DynValue::Option(None))
                ]))
            }
        })
        .collect::<Vec<_>>();
        DynValue::Struct(struct_inner)
    };

    for val in param_values {
        match val {
            None => {
                values.push(None);
            }
            Some(DynValue::Enum(variant_name, inner_val)) => {
                values.push(Some(make_struct(variant_name, *inner_val)));
            }
            Some(_) => return Err(anyhow!("type mismatch")),
        }
    }

    // Convert variants to struct type
    let struct_variants = variants.iter()
        .map(|(name, param_type)| {
            (name.clone(), DynType::Struct(vec![
                ("variant_chosen".to_string(), DynType::Bool),
                ("Data".to_string(), DynType::Option(Box::new(param_type.clone())))
            ]))
        })
        .collect::<Vec<_>>();

    to_struct(&struct_variants, values)
}

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

    for ((param_name, param_type), arr_vals) in fields.iter().zip(inner_values.into_iter()) {
        arrays.push(to_arrow(param_type, arr_vals).context("Failed to convert struct inner values to arrow")?);
    }

    let fields = arrays
        .iter()
        .zip(fields.iter())
        .map(|(arr, (name, _))| Field::new(name, arr.data_type().clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema, arrays).context("Failed to create record batch from struct arrays")?;
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
        return Err(anyhow::anyhow!(
            "Instruction data discriminator doesn't match signature discriminator"
        ));
    }
    if let Some(sec_discriminator) = sec_discriminator {
        let sec_discriminator_len = sec_discriminator.len();
        let sec_disc = &ix_data[..sec_discriminator_len];
        if !sec_disc.eq(sec_discriminator) {
            return Err(anyhow::anyhow!(
                "Instruction data sec_discriminator doesn't match signature sec_discriminator"
            ));
        }
        ix_data = &ix_data[sec_discriminator_len..];
    }
    Ok(ix_data.to_vec())
}

mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_simple_borsh() {
        #[derive(borsh::BorshSerialize)]
        enum InnerEnum {
            Number(i32),
            Text,
        }

        #[derive(borsh::BorshSerialize)]
        enum OuterEnum {
            First(InnerEnum),
            Second(bool),
            Third,
        }

        let bytes = borsh::to_vec(&OuterEnum::Third).unwrap();

        let enum_type = DynType::Enum(vec![
            ("First".to_owned(), DynType::Enum(vec![
                ("Number".to_owned(), DynType::I32),
                ("Text".to_owned(), DynType::NoData),
            ])),
            ("Second".to_owned(), DynType::Bool),
            ("Third".to_owned(), DynType::NoData),
        ]);

        let decoded_ix_result = deserialize_data(
            &bytes,
            &vec![ParamInput {
                name: "my_enum".to_owned(),
                param_type: enum_type.clone(),
            }],
        ).unwrap();

        let r = to_arrow(&enum_type, vec![Some(decoded_ix_result[0].clone())]).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("example", to_arrow_dtype(&enum_type).unwrap(), true)]));
        let batch = RecordBatch::try_new(schema, vec![r]).context("Failed to create record batch from data arrays").unwrap();
        // Save the filtered instructions to a new parquet file
        let mut file = File::create("decoded_instructions.parquet").unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        panic!("{:?}", batch);
    }

    #[test]
    // #[ignore]
    fn read_parquet_files() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use arrow::compute::filter_record_batch;
        
        // let builder = ParquetRecordBatchReaderBuilder::try_new(File::open("../core/reports/instruction.parquet").unwrap()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            File::open("filtered_instructions2.parquet").unwrap(),
            // File::open("../core/reports/instruction.parquet").unwrap(),
        )
        .unwrap();
        let mut reader = builder.build().unwrap();
        let instructions = reader.next().unwrap().unwrap();

        // Filter instructions by program id
        let jup_program_id =
            Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").to_bytes();
        // let spl_token_program_id =
        //     Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").to_bytes();
        // let spl_token_2022_program_id =
        //     Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").to_bytes();

        // Get the index of the program_id column
        let program_id_idx = instructions.schema().index_of("program_id").unwrap();

        // Get the program_id column as a BinaryArray
        let program_id_col = instructions.column(program_id_idx);
        let program_id_col = program_id_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        // Create a boolean mask for filtering
        let mut mask = Vec::with_capacity(instructions.num_rows());
        for i in 0..instructions.num_rows() {
            if program_id_col.is_null(i) {
                mask.push(false);
            } else {
                let value = program_id_col.value(i);
                mask.push(
                    // value == spl_token_program_id ||
                    value == jup_program_id, // value == spl_token_2022_program_id
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
                    param_type: DynType::Vec(Box::new(DynType::Struct(vec![
                        (
                            "Swap".to_string(),
                            DynType::Enum(vec![
                                ("Saber".to_string(), DynType::NoData),
                                ("SaberAddDecimalsDeposit".to_string(), DynType::NoData),
                                ("SaberAddDecimalsWithdraw".to_string(), DynType::NoData),
                                ("TokenSwap".to_string(), DynType::NoData),
                                ("Sencha".to_string(), DynType::NoData),
                                ("Step".to_string(), DynType::NoData),
                                ("Cropper".to_string(), DynType::NoData),
                                ("Raydium".to_string(), DynType::NoData),
                                (
                                    "Crema".to_string(),
                                    DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool)]),
                                ),
                                ("Lifinity".to_string(), DynType::NoData),
                                ("Mercurial".to_string(), DynType::NoData),
                                ("Cykura".to_string(), DynType::NoData),
                                (
                                    "Serum".to_string(),
                                    DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), DynType::NoData),
                                            ("Ask".to_string(), DynType::NoData),
                                        ]),
                                    )]),
                                ),
                                ("MarinadeDeposit".to_string(), DynType::NoData),
                                ("MarinadeUnstake".to_string(), DynType::NoData),
                                (
                                    "Aldrin".to_string(),
                                    DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), DynType::NoData),
                                            ("Ask".to_string(), DynType::NoData),
                                        ]),
                                    )]),
                                ),
                                (
                                    "AldrinV2".to_string(),
                                    DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), DynType::NoData),
                                            ("Ask".to_string(), DynType::NoData),
                                        ]),
                                    )]),
                                ),
                                (
                                    "Whirlpool".to_string(),
                                    DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool)]),
                                ),
                                (
                                    "Invariant".to_string(),
                                    DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)]),
                                ),
                                ("Meteora".to_string(), DynType::NoData),
                                ("GooseFX".to_string(), DynType::NoData),
                                (
                                    "DeltaFi".to_string(),
                                    DynType::Struct(vec![("stable".to_string(), DynType::Bool)]),
                                ),
                                ("Balansol".to_string(), DynType::NoData),
                                (
                                    "MarcoPolo".to_string(),
                                    DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)]),
                                ),
                                (
                                    "Dradex".to_string(),
                                    DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), DynType::NoData),
                                            ("Ask".to_string(), DynType::NoData),
                                        ]),
                                    )]),
                                ),
                                ("LifinityV2".to_string(), DynType::NoData),
                                ("RaydiumClmm".to_string(), DynType::NoData),
                                (
                                    "Openbook".to_string(),
                                    DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), DynType::NoData),
                                            ("Ask".to_string(), DynType::NoData),
                                        ]),
                                    )]),
                                ),
                                (
                                    "Phoenix".to_string(),
                                    DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), DynType::NoData),
                                            ("Ask".to_string(), DynType::NoData),
                                        ]),
                                    )]),
                                ),
                                (
                                    "Symmetry".to_string(),
                                    DynType::Struct(vec![
                                        ("from_token_id".to_string(), DynType::U64),
                                        ("to_token_id".to_string(), DynType::U64),
                                    ]),
                                ),
                                ("TokenSwapV2".to_string(), DynType::NoData),
                                (
                                    "HeliumTreasuryManagementRedeemV0".to_string(),
                                    DynType::NoData,
                                ),
                                ("StakeDexStakeWrappedSol".to_string(), DynType::NoData),
                                (
                                    "StakeDexSwapViaStake".to_string(),
                                    DynType::Struct(vec![(
                                        "bridge_stake_seed".to_string(),
                                        DynType::U32,
                                    )]),
                                ),
                                ("GooseFXV2".to_string(), DynType::NoData),
                                ("Perps".to_string(), DynType::NoData),
                                ("PerpsAddLiquidity".to_string(), DynType::NoData),
                                ("PerpsRemoveLiquidity".to_string(), DynType::NoData),
                                ("MeteoraDlmm".to_string(), DynType::NoData),
                                (
                                    "OpenBookV2".to_string(),
                                    DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), DynType::NoData),
                                            ("Ask".to_string(), DynType::NoData),
                                        ]),
                                    )]),
                                ),
                                ("RaydiumClmmV2".to_string(), DynType::NoData),
                                (
                                    "StakeDexPrefundWithdrawStakeAndDepositStake".to_string(),
                                    DynType::Struct(vec![(
                                        "bridge_stake_seed".to_string(),
                                        DynType::U32,
                                    )]),
                                ),
                                (
                                    "Clone".to_string(),
                                    DynType::Struct(vec![
                                        ("pool_index".to_string(), DynType::U8),
                                        ("quantity_is_input".to_string(), DynType::Bool),
                                        ("quantity_is_collateral".to_string(), DynType::Bool),
                                    ]),
                                ),
                                (
                                    "SanctumS".to_string(),
                                    DynType::Struct(vec![
                                        ("src_lst_value_calc_accs".to_string(), DynType::U8),
                                        ("dst_lst_value_calc_accs".to_string(), DynType::U8),
                                        ("src_lst_index".to_string(), DynType::U32),
                                        ("dst_lst_index".to_string(), DynType::U32),
                                    ]),
                                ),
                                (
                                    "SanctumSAddLiquidity".to_string(),
                                    DynType::Struct(vec![
                                        ("lst_value_calc_accs".to_string(), DynType::U8),
                                        ("lst_index".to_string(), DynType::U32),
                                    ]),
                                ),
                                (
                                    "SanctumSRemoveLiquidity".to_string(),
                                    DynType::Struct(vec![
                                        ("lst_value_calc_accs".to_string(), DynType::U8),
                                        ("lst_index".to_string(), DynType::U32),
                                    ]),
                                ),
                                ("RaydiumCP".to_string(), DynType::NoData),
                                (
                                    "WhirlpoolSwapV2".to_string(),
                                    DynType::Struct(vec![
                                        ("a_to_b".to_string(), DynType::Bool),
                                        ("remaining_accounts_info".to_string(), DynType::NoData),
                                    ]),
                                ),
                                ("OneIntro".to_string(), DynType::NoData),
                                ("PumpdotfunWrappedBuy".to_string(), DynType::NoData),
                                ("PumpdotfunWrappedSell".to_string(), DynType::NoData),
                                ("PerpsV2".to_string(), DynType::NoData),
                                ("PerpsV2AddLiquidity".to_string(), DynType::NoData),
                                ("PerpsV2RemoveLiquidity".to_string(), DynType::NoData),
                                ("MoonshotWrappedBuy".to_string(), DynType::NoData),
                                ("MoonshotWrappedSell".to_string(), DynType::NoData),
                                ("StabbleStableSwap".to_string(), DynType::NoData),
                                ("StabbleWeightedSwap".to_string(), DynType::NoData),
                                (
                                    "Obric".to_string(),
                                    DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)]),
                                ),
                                ("FoxBuyFromEstimatedCost".to_string(), DynType::NoData),
                                (
                                    "FoxClaimPartial".to_string(),
                                    DynType::Struct(vec![("is_y".to_string(), DynType::Bool)]),
                                ),
                                (
                                    "SolFi".to_string(),
                                    DynType::Struct(vec![(
                                        "is_quote_to_base".to_string(),
                                        DynType::Bool,
                                    )]),
                                ),
                                ("SolayerDelegateNoInit".to_string(), DynType::NoData),
                                ("SolayerUndelegateNoInit".to_string(), DynType::NoData),
                                (
                                    "TokenMill".to_string(),
                                    DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), DynType::NoData),
                                            ("Ask".to_string(), DynType::NoData),
                                        ]),
                                    )]),
                                ),
                                ("DaosFunBuy".to_string(), DynType::NoData),
                                ("DaosFunSell".to_string(), DynType::NoData),
                                ("ZeroFi".to_string(), DynType::NoData),
                                ("StakeDexWithdrawWrappedSol".to_string(), DynType::NoData),
                                ("VirtualsBuy".to_string(), DynType::NoData),
                                ("VirtualsSell".to_string(), DynType::NoData),
                                (
                                    "Peren".to_string(),
                                    DynType::Struct(vec![
                                        ("in_index".to_string(), DynType::U8),
                                        ("out_index".to_string(), DynType::U8),
                                    ]),
                                ),
                                ("PumpdotfunAmmBuy".to_string(), DynType::NoData),
                                ("PumpdotfunAmmSell".to_string(), DynType::NoData),
                                ("Gamma".to_string(), DynType::NoData),
                            ]),
                        ),
                        ("Percent".to_string(), DynType::U8),
                        ("InputIndex".to_string(), DynType::U8),
                        ("OutputIndex".to_string(), DynType::U8),
                    ]))),
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
