use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::{array::RecordBatch, compute::filter_record_batch};
use std::fs::File;
use anchor_lang::prelude::Pubkey;
use arrow::array::{Array, BinaryArray, ListArray, UInt64Array};
use anyhow::{anyhow, Result};

pub struct DecodedInstructionData {
    pub param_type: DynType,
    pub value: DynValue,
}
pub struct InstructionSignature {
    pub program_id: Pubkey,
    pub name: String,
    pub discriminator: &'static [u8],
    pub sec_discriminator: Option<&'static [u8]>,
    pub params: Vec<InputParam>,
    pub accounts: Vec<String>,
}
pub struct InputParam {
    pub name: String,
    pub param_type: DynType,
}

#[derive(Debug, Clone)]
pub enum DynType {
    Bool,
    Int,
    Uint,
    FixedBytes(usize),
    Pubkey,
    Bytes(usize),
    String(usize),
    FixedArray(usize),
}

#[derive(Debug, Clone)]
pub enum DynValue {
    Bool(bool),
    Uint(u64),
    Int(i64),
    FixedBytes(usize, Vec<u8>),
    Pubkey(Pubkey),
    Bytes(usize, Vec<u8>),
    String(String, usize),
    FixedArray(usize, Vec<DynValue>),
}

#[derive(Debug)]
pub struct DecodedIx {
    param_types: Vec<DynType>,
    fields: Vec<DynValue>
}

impl DecodedIx {
    fn deserialize(&self, data: &mut [u8]) -> Result<Self> {
        let mut ix_values = Vec::new();
        let mut remaining_data = data;
        
        for param_type in &self.param_types {           
            // Deserialize value based on type
            let (value, new_data) = deserialize_ix_value(param_type, remaining_data)?;
            ix_values.push(value);
            remaining_data = new_data;
        }
        if remaining_data.len() > 0 {
            return Err(anyhow::anyhow!("Remaining data after deserialization"));
        }
        
        Ok(DecodedIx { 
            param_types: self.param_types.clone(), 
            fields: ix_values 
        })
    }
}

fn deserialize_ix_value<'a>(param_type: &DynType, data: &'a mut [u8]) -> Result<(DynValue, &'a mut [u8])> {
    match param_type {
        DynType::Bool => {
            if data.is_empty() {
                return Err(anyhow::anyhow!("Not enough data for bool"));
            }
            let value = data[0] != 0;
            Ok((DynValue::Bool(value), &mut data[1..]))
        }
        DynType::Uint => {
            if data.len() < 8 {
                return Err(anyhow::anyhow!("Not enough data for uint"));
            }
            let value = u64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((DynValue::Uint(value), &mut data[8..]))
        }
        DynType::Int => {
            if data.len() < 8 {
                return Err(anyhow::anyhow!("Not enough data for int"));
            }
            let value = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((DynValue::Int(value), &mut data[8..]))
        }
        DynType::FixedBytes(size) => {
            if data.len() < *size {
                return Err(anyhow::anyhow!("Not enough data for fixed bytes"));
            }
            let value = data[..*size].to_vec();
            Ok((DynValue::FixedBytes(*size, value), &mut data[*size..]))
        }
        DynType::Pubkey => {
            if data.len() < 32 {
                return Err(anyhow::anyhow!("Not enough data for pubkey"));
            }
            let value = Pubkey::new_from_array(data[..32].try_into().unwrap());
            Ok((DynValue::Pubkey(value), &mut data[32..]))
        }
        DynType::Bytes(size) => {
            if data.len() < *size {
                return Err(anyhow::anyhow!("Not enough data for bytes"));
            }
            let value = data[..*size].to_vec();
            Ok((DynValue::Bytes(*size, value), &mut data[*size..]))
        }
        DynType::String(size) => {
            if data.len() < *size {
                return Err(anyhow::anyhow!("Not enough data for string"));
            }
            let value = String::from_utf8(data[..*size].to_vec())?;
            Ok((DynValue::String(value, *size), &mut data[*size..]))
        }
        DynType::FixedArray(size) => {
            let mut values = Vec::new();
            let mut remaining_data = data;
            
            for _ in 0..*size {
                let (value, new_data) = deserialize_ix_value(param_type, remaining_data)?;
                values.push(value);
                remaining_data = new_data;
            }
            
            Ok((DynValue::FixedArray(*size, values), remaining_data))
        }
    }
}

pub fn decode_batch(batch: &RecordBatch, signature: InstructionSignature) -> Result<Vec<Vec<Option<DynValue>>>> {
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
    let mut decoded_instructions:Vec<DecodedIx>  = Vec::new();
    let mut exploded_values: Vec<Vec<Option<DynValue>>> = (0..num_params)
        .map(|_| Vec::new())
        .collect();
    
    for row_idx in 0..batch.num_rows() {
        let instr_program_id: [u8; 32] = program_id_array.value(row_idx).try_into().unwrap();
        let instr_program_id = Pubkey::new_from_array(instr_program_id);

        if !instr_program_id.to_string().as_str().eq(signature.program_id.to_string().as_str()) {
            println!("Program ID doesn't match: {:?}", instr_program_id);
            exploded_values.iter_mut().for_each(|v| v.push(None));
            continue;
        }

        if data_array.is_null(row_idx) {
            println!("Instruction data is null");
            exploded_values.iter_mut().for_each(|v| v.push(None));
            continue;
        }

        let instruction_data = data_array.value(row_idx);
        println!("Instruction data: {:?}", instruction_data);
        let data_result = match_discriminators(&instruction_data, signature.discriminator, signature.sec_discriminator);
        let mut data = match data_result {
            Ok(data) => data,
            Err(e) => {
                println!("Error matching discriminators: {:?}", e);
                exploded_values.iter_mut().for_each(|v| v.push(None));
                continue;
            }
        };

        let decoded_ix_result = DecodedIx {
            param_types: signature.params.iter().map(|p| p.param_type.clone()).collect(),
            fields: Vec::new(),
        }.deserialize(&mut data);

        let decoded_ix = match decoded_ix_result {
            Ok(ix) => ix,
            Err(e) => {
                println!("Error deserializing instruction: {:?}", e);
                exploded_values.iter_mut().for_each(|v| v.push(None));
                continue;
            }
        };
        
        println!("Decoded instruction: {:?}", decoded_ix);
        exploded_values.iter_mut().for_each(|v| v.push(Some(decoded_ix.fields[0].clone())));
    }

    println!("Unpacked: {:?}", exploded_values);

    Ok(exploded_values)
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
    #[ignore]
    fn read_parquet_files() {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open("../core/reports/instruction.parquet").unwrap()).unwrap();
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
                mask.push(value == spl_token_program_id); //value == jup_program_id || value == spl_token_2022_program_id || 
            }
        }

        // Convert mask to BooleanArray and filter the RecordBatch
        let mask_array = arrow::array::BooleanArray::from(mask);
        let filtered_instructions = filter_record_batch(&instructions, &mask_array).unwrap();

        // Save the filtered instructions to a new parquet file
        let mut file = File::create("filtered_instructions.parquet").unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut file, filtered_instructions.schema(), None).unwrap();
        writer.write(&filtered_instructions).unwrap();
        writer.close().unwrap();

        // let result = decode_batch(&filtered_instructions);

    }

    #[test]
    // #[ignore]
    fn decode_instruction_test() {
        // read the filtered_instructions.parquet file
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open("filtered_instructions.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let instructions = reader.next().unwrap().unwrap();

        let ix_signature = InstructionSignature {
            program_id: Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
            name: "Transfer".to_string(),
            discriminator: &[3],
            sec_discriminator: None,
            params: vec![
                InputParam {
                    name: "Amount".to_string(),
                    param_type: DynType::Uint,
                },
                
            ],
            accounts: vec![
                "Source".to_string(),
                "Destination".to_string(),
                "Authority".to_string(),
            ],
        };

        let result = decode_batch(&instructions, ix_signature);
    }
}