use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::compute::filter_record_batch;
use std::fmt::{self, Display};
use std::fs::File;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, BinaryArray, ListArray, UInt64Array};
use anchor_lang::prelude::Pubkey;
use anyhow::{anyhow, Context, Result};
use hex;
mod jup_program;
use jup_program::*;
use spl_token_2022::instruction::TokenInstruction;
use arrow_schema_derive::ToArrowSchema;
use arrow::datatypes::{DataType, Field, Schema};


#[derive(Debug)]
pub struct RawInstruction<'a> {
    pub program_id: Pubkey,
    pub accounts: Vec<Pubkey>,
    pub data: &'a [u8],
}

#[derive(Debug)]
pub struct DecodedInstruction<'a> {
    pub program_id: Pubkey,
    pub accounts: Vec<Pubkey>,
    pub data: ProgramInstructions<'a>,
}

#[derive(Debug)]
pub struct InstructionSignature<'a> {
    pub program_id: Pubkey,
    pub name: &'a str,
    pub discriminator: &'a [u8],
}

impl Display for RawInstruction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RawInstruction\nProgram ID: {}\nAccounts: {}\nData: {}", self.program_id, self.accounts.iter().map(|account| account.to_string()).collect::<Vec<String>>().join(", "), hex::encode(self.data))
    }
}

#[derive(Debug)]
pub enum ProgramInstructions<'a> {
    TokenInstruction(TokenInstruction<'a>),
    JupInstruction(JupInstruction),
}

pub trait ToArrowSchema {
    fn to_arrow_schema() -> Schema;
}

#[derive(ToArrowSchema)]
pub struct TransferEvent {
    pub source: Pubkey,
    pub destination: Pubkey,
    pub authority: Pubkey,
    pub amount: u64,
}

pub fn decode_instructions(
    ix_signature: InstructionSignature,
    data: &RecordBatch,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    // let program_id_idx = data.schema().index_of("program_id").unwrap();
    // let program_id_col = data.column(program_id_idx);
    // let program_id_col = program_id_col.as_any().downcast_ref::<BinaryArray>().unwrap();
    
    // // Create a boolean mask for filtering
    // let mut mask = Vec::with_capacity(data.num_rows());
    // for i in 0..data.num_rows() {
    //     if program_id_col.is_null(i) {
    //         mask.push(false);
    //     } else {
    //         let value = program_id_col.value(i);
    //         mask.push(value == ix_signature.program_id.to_bytes());
    //     }
    // }
    
    // // Convert mask to BooleanArray and filter the RecordBatch
    // let mask_array = arrow::array::BooleanArray::from(mask);
    // let program_filtered_data = filter_record_batch(&data, &mask_array).unwrap();
    let schema = TransferEvent::to_arrow_schema();
    let mut arrays: Vec<Arc<dyn Array + 'static>> = Vec::with_capacity(schema.fields().len());
    println!("schema: {:?}", schema);
    let decoded_instructions = decode_batch(&data)?;

    let mut transfer_events = Vec::<Option<TransferEvent>>::new();
    for instruction in decoded_instructions.iter() {
        match instruction {
            Some(DecodedInstruction {
                program_id: _,
                accounts: acc,
                data: ProgramInstructions::TokenInstruction(TokenInstruction::Transfer { amount }),
            }) => {
                transfer_events.push(Some(TransferEvent {
                    source: acc[0],
                    destination: acc[1],
                    authority: acc[2],
                    amount: amount.clone(),
                }));
            }
            _ => transfer_events.push(None),
        }
    }

    todo!()
    // RecordBatch::try_new(Arc::new(schema), arrays).context("construct arrow batch")
}

pub fn decode_batch(batch: &RecordBatch) -> Result<Vec<Option<DecodedInstruction>>> {    
    let program_id_col = batch.column_by_name("program_id").unwrap();
    let program_id_array = program_id_col.as_any().downcast_ref::<BinaryArray>().unwrap();
    
    let data_col = batch.column_by_name("data").unwrap();
    let data_array = data_col.as_any().downcast_ref::<BinaryArray>().unwrap();

    let rest_of_accounts_col = batch.column_by_name("rest_of_accounts").unwrap();
    let rest_of_accounts_array = rest_of_accounts_col.as_any().downcast_ref::<ListArray>().unwrap();
    
    // Get account arrays for potential use
    let account_arrays: Vec<&BinaryArray> = (0..10).map(|i| {
        let col_name = format!("a{}", i);
        let col = batch.column_by_name(&col_name).unwrap();
        col.as_any().downcast_ref::<BinaryArray>().unwrap()
    }).collect();
    
    // Process each row in the batch
    let mut instructions = Vec::new();
    for row_idx in 0..batch.num_rows() {
        
        let instr_program_id: [u8; 32] = program_id_array.value(row_idx).try_into().unwrap();
        let instr_program_id = Pubkey::new_from_array(instr_program_id);
                
        // Get instruction data
        if data_array.is_null(row_idx) {
            continue;
        }
        
        let instruction_data = data_array.value(row_idx);
        
        // Get account keys for this instruction
        let mut accounts: Vec<Pubkey> = Vec::new();
        for account_array in &account_arrays {
            if !account_array.is_null(row_idx) {
                let account_data: [u8; 32] = account_array.value(row_idx).try_into().unwrap();
                if !account_data.is_empty() {
                    accounts.push(Pubkey::new_from_array(account_data));
                }
            }
        }
        
        let rest_of_accounts = rest_of_accounts_array.value(row_idx);
        for account_data in rest_of_accounts.as_any().downcast_ref::<BinaryArray>().unwrap().iter() {
            let account_data: [u8; 32] = account_data.unwrap().try_into().unwrap();
            if !account_data.is_empty() {
                accounts.push(Pubkey::new_from_array(account_data));
            }
        }

        let instruction = RawInstruction {
            program_id: instr_program_id,
            accounts: accounts,
            data: instruction_data,
        };
        instructions.push(instruction);
    }

    parse_program_instruction(instructions)
}

pub fn parse_program_instruction(
    instructions: Vec<RawInstruction>,
) -> Result<Vec<Option<DecodedInstruction>>> {
    let mut decoded_instructions = Vec::new();

    for (i, ix) in instructions.iter().enumerate() {
                let output = format!("\ninstruction #{}", i + 1);
                println!("{}", output);
                println!("{}", ix);
                match unpack_instruction(
                    ix.program_id.clone(),
                    &ix.data,
                    ix.accounts.clone(),
                ) {
                    Ok(chain_instruction) => {
                        println!("decoded_instruction: {:?}", chain_instruction);
                        decoded_instructions.push(Some(chain_instruction));
                    }
                    Err(e) => {
                        eprintln!("Error decoding instruction: {}", e);
                        decoded_instructions.push(None);
                    }
                };
    }

    Ok(decoded_instructions)
}

pub fn unpack_instruction(
    program_id: Pubkey,
    instr_data: &[u8],
    accounts: Vec<Pubkey>,
) -> Result<DecodedInstruction> {

    match program_id.to_string().as_str() {
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4" => {
            let jup_ix = JupInstruction::unpack(&mut &instr_data[..])?;
            Ok(DecodedInstruction {
                program_id: program_id,
                accounts: accounts,
                data: ProgramInstructions::JupInstruction(jup_ix),
            })
        }
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => {
            let token_ix = spl_token_2022::instruction::TokenInstruction::unpack(&mut &instr_data[..])?;
            Ok(DecodedInstruction {
                program_id: program_id,
                accounts: accounts,
                data: ProgramInstructions::TokenInstruction(token_ix),
            })
        }
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" => {
            let token_ix = spl_token_2022::instruction::TokenInstruction::unpack(&mut &instr_data[..])?;
            Ok(DecodedInstruction {
                program_id: program_id,
                accounts: accounts,
                data: ProgramInstructions::TokenInstruction(token_ix),
            })
        }
        _ => Err(anyhow::anyhow!("Program id not supported: {:?}", program_id)),
    }
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
                mask.push(value == jup_program_id || value == spl_token_program_id || value == spl_token_2022_program_id);
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

        let result = decode_batch(&filtered_instructions);

    }

    #[test]
    // #[ignore]
    fn decode_instruction_test() {
        // read the filtered_instructions.parquet file
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open("filtered_instructions3.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let instructions = reader.next().unwrap().unwrap();

        let ix_signature = InstructionSignature {
            program_id: Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"),
            name: "SwapEvent",
            discriminator: &[64, 198, 205, 232, 38, 8, 113, 226],
        };

        let result = decode_instructions(ix_signature, &instructions, false);

        // decode the instruction
        // let result = decode_batch(&instructions);
    }
}
