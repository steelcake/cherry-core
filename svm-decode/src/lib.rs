use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::compute::filter_record_batch;
use std::fs::File;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, BinaryArray, ListArray, UInt64Array};
use anchor_lang::prelude::Pubkey;
use anyhow::{anyhow, Context, Result};
use hex;
mod program;
use program::*;

pub enum InstructionDecodeType {
    BaseHex,
    Base64,
    Base58,
}

#[derive(Debug)]
pub struct RawInstruction {
    pub program_id: String,
    pub accounts: Vec<String>,
    pub data: String,
}

pub fn decode_instructions(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    let program_id = Pubkey::try_from(PROGRAM_ID)?;
    
    let program_id_col = batch.column_by_name("program_id").unwrap();
    let program_id_array = program_id_col.as_any().downcast_ref::<BinaryArray>().unwrap();
    
    let data_col = batch.column_by_name("data").unwrap();
    let data_array = data_col.as_any().downcast_ref::<BinaryArray>().unwrap();
    
    let block_slot_col = batch.column_by_name("block_slot").unwrap();
    let block_slot_array = block_slot_col.as_any().downcast_ref::<UInt64Array>().unwrap();

    let rest_of_accounts_col = batch.column_by_name("rest_of_accounts").unwrap();
    let rest_of_accounts_array = rest_of_accounts_col.as_any().downcast_ref::<ListArray>().unwrap();
    
    // Get account arrays for potential use
    let mut account_arrays: Vec<&BinaryArray> = (0..10).map(|i| {
        let col_name = format!("a{}", i);
        let col = batch.column_by_name(&col_name).unwrap();
        col.as_any().downcast_ref::<BinaryArray>().unwrap()
    }).collect();
    
    // Process each row in the batch
    let mut instructions = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let slot = block_slot_array.value(row_idx);
        
        // Check if this instruction matches our program
        if program_id_array.is_null(row_idx) {
            continue;
        }
        
        let instr_program_id: [u8; 32] = program_id_array.value(row_idx).try_into().unwrap();
        let instr_program_id = Pubkey::new_from_array(instr_program_id);
        
        // Skip if not matching our target program
        if instr_program_id != program_id {
            continue;
        }
        
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
            program_id: instr_program_id.to_string(),
            accounts: accounts.iter().map(|account| account.to_string()).collect(),
            data: hex::encode(instruction_data),
        };
        instructions.push(instruction);
    }
    
    println!("instructions: {:#?}", instructions);

    let program_instructions = parse_program_instruction(instructions);
    
    Ok(())
}



pub fn parse_program_instruction(
    // self_program_str: &str,
    instructions: Vec<RawInstruction>,
) -> Result<Vec<ProgramInstructions>> {
    let mut chain_instructions = Vec::new();

    for (i, ix) in instructions.iter().enumerate() {
            // if (ix.program_id) == self_program_str {
                let out_put = format!("instruction #{}", i + 1);
                println!("{}", out_put);
                let accounts: Vec<String> = ix.accounts.iter().cloned().collect();
                match handle_program_instruction(
                    &ix.data,
                    InstructionDecodeType::BaseHex,
                    accounts,
                ) {
                    Ok(chain_instruction) => {
                        chain_instructions.push(chain_instruction);
                    }
                    Err(e) => {
                        eprintln!("Error decoding instruction: {}", e);
                        continue;
                    }
                }
            // }
    }

    Ok(chain_instructions)
}

pub fn handle_program_instruction(
    instr_data: &str,
    decode_type: InstructionDecodeType,
    accounts: Vec<String>,
) -> Result<ProgramInstructions> {
    let data = match decode_type {
        InstructionDecodeType::BaseHex => hex::decode(instr_data).unwrap(),
        InstructionDecodeType::Base64 => match anchor_lang::__private::base64::decode(instr_data) {
            Ok(decoded) => decoded,
            Err(_) => return Err(anyhow::anyhow!("Could not base64 decode instruction".to_string()))
        },
        InstructionDecodeType::Base58 => match bs58::decode(instr_data).into_vec() {
            Ok(decoded) => decoded,
            Err(_) => return Err(anyhow::anyhow!("Could not base58 decode instruction".to_string()))
        },
    };

    let mut ix_data: &[u8] = &data[..];
    let disc: [u8; 8] = {
        let mut disc = [0; 8];
        disc.copy_from_slice(&data[..8]);
        ix_data = &ix_data[8..];
        disc
    };

    println!("disc: {:?}", disc);

    let result = match disc {
        // Instructions
        CLAIM_IX_DISCM => {
            match decode_instruction::<Claim>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::Claim {
                    id: ix.id,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode Claim instruction: {}",
                    e
                )),
            }
        }
        CLAIM_TOKEN_IX_DISCM => {
            match decode_instruction::<ClaimToken>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::ClaimToken {
                    id: ix.id,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode ClaimToken instruction: {}",
                    e
                )),
            }
        }
        CLOSE_TOKEN_IX_DISCM => {
            match decode_instruction::<CloseToken>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::CloseToken {
                    id: ix.id,
                    burn_all: ix.burn_all,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode CloseToken instruction: {}",
                    e
                )),
            }
        }
        CREATE_OPEN_ORDERS_IX_DISCM => {
            match decode_instruction::<CreateOpenOrders>(&mut ix_data) {
                Ok(_) => Ok(ProgramInstructions::CreateOpenOrders),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode CreateOpenOrders instruction: {}",
                    e
                )),
            }
        }
        CREATE_PROGRAM_OPEN_ORDERS_IX_DISCM => {
            match decode_instruction::<CreateProgramOpenOrders>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::CreateProgramOpenOrders {
                    id: ix.id,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode CreateProgramOpenOrders instruction: {}",
                    e
                )),
            }
        }
        CREATE_TOKEN_LEDGER_IX_DISCM => {
            match decode_instruction::<CreateTokenLedger>(&mut ix_data) {
                Ok(_) => Ok(ProgramInstructions::CreateTokenLedger),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode CreateTokenLedger instruction: {}",
                    e
                )),
            }
        }
        CREATE_TOKEN_ACCOUNT_IX_DISCM => {
            match decode_instruction::<CreateTokenAccount>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::CreateTokenAccount {
                    bump: ix.bump,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode CreateTokenAccount instruction: {}",
                    e
                )),
            }
        }
        EXACT_OUT_ROUTE_IX_DISCM => {
            match decode_instruction::<ExactOutRoute>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::ExactOutRoute {
                    route_plan: ix.route_plan,
                    out_amount: ix.out_amount,
                    quoted_in_amount: ix.quoted_in_amount,
                    slippage_bps: ix.slippage_bps,
                    platform_fee_bps: ix.platform_fee_bps,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode ExactOutRoute instruction: {}",
                    e
                )),
            }
        }
        ROUTE_IX_DISCM => {
            match decode_instruction::<Route>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::Route {
                    route_plan: ix.route_plan,
                    in_amount: ix.in_amount,
                    quoted_out_amount: ix.quoted_out_amount,
                    slippage_bps: ix.slippage_bps,
                    platform_fee_bps: ix.platform_fee_bps,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode Route instruction: {}",
                    e
                )),
            }
        }
        ROUTE_WITH_TOKEN_LEDGER_IX_DISCM => {
            match decode_instruction::<RouteWithTokenLedger>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::RouteWithTokenLedger {
                    route_plan: ix.route_plan,
                    quoted_out_amount: ix.quoted_out_amount,
                    slippage_bps: ix.slippage_bps,
                    platform_fee_bps: ix.platform_fee_bps,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode RouteWithTokenLedger instruction: {}",
                    e
                )),
            }
        }
        SET_TOKEN_LEDGER_IX_DISCM => {
            match decode_instruction::<SetTokenLedger>(&mut ix_data) {
                Ok(_) => Ok(ProgramInstructions::SetTokenLedger),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode SetTokenLedger instruction: {}",
                    e
                )),
            }
        }
        SHARED_ACCOUNTS_EXACT_OUT_ROUTE_IX_DISCM => {
            match decode_instruction::<SharedAccountsExactOutRoute>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::SharedAccountsExactOutRoute {
                    id: ix.id,
                    route_plan: ix.route_plan,
                    out_amount: ix.out_amount,
                    quoted_in_amount: ix.quoted_in_amount,
                    slippage_bps: ix.slippage_bps,
                    platform_fee_bps: ix.platform_fee_bps,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode SharedAccountsExactOutRoute instruction: {}",
                    e
                )),
            }
        }
        SHARED_ACCOUNTS_ROUTE_IX_DISCM => {
            match decode_instruction::<SharedAccountsRoute>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::SharedAccountsRoute{
                    id: ix.id,
                    route_plan: ix.route_plan,
                    in_amount: ix.in_amount,
                    quoted_out_amount: ix.quoted_out_amount,
                    slippage_bps: ix.slippage_bps,
                    platform_fee_bps: ix.platform_fee_bps,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode SharedAccountsRoute instruction: {}",
                    e
                )),
            }
        }
        SHARED_ACCOUNTS_ROUTE_WITH_TOKEN_LEDGER_IX_DISCM => {
            match decode_instruction::<SharedAccountsRouteWithTokenLedger>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::SharedAccountsRouteWithTokenLedger{
                    id: ix.id,
                    route_plan: ix.route_plan,
                    quoted_out_amount: ix.quoted_out_amount,
                    slippage_bps: ix.slippage_bps,
                    platform_fee_bps: ix.platform_fee_bps,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode SharedAccountsRouteWithTokenLedger instruction: {}",
                    e
                )),
            }
        }
        // Account  
        TOKEN_LEDGER_ACCOUNT_DISCM => {
            match decode_instruction::<TokenLedger>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::TokenLedger{
                    token_account: ix.token_account,
                    amount: ix.amount,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode TokenLedgerAccount instruction: {}",
                    e
                )),
            }
        }
        // Events
        FEE_EVENT_DISCM => {
            match decode_instruction::<FeeEvent>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::FeeEvent{
                    account: ix.account,
                    mint: ix.mint,
                    amount: ix.amount,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode FeeEvent instruction: {}",
                    e
                )),
            }
        }
        SWAP_EVENT_DISCM => {
            match decode_instruction::<SwapEvent>(&mut ix_data) {
                Ok(ix) => Ok(ProgramInstructions::SwapEvent{
                    amm: ix.amm,
                    input_mint: ix.input_mint,
                    input_amount: ix.input_amount,
                    output_mint: ix.output_mint,
                    output_amount: ix.output_amount,
                }),
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to decode SwapEvent instruction: {}",
                    e
                )),
            }
        }
        _ => Err(anyhow::anyhow!("Unknown instruction discriminator: {:?}", disc)),
    };

    println!("result: {:?}", result);
    result
}

fn decode_instruction<T: anchor_lang::AnchorDeserialize>(
    slice: &mut &[u8],
) -> Result<T, anchor_lang::error::ErrorCode> {
    let instruction: T = anchor_lang::AnchorDeserialize::deserialize(slice)
        .map_err(|_| anchor_lang::error::ErrorCode::InstructionDidNotDeserialize)?;
    Ok(instruction)
}

mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn read_parquet_files() {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open("../core/reports/instruction.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let instructions = reader.next().unwrap().unwrap();
        // Print the schema of the instructions
        // println!("Schema: {:?}", instructions.schema());

        // Filter instructions by program id
        let program_id = Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").to_bytes();
        
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
                mask.push(value == program_id);
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

        let result = decode_instructions(&filtered_instructions);
        println!("result: {:?}", result);

    }

    #[test]
    // #[ignore]
    fn decode_instruction_test() {
        // read the filtered_instructions.parquet file
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open("filtered_instructions2.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let instructions = reader.next().unwrap().unwrap();

        // decode the instruction
        let result = decode_instructions(&instructions);
        println!("result: {:?}", result);
    }
}
