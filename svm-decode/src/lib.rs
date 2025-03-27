use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::{array::RecordBatch, compute::filter_record_batch};
use std::fs::File;
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
    // #[ignore]
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

            // JUP SwapEvent
            program_id: Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"),
            name: "SwapEvent".to_string(),
            discriminator: &[228, 69, 165, 46, 81, 203, 154, 29],
            sec_discriminator: Some(&[64, 198, 205, 232, 38, 8, 113, 226]),
            params: vec![
                ParamInput {
                    name: "Amm".to_string(),
                    param_type: DynType::Pubkey,
                },
                ParamInput {
                    name: "InputMint".to_string(),
                    param_type: DynType::Pubkey,
                },
                ParamInput {
                    name: "InputAmount".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "OutputMint".to_string(),
                    param_type: DynType::Pubkey,
                },
                ParamInput {
                    name: "OutputAmount".to_string(),
                    param_type: DynType::U64,
                },
            ],
            accounts: vec![
            ],

            // // JUP Route
            // program_id: Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"),
            // name: "Route".to_string(),
            // discriminator: &[229, 23, 203, 151, 122, 227, 173, 42],
            // sec_discriminator: None,
            // params: vec![
            //     ParamInput {
            //         name: "RoutePlan".to_string(),
            //         param_type: DynType::Vec(Box::new(
            //             DynType::Struct(vec![
            //                 ("RoutePlanStep".to_string(), 
            //                 DynType::Enum(vec![
            //                     ("Saber".to_string(), DynType::Defined),
            //                     ("SaberAddDecimalsDeposit".to_string(), DynType::Defined),
            //                     ("SaberAddDecimalsWithdraw".to_string(), DynType::Defined),
            //                     ("TokenSwap".to_string(), DynType::Defined),
            //                     ("Sencha".to_string(), DynType::Defined),
            //                     ("Step".to_string(), DynType::Defined),
            //                     ("Cropper".to_string(), DynType::Defined),
            //                     ("Raydium".to_string(), DynType::Defined),
            //                     ("Crema".to_string(), DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool)])),
            //                     ("Lifinity".to_string(), DynType::Defined),
            //                     ("Mercurial".to_string(), DynType::Defined),
            //                     ("Cykura".to_string(), DynType::Defined),
            //                     ("Serum".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
            //                     ("MarinadeDeposit".to_string(), DynType::Defined),
            //                     ("MarinadeUnstake".to_string(), DynType::Defined),
            //                     ("Aldrin".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
            //                     ("AldrinV2".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
            //                     ("Whirlpool".to_string(), DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool)])),
            //                     ("Invariant".to_string(), DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)])),
            //                     ("Meteora".to_string(), DynType::Defined),
            //                     ("GooseFX".to_string(), DynType::Defined),
            //                     ("DeltaFi".to_string(), DynType::Struct(vec![("stable".to_string(), DynType::Bool)])),
            //                     ("Balansol".to_string(), DynType::Defined),
            //                     ("MarcoPolo".to_string(), DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)])),
            //                     ("Dradex".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
            //                     ("LifinityV2".to_string(), DynType::Defined),
            //                     ("RaydiumClmm".to_string(), DynType::Defined),
            //                     ("Openbook".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
            //                     ("Phoenix".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
            //                     ("Symmetry".to_string(), DynType::Struct(vec![("from_token_id".to_string(), DynType::U64), ("to_token_id".to_string(), DynType::U64)])),
            //                     ("TokenSwapV2".to_string(), DynType::Defined),
            //                     ("HeliumTreasuryManagementRedeemV0".to_string(), DynType::Defined),
            //                     ("StakeDexStakeWrappedSol".to_string(), DynType::Defined),
            //                     ("StakeDexSwapViaStake".to_string(), DynType::Struct(vec![("bridge_stake_seed".to_string(), DynType::U32)])),
            //                     ("GooseFXV2".to_string(), DynType::Defined),
            //                     ("Perps".to_string(), DynType::Defined),
            //                     ("PerpsAddLiquidity".to_string(), DynType::Defined),
            //                     ("PerpsRemoveLiquidity".to_string(), DynType::Defined),
            //                     ("MeteoraDlmm".to_string(), DynType::Defined),
            //                     ("OpenBookV2".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
            //                     ("RaydiumClmmV2".to_string(), DynType::Defined),
            //                     ("StakeDexPrefundWithdrawStakeAndDepositStake".to_string(), DynType::Struct(vec![("bridge_stake_seed".to_string(), DynType::U32)])),
            //                     ("Clone".to_string(), DynType::Struct(vec![("pool_index".to_string(), DynType::U8), ("quantity_is_input".to_string(), DynType::Bool), ("quantity_is_collateral".to_string(), DynType::Bool)])),
            //                     ("SanctumS".to_string(), DynType::Struct(vec![("src_lst_value_calc_accs".to_string(), DynType::U8), ("dst_lst_value_calc_accs".to_string(), DynType::U8), ("src_lst_index".to_string(), DynType::U32), ("dst_lst_index".to_string(), DynType::U32)])),
            //                     ("SanctumSAddLiquidity".to_string(), DynType::Struct(vec![("lst_value_calc_accs".to_string(), DynType::U8), ("lst_index".to_string(), DynType::U32)])),
            //                     ("SanctumSRemoveLiquidity".to_string(), DynType::Struct(vec![("lst_value_calc_accs".to_string(), DynType::U8), ("lst_index".to_string(), DynType::U32)])),
            //                     ("RaydiumCP".to_string(), DynType::Defined),
            //                     ("WhirlpoolSwapV2".to_string(), DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool), ("remaining_accounts_info".to_string(), DynType::Defined)])),
            //                     ("OneIntro".to_string(), DynType::Defined),
            //                     ("PumpdotfunWrappedBuy".to_string(), DynType::Defined),
            //                     ("PumpdotfunWrappedSell".to_string(), DynType::Defined),
            //                     ("PerpsV2".to_string(), DynType::Defined),
            //                     ("PerpsV2AddLiquidity".to_string(), DynType::Defined),
            //                     ("PerpsV2RemoveLiquidity".to_string(), DynType::Defined),
            //                     ("MoonshotWrappedBuy".to_string(), DynType::Defined),
            //                     ("MoonshotWrappedSell".to_string(), DynType::Defined),
            //                     ("StabbleStableSwap".to_string(), DynType::Defined),
            //                     ("StabbleWeightedSwap".to_string(), DynType::Defined),
            //                     ("Obric".to_string(), DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)])),
            //                     ("FoxBuyFromEstimatedCost".to_string(), DynType::Defined),
            //                     ("FoxClaimPartial".to_string(), DynType::Struct(vec![("is_y".to_string(), DynType::Bool)])),
            //                     ("SolFi".to_string(), DynType::Struct(vec![("is_quote_to_base".to_string(), DynType::Bool)])),
            //                     ("SolayerDelegateNoInit".to_string(), DynType::Defined),
            //                     ("SolayerUndelegateNoInit".to_string(), DynType::Defined),
            //                     ("TokenMill".to_string(), DynType::Struct(vec![("side".to_string(), DynType::Enum(vec![("Bid".to_string(), DynType::Defined), ("Ask".to_string(), DynType::Defined)]))])),
            //                     ("DaosFunBuy".to_string(), DynType::Defined),
            //                     ("DaosFunSell".to_string(), DynType::Defined),
            //                     ("ZeroFi".to_string(), DynType::Defined),
            //                     ("StakeDexWithdrawWrappedSol".to_string(), DynType::Defined),
            //                     ("VirtualsBuy".to_string(), DynType::Defined),
            //                     ("VirtualsSell".to_string(), DynType::Defined),
            //                     ("Peren".to_string(), DynType::Struct(vec![("in_index".to_string(), DynType::U8), ("out_index".to_string(), DynType::U8)])),
            //                     ("PumpdotfunAmmBuy".to_string(), DynType::Defined),
            //                     ("PumpdotfunAmmSell".to_string(), DynType::Defined),
            //                     ("Gamma".to_string(), DynType::Defined),
            //                 ])),
            //                 ("Percent".to_string(), DynType::U8),
            //                 ("InputIndex".to_string(), DynType::U8),
            //                 ("OutputIndex".to_string(), DynType::U8),
            //             ])
            //         )),
            //     },
            //     ParamInput {
            //         name: "InAmount".to_string(),
            //         param_type: DynType::U64,
            //     },
            //     ParamInput {
            //         name: "QuotedOutAmount".to_string(),
            //         param_type: DynType::U64,
            //     },
            //     ParamInput {
            //         name: "SlippageBps".to_string(),
            //         param_type: DynType::U16,
            //     },
            //     ParamInput {
            //         name: "PlatformFeeBps".to_string(),
            //         param_type: DynType::U8,
            //     },          
            // ],
            // accounts: vec![
            //     "TokenProgram".to_string(),
            //     "UserTransferAuthority".to_string(),
            //     "UserSourceTokenAccount".to_string(),
            //     "UserDestinationTokenAccount".to_string(),
            //     "DestinationTokenAccount".to_string(),
            //     "PlatformFeeAccount".to_string(),
            //     "EventAuthority".to_string(),
            //     "Program".to_string(),
            // ],


        };

        let result = decode_batch(&instructions, ix_signature);
    }
}