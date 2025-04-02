use anchor_lang::prelude::Pubkey;
use anyhow::{Context, Result};
use arrow::array::{Array, BinaryArray};
use arrow::{array::RecordBatch, datatypes::*};
use std::sync::Arc;
mod deserialize;
use deserialize::{deserialize_data, DynValue, ParamInput};
mod arrow_converter;
use arrow_converter::{to_arrow, to_arrow_dtype};

pub struct InstructionSignature<'a> {
    pub program_id: Pubkey,
    pub name: String,
    pub discriminator: &'a [u8],
    pub params: Vec<ParamInput>,
    pub accounts: Vec<String>,
}

pub fn decode_instruction_data(
    batch: &RecordBatch,
    signature: InstructionSignature,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    let program_id_col = batch.column_by_name("program_id").unwrap();
    let program_id_array = program_id_col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();

    let data_col = batch.column_by_name("data").unwrap();
    let data_array = data_col.as_any().downcast_ref::<BinaryArray>().unwrap();

    let num_params = signature.params.len();

    let mut decoded_params_vec: Vec<Vec<Option<DynValue>>> =
        (0..num_params).map(|_| Vec::new()).collect();

    for row_idx in 0..batch.num_rows() {
        let instr_program_id: [u8; 32] = program_id_array.value(row_idx).try_into().unwrap();
        let instr_program_id = Pubkey::new_from_array(instr_program_id);
        if instr_program_id != signature.program_id {
            if allow_decode_fail {
                log::debug!("Instruction program id doesn't match signature program id");
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            } else {
                return Err(anyhow::anyhow!(
                    "Instruction program id doesn't match signature program id"
                ));
            }
        }

        if data_array.is_null(row_idx) {
            if allow_decode_fail {
                log::debug!("Instruction data is null");
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            } else {
                return Err(anyhow::anyhow!("Instruction data is null"));
            }
        }

        let instruction_data = data_array.value(row_idx);
        let data_result = match_discriminators(
            &instruction_data,
            signature.discriminator,
        );
        let mut data = match data_result {
            Ok(data) => data,
            Err(e) if allow_decode_fail => {
                log::debug!("Error matching discriminators: {:?}", e);
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Error matching discriminators: {:?}", e));
            }
        };

        let decoded_ix_result = deserialize_data(&mut data, &signature.params);
        let decoded_ix = match decoded_ix_result {
            Ok(ix) => ix,
            Err(e) if allow_decode_fail => {
                log::debug!("Error deserializing instruction: {:?}", e);
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Error deserializing instruction: {:?}", e));
            }
        };

        for (i, value) in decoded_ix.into_iter().enumerate() {
            decoded_params_vec[i].push(Some(value));
        }
    }

    let data_arrays: Vec<Arc<dyn Array>> = decoded_params_vec
        .iter()
        .enumerate()
        .map(|(i, v)| to_arrow(&signature.params[i].param_type, v.clone()).unwrap())
        .collect::<Vec<_>>();

    let data_fields = signature
        .params
        .iter()
        .map(|p| Field::new(p.name.clone(), to_arrow_dtype(&p.param_type).unwrap(), true))
        .collect::<Vec<_>>();

    let mut account_arrays: Vec<Arc<dyn Array>> = (0..10)
        .map(|i| {
            let col_name = format!("a{}", i);
            let col = batch.column_by_name(&col_name).unwrap();
            let byte_array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            Arc::new(byte_array.clone()) as Arc<dyn Array>
        })
        .collect();

    let acc_names_len = signature.accounts.len();

    let mut acc_fields = Vec::new();
    if acc_names_len < 10 {
        let _ = account_arrays.split_off(acc_names_len);
        for i in 0..acc_names_len {
            let field = Field::new(signature.accounts[i].clone(), DataType::Binary, true);
            acc_fields.push(field);
        }
    } else {
        for i in 0..10 {
            let field = Field::new(signature.accounts[i].clone(), DataType::Binary, true);
            acc_fields.push(field);
        }
    }

    let decoded_instructions_array = data_arrays
        .into_iter()
        .chain(account_arrays)
        .collect::<Vec<_>>();
    let decoded_instructions_fields = data_fields
        .into_iter()
        .chain(acc_fields.clone())
        .collect::<Vec<_>>();

    let schema = Arc::new(Schema::new(decoded_instructions_fields));
    let batch = RecordBatch::try_new(schema, decoded_instructions_array)
        .context("Failed to create record batch from data arrays")
        .unwrap();

    Ok(batch)
}

pub fn match_discriminators(
    instr_data: &[u8],
    discriminator: &[u8],
) -> Result<Vec<u8>> {
    let discriminator_len = discriminator.len();
    let disc = &instr_data[..discriminator_len];
    let mut ix_data = &instr_data[discriminator_len..];
    if !disc.eq(discriminator) {
        return Err(anyhow::anyhow!(
            "Instruction data discriminator doesn't match signature discriminator"
        ));
    }
    Ok(ix_data.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deserialize::{DynType, ParamInput};
    use std::fs::File;

    #[test]
    #[ignore]
    fn read_parquet_with_real_data() {
        use arrow::compute::filter_record_batch;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let builder = ParquetRecordBatchReaderBuilder::try_new(
            File::open("../core/reports/instruction.parquet").unwrap(),
        )
        .unwrap();
        let mut reader = builder.build().unwrap();
        let instructions = reader.next().unwrap().unwrap();

        // Filter instructions by program id
        let jup_program_id =
            Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").to_bytes();
        // let spl_token_program_id =
        // Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").to_bytes();

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
            discriminator: &[228, 69, 165, 46, 81, 203, 154, 29, 64, 198, 205, 232, 38, 8, 113, 226],
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
            accounts: vec![],
            //     // JUP Route
            //     program_id: Pubkey::from_str_const("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"),
            //     name: "Route".to_string(),
            //     discriminator: &[229, 23, 203, 151, 122, 227, 173, 42],
            //     params: vec![
            //         ParamInput {
            //             name: "RoutePlan".to_string(),
            //             param_type: DynType::Vec(Box::new(DynType::Struct(vec![
            //                 (
            //                     "Swap".to_string(),
            //                     DynType::Enum(vec![
            //                         ("Saber".to_string(), DynType::NoData),
            //                         ("SaberAddDecimalsDeposit".to_string(), DynType::NoData),
            //                         ("SaberAddDecimalsWithdraw".to_string(), DynType::NoData),
            //                         ("TokenSwap".to_string(), DynType::NoData),
            //                         ("Sencha".to_string(), DynType::NoData),
            //                         ("Step".to_string(), DynType::NoData),
            //                         ("Cropper".to_string(), DynType::NoData),
            //                         ("Raydium".to_string(), DynType::NoData),
            //                         (
            //                             "Crema".to_string(),
            //                             DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool)]),
            //                         ),
            //                         ("Lifinity".to_string(), DynType::NoData),
            //                         ("Mercurial".to_string(), DynType::NoData),
            //                         ("Cykura".to_string(), DynType::NoData),
            //                         (
            //                             "Serum".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "side".to_string(),
            //                                 DynType::Enum(vec![
            //                                     ("Bid".to_string(), DynType::NoData),
            //                                     ("Ask".to_string(), DynType::NoData),
            //                                 ]),
            //                             )]),
            //                         ),
            //                         ("MarinadeDeposit".to_string(), DynType::NoData),
            //                         ("MarinadeUnstake".to_string(), DynType::NoData),
            //                         (
            //                             "Aldrin".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "side".to_string(),
            //                                 DynType::Enum(vec![
            //                                     ("Bid".to_string(), DynType::NoData),
            //                                     ("Ask".to_string(), DynType::NoData),
            //                                 ]),
            //                             )]),
            //                         ),
            //                         (
            //                             "AldrinV2".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "side".to_string(),
            //                                 DynType::Enum(vec![
            //                                     ("Bid".to_string(), DynType::NoData),
            //                                     ("Ask".to_string(), DynType::NoData),
            //                                 ]),
            //                             )]),
            //                         ),
            //                         (
            //                             "Whirlpool".to_string(),
            //                             DynType::Struct(vec![("a_to_b".to_string(), DynType::Bool)]),
            //                         ),
            //                         (
            //                             "Invariant".to_string(),
            //                             DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)]),
            //                         ),
            //                         ("Meteora".to_string(), DynType::NoData),
            //                         ("GooseFX".to_string(), DynType::NoData),
            //                         (
            //                             "DeltaFi".to_string(),
            //                             DynType::Struct(vec![("stable".to_string(), DynType::Bool)]),
            //                         ),
            //                         ("Balansol".to_string(), DynType::NoData),
            //                         (
            //                             "MarcoPolo".to_string(),
            //                             DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)]),
            //                         ),
            //                         (
            //                             "Dradex".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "side".to_string(),
            //                                 DynType::Enum(vec![
            //                                     ("Bid".to_string(), DynType::NoData),
            //                                     ("Ask".to_string(), DynType::NoData),
            //                                 ]),
            //                             )]),
            //                         ),
            //                         ("LifinityV2".to_string(), DynType::NoData),
            //                         ("RaydiumClmm".to_string(), DynType::NoData),
            //                         (
            //                             "Openbook".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "side".to_string(),
            //                                 DynType::Enum(vec![
            //                                     ("Bid".to_string(), DynType::NoData),
            //                                     ("Ask".to_string(), DynType::NoData),
            //                                 ]),
            //                             )]),
            //                         ),
            //                         (
            //                             "Phoenix".to_string(),
            //                             DynType::Struct(vec![
            //                                 ("side".to_string(),
            //                                 DynType::Enum(vec![
            //                                     ("Bid".to_string(), DynType::NoData),
            //                                     ("Ask".to_string(), DynType::NoData),
            //                                 ]),
            //                             )]),
            //                         ),
            //                         (
            //                             "Symmetry".to_string(),
            //                             DynType::Struct(vec![
            //                                 ("from_token_id".to_string(), DynType::U64),
            //                                 ("to_token_id".to_string(), DynType::U64),
            //                             ]),
            //                         ),
            //                         ("TokenSwapV2".to_string(), DynType::NoData),
            //                         (
            //                             "HeliumTreasuryManagementRedeemV0".to_string(),
            //                             DynType::NoData,
            //                         ),
            //                         ("StakeDexStakeWrappedSol".to_string(), DynType::NoData),
            //                         (
            //                             "StakeDexSwapViaStake".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "bridge_stake_seed".to_string(),
            //                                 DynType::U32,
            //                             )]),
            //                         ),
            //                         ("GooseFXV2".to_string(), DynType::NoData),
            //                         ("Perps".to_string(), DynType::NoData),
            //                         ("PerpsAddLiquidity".to_string(), DynType::NoData),
            //                         ("PerpsRemoveLiquidity".to_string(), DynType::NoData),
            //                         ("MeteoraDlmm".to_string(), DynType::NoData),
            //                         (
            //                             "OpenBookV2".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "side".to_string(),
            //                                 DynType::Enum(vec![
            //                                     ("Bid".to_string(), DynType::NoData),
            //                                     ("Ask".to_string(), DynType::NoData),
            //                                 ]),
            //                             )]),
            //                         ),
            //                         ("RaydiumClmmV2".to_string(), DynType::NoData),
            //                         (
            //                             "StakeDexPrefundWithdrawStakeAndDepositStake".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "bridge_stake_seed".to_string(),
            //                                 DynType::U32,
            //                             )]),
            //                         ),
            //                         (
            //                             "Clone".to_string(),
            //                             DynType::Struct(vec![
            //                                 ("pool_index".to_string(), DynType::U8),
            //                                 ("quantity_is_input".to_string(), DynType::Bool),
            //                                 ("quantity_is_collateral".to_string(), DynType::Bool),
            //                             ]),
            //                         ),
            //                         (
            //                             "SanctumS".to_string(),
            //                             DynType::Struct(vec![
            //                                 ("src_lst_value_calc_accs".to_string(), DynType::U8),
            //                                 ("dst_lst_value_calc_accs".to_string(), DynType::U8),
            //                                 ("src_lst_index".to_string(), DynType::U32),
            //                                 ("dst_lst_index".to_string(), DynType::U32),
            //                             ]),
            //                         ),
            //                         (
            //                             "SanctumSAddLiquidity".to_string(),
            //                             DynType::Struct(vec![
            //                                 ("lst_value_calc_accs".to_string(), DynType::U8),
            //                                 ("lst_index".to_string(), DynType::U32),
            //                             ]),
            //                         ),
            //                         (
            //                             "SanctumSRemoveLiquidity".to_string(),
            //                             DynType::Struct(vec![
            //                                 ("lst_value_calc_accs".to_string(), DynType::U8),
            //                                 ("lst_index".to_string(), DynType::U32),
            //                             ]),
            //                         ),
            //                         ("RaydiumCP".to_string(), DynType::NoData),
            //                         (
            //                             "WhirlpoolSwapV2".to_string(),
            //                             DynType::Struct(vec![
            //                                 ("a_to_b".to_string(), DynType::Bool),
            //                                 ("remaining_accounts_info".to_string(), DynType::NoData),
            //                             ]),
            //                         ),
            //                         ("OneIntro".to_string(), DynType::NoData),
            //                         ("PumpdotfunWrappedBuy".to_string(), DynType::NoData),
            //                         ("PumpdotfunWrappedSell".to_string(), DynType::NoData),
            //                         ("PerpsV2".to_string(), DynType::NoData),
            //                         ("PerpsV2AddLiquidity".to_string(), DynType::NoData),
            //                         ("PerpsV2RemoveLiquidity".to_string(), DynType::NoData),
            //                         ("MoonshotWrappedBuy".to_string(), DynType::NoData),
            //                         ("MoonshotWrappedSell".to_string(), DynType::NoData),
            //                         ("StabbleStableSwap".to_string(), DynType::NoData),
            //                         ("StabbleWeightedSwap".to_string(), DynType::NoData),
            //                         (
            //                             "Obric".to_string(),
            //                             DynType::Struct(vec![("x_to_y".to_string(), DynType::Bool)]),
            //                         ),
            //                         ("FoxBuyFromEstimatedCost".to_string(), DynType::NoData),
            //                         (
            //                             "FoxClaimPartial".to_string(),
            //                             DynType::Struct(vec![("is_y".to_string(), DynType::Bool)]),
            //                         ),
            //                         (
            //                             "SolFi".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "is_quote_to_base".to_string(),
            //                                 DynType::Bool,
            //                             )]),
            //                         ),
            //                         ("SolayerDelegateNoInit".to_string(), DynType::NoData),
            //                         ("SolayerUndelegateNoInit".to_string(), DynType::NoData),
            //                         (
            //                             "TokenMill".to_string(),
            //                             DynType::Struct(vec![(
            //                                 "side".to_string(),
            //                                 DynType::Enum(vec![
            //                                     ("Bid".to_string(), DynType::NoData),
            //                                     ("Ask".to_string(), DynType::NoData),
            //                                 ]),
            //                             )]),
            //                         ),
            //                         ("DaosFunBuy".to_string(), DynType::NoData),
            //                         ("DaosFunSell".to_string(), DynType::NoData),
            //                         ("ZeroFi".to_string(), DynType::NoData),
            //                         ("StakeDexWithdrawWrappedSol".to_string(), DynType::NoData),
            //                         ("VirtualsBuy".to_string(), DynType::NoData),
            //                         ("VirtualsSell".to_string(), DynType::NoData),
            //                         (
            //                             "Peren".to_string(),
            //                             DynType::Struct(vec![
            //                                 ("in_index".to_string(), DynType::U8),
            //                                 ("out_index".to_string(), DynType::U8),
            //                             ]),
            //                         ),
            //                         ("PumpdotfunAmmBuy".to_string(), DynType::NoData),
            //                         ("PumpdotfunAmmSell".to_string(), DynType::NoData),
            //                         ("Gamma".to_string(), DynType::NoData),
            //                     ]),
            //                 ),
            //                 ("Percent".to_string(), DynType::U8),
            //                 ("InputIndex".to_string(), DynType::U8),
            //                 ("OutputIndex".to_string(), DynType::U8),
            //             ]))),
            //         },
            //         ParamInput {
            //             name: "InAmount".to_string(),
            //             param_type: DynType::U64,
            //         },
            //         ParamInput {
            //             name: "QuotedOutAmount".to_string(),
            //             param_type: DynType::U64,
            //         },
            //         ParamInput {
            //             name: "SlippageBps".to_string(),
            //             param_type: DynType::U16,
            //         },
            //         ParamInput {
            //             name: "PlatformFeeBps".to_string(),
            //             param_type: DynType::U8,
            //         },
            //     ],
            //     accounts: vec![
            //         "TokenProgram".to_string(),
            //         "UserTransferAuthority".to_string(),
            //         "UserSourceTokenAccount".to_string(),
            //         "UserDestinationTokenAccount".to_string(),
            //         "DestinationTokenAccount".to_string(),
            //         "PlatformFeeAccount".to_string(),
            //         "EventAuthority".to_string(),
            //         "Program".to_string(),
            //     ],
        };

        let result = decode_instruction_data(&filtered_instructions, ix_signature, true)
            .context("decode failed")
            .unwrap();

        // Save the filtered instructions to a new parquet file
        let mut file = File::create("decoded_instructions.parquet").unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, result.schema(), None).unwrap();
        writer.write(&result).unwrap();
        writer.close().unwrap();
    }
}
