use anyhow::{anyhow, Context, Result};
use arrow::array::{
    builder, Array, BinaryArray, GenericBinaryArray, GenericListArray, GenericStringArray,
    LargeBinaryArray, LargeStringArray, OffsetSizeTrait, StringArray,
};
use arrow::{array::RecordBatch, datatypes::*};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::sync::Arc;
mod deserialize;
pub use deserialize::{deserialize_data, DynType, DynValue, ParamInput};
mod arrow_converter;
use arrow_converter::{to_arrow, to_arrow_dtype};

#[derive(Debug, Clone)]
pub struct InstructionSignature {
    pub discriminator: Vec<u8>,
    pub params: Vec<ParamInput>,
    pub accounts_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LogSignature {
    pub params: Vec<ParamInput>,
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for InstructionSignature {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;
        use pyo3::types::PyTypeMethods;

        let discriminator_ob = ob.getattr("discriminator")?;

        let discriminator_ob_type: String = discriminator_ob.get_type().name()?.to_string();
        let discriminator = match discriminator_ob_type.as_str() {
            "str" => {
                let s: &str = discriminator_ob.extract()?;
                hex_to_bytes(s).context("failed to decode hex")?
            }
            "bytes" => discriminator_ob.extract()?,
            _ => return Err(anyhow!("unknown type: {}", discriminator_ob_type).into()),
        };

        let params = ob.getattr("params")?.extract::<Vec<ParamInput>>()?;
        let accounts_names = ob.getattr("accounts_names")?.extract::<Vec<String>>()?;

        Ok(InstructionSignature {
            discriminator,
            params,
            accounts_names,
        })
    }
}

fn hex_to_bytes(hex_string: &str) -> Result<Vec<u8>> {
    let hex_string = hex_string.strip_prefix("0x").unwrap_or(hex_string);
    let hex_string = if hex_string.len() % 2 == 1 {
        format!("0{}", hex_string)
    } else {
        hex_string.to_string()
    };
    let out = (0..hex_string.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex_string[i..i + 2], 16)
                .context("failed to parse hexstring to bytes")
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(out)
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for LogSignature {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;

        let params = ob.getattr("params")?.extract::<Vec<ParamInput>>()?;

        Ok(LogSignature { params })
    }
}

fn unpack_rest_of_accounts<ListI: OffsetSizeTrait, InnerI: OffsetSizeTrait>(
    num_acc: usize,
    rest_of_acc: &GenericListArray<ListI>,
    account_arrays: &mut Vec<BinaryArray>,
) -> Result<()> {
    let data_size = rest_of_acc.len() * 32;

    for acc_arr in rest_of_acc.iter().flatten() {
        if acc_arr.len() < num_acc {
            return Err(anyhow!(
                "expected rest_of_accounts to have at least {} addresses but it has {}",
                num_acc,
                acc_arr.len()
            ));
        }
    }

    for i in 0..num_acc {
        let mut builder = builder::BinaryBuilder::with_capacity(rest_of_acc.len(), data_size);

        for acc_arr in rest_of_acc.iter() {
            let acc_arr = match acc_arr {
                Some(a) => a,
                None => {
                    builder.append_null();
                    continue;
                }
            };

            let arr = acc_arr
                .as_any()
                .downcast_ref::<GenericBinaryArray<InnerI>>()
                .unwrap();
            if !arr.is_null(i) {
                builder.append_value(arr.value(i));
            } else {
                builder.append_null();
            }
        }

        account_arrays.push(builder.finish());
    }

    Ok(())
}

pub fn decode_instructions_batch(
    signature: InstructionSignature,
    batch: &RecordBatch,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    let mut account_arrays: Vec<BinaryArray> = Vec::with_capacity(20);

    for i in 0..signature.accounts_names.len().min(10) {
        let col_name = format!("a{}", i);
        let col = batch
            .column_by_name(&col_name)
            .with_context(|| format!("account {} not found but was required", i))?;

        if col.data_type() == &DataType::Binary {
            account_arrays.push(col.as_any().downcast_ref::<BinaryArray>().unwrap().clone());
        } else if col.data_type() == &DataType::LargeBinary {
            account_arrays.push(
                arrow::compute::cast(col, &DataType::Binary)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .clone(),
            );
        }
    }

    if signature.accounts_names.len() > 10 {
        let rest_of_acc = batch
            .column_by_name("rest_of_accounts")
            .context("rest_of_accounts column not found in instructions batch")?;

        let num_acc = signature.accounts_names.len() - 10;
        if rest_of_acc.data_type() == &DataType::new_list(DataType::Binary, true) {
            unpack_rest_of_accounts::<i32, i32>(
                num_acc,
                rest_of_acc.as_any().downcast_ref().unwrap(),
                &mut account_arrays,
            )
            .context("unpack rest_of_accounts column")?;
        } else if rest_of_acc.data_type() == &DataType::new_list(DataType::LargeBinary, true) {
            unpack_rest_of_accounts::<i32, i64>(
                num_acc,
                rest_of_acc.as_any().downcast_ref().unwrap(),
                &mut account_arrays,
            )
            .context("unpack rest_of_accounts column")?;
        } else if rest_of_acc.data_type() == &DataType::new_large_list(DataType::Binary, true) {
            unpack_rest_of_accounts::<i64, i32>(
                num_acc,
                rest_of_acc.as_any().downcast_ref().unwrap(),
                &mut account_arrays,
            )
            .context("unpack rest_of_accounts column")?;
        } else if rest_of_acc.data_type() == &DataType::new_large_list(DataType::LargeBinary, true)
        {
            unpack_rest_of_accounts::<i64, i64>(
                num_acc,
                rest_of_acc.as_any().downcast_ref().unwrap(),
                &mut account_arrays,
            )
            .context("unpack rest_of_accounts column")?;
        }
    }

    let data_col = batch
        .column_by_name("data")
        .context("data column not found in instructions batch")?;

    if data_col.data_type() == &DataType::Binary {
        decode_instructions(
            signature,
            &account_arrays,
            data_col.as_any().downcast_ref::<BinaryArray>().unwrap(),
            allow_decode_fail,
        )
    } else if data_col.data_type() == &DataType::LargeBinary {
        decode_instructions(
            signature,
            &account_arrays,
            data_col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap(),
            allow_decode_fail,
        )
    } else {
        Err(anyhow!(
            "expected the data column to be Binary or LargeBinary"
        ))
    }
}

pub fn decode_instructions<I: OffsetSizeTrait>(
    signature: InstructionSignature,
    accounts: &[BinaryArray],
    data: &GenericBinaryArray<I>,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    let num_params = signature.params.len();

    let mut decoded_params_vec: Vec<Vec<Option<DynValue>>> =
        (0..num_params).map(|_| Vec::new()).collect();

    for row_idx in 0..data.len() {
        if data.is_null(row_idx) {
            if allow_decode_fail {
                log::debug!("Instruction data is null in row {}", row_idx);
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            } else {
                return Err(anyhow::anyhow!(
                    "Instruction data is null in row {}",
                    row_idx
                ));
            }
        }

        let instruction_data = data.value(row_idx).to_vec();
        let data_result = match_discriminators(&instruction_data, &signature.discriminator);
        let data = match data_result {
            Ok(data) => data,
            Err(e) if allow_decode_fail => {
                log::debug!("Error matching discriminators in row {}: {:?}", row_idx, e);
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error matching discriminators in row {}: {:?}",
                    row_idx,
                    e
                ));
            }
        };

        // Don't error on remaining data because this is the behavior implemented by anchor.
        // Note that borsh does error if there is remaining data after deserialization but anchor
        // doesn't.
        //
        // Might be a good idea to extract this to a parameter to this function as well
        let error_on_remanining = false;
        let decoded_ix_result = deserialize_data(&data, &signature.params, error_on_remanining);
        let decoded_ix = match decoded_ix_result {
            Ok(ix) => ix,
            Err(e) if allow_decode_fail => {
                log::debug!(
                    "Error deserializing instruction in row {}: {:?}",
                    row_idx,
                    e
                );
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error deserializing instruction in row {}: {:?}",
                    row_idx,
                    e
                ));
            }
        };

        for (i, value) in decoded_ix.into_iter().enumerate() {
            decoded_params_vec[i].push(Some(value));
        }
    }

    let mut data_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(decoded_params_vec.len());
    for (i, v) in decoded_params_vec.iter().enumerate() {
        let array = to_arrow(&signature.params[i].param_type, v.clone())
            .context("unable to convert instruction value to a arrow format value")?;
        data_arrays.push(array);
    }

    let mut data_fields = Vec::with_capacity(signature.params.len());
    for param in &signature.params {
        let field = Field::new(
            param.name.clone(),
            to_arrow_dtype(&param.param_type)
                .context("unable to convert instruction param type to arrow dtype")?,
            true,
        );
        data_fields.push(field);
    }

    let acc_names_len = signature.accounts_names.len();
    let mut accounts_arrays = Vec::new();
    let mut acc_fields = Vec::new();

    for i in 0..acc_names_len {
        let arr = accounts
            .get(i)
            .context(format!("Account a{} not found during decoding", i))?;

        if arr.data_type() == &DataType::LargeBinary {
            accounts_arrays.push(arrow::compute::cast(arr, &DataType::Binary).unwrap());
        } else {
            accounts_arrays.push(Arc::new(arr.clone()) as Arc<dyn Array>);
        }

        if signature.accounts_names[i].is_empty() {
            let field = Field::new(format!("a{}", i), DataType::Binary, true);
            acc_fields.push(field);
        } else {
            let field = Field::new(signature.accounts_names[i].clone(), DataType::Binary, true);
            acc_fields.push(field);
        }
    }

    let decoded_instructions_array = data_arrays
        .into_iter()
        .chain(accounts_arrays)
        .collect::<Vec<_>>();
    let decoded_instructions_fields = data_fields
        .into_iter()
        .chain(acc_fields.clone())
        .collect::<Vec<_>>();

    let schema = Arc::new(Schema::new(decoded_instructions_fields));
    let batch = RecordBatch::try_new(schema, decoded_instructions_array)
        .context("Failed to create record batch from data arrays")?;

    Ok(batch)
}

pub fn decode_logs_batch(
    signature: LogSignature,
    batch: &RecordBatch,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    let message_col = batch
        .column_by_name("message")
        .context("message column not found in logs batch")?;

    if message_col.data_type() == &DataType::Utf8 {
        decode_logs(
            signature,
            message_col.as_any().downcast_ref::<StringArray>().unwrap(),
            allow_decode_fail,
        )
    } else if message_col.data_type() == &DataType::LargeUtf8 {
        decode_logs(
            signature,
            message_col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap(),
            allow_decode_fail,
        )
    } else {
        Err(anyhow!("expected String or LargeString message column"))
    }
}

pub fn decode_logs<I: OffsetSizeTrait>(
    signature: LogSignature,
    data: &GenericStringArray<I>,
    allow_decode_fail: bool,
) -> Result<RecordBatch> {
    let num_params = signature.params.len();

    let mut decoded_params_vec: Vec<Vec<Option<DynValue>>> =
        (0..num_params).map(|_| Vec::new()).collect();

    for row_idx in 0..data.len() {
        if data.is_null(row_idx) {
            if allow_decode_fail {
                log::debug!("Log data is null in row {}", row_idx);
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            } else {
                return Err(anyhow::anyhow!("Log data is null in row {}", row_idx));
            }
        }

        let log_data = data.value(row_idx);
        let log_data = STANDARD.decode(log_data);
        let log_data = match log_data {
            Ok(log_data) => log_data,
            Err(e) if allow_decode_fail => {
                log::debug!(
                    "Error base 64 decoding log data in row {}: {:?}",
                    row_idx,
                    e
                );
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error base 64 decoding log data in row {}: {:?}",
                    row_idx,
                    e
                ));
            }
        };

        let decoded_log_result = deserialize_data(&log_data, &signature.params, false);
        let decoded_log = match decoded_log_result {
            Ok(log) => log,
            Err(e) if allow_decode_fail => {
                log::debug!("Error deserializing log in row {}: {:?}", row_idx, e);
                decoded_params_vec.iter_mut().for_each(|v| v.push(None));
                continue;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error deserializing log in row {}: {:?}",
                    row_idx,
                    e
                ));
            }
        };

        for (i, value) in decoded_log.into_iter().enumerate() {
            decoded_params_vec[i].push(Some(value));
        }
    }

    let mut data_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(decoded_params_vec.len());
    for (i, v) in decoded_params_vec.iter().enumerate() {
        let array = to_arrow(&signature.params[i].param_type, v.clone())
            .context("unable to convert log value to a arrow format value")?;
        data_arrays.push(array);
    }

    let mut data_fields = Vec::with_capacity(signature.params.len());
    for param in &signature.params {
        let field = Field::new(
            param.name.clone(),
            to_arrow_dtype(&param.param_type)
                .context("unable to convert log param type to arrow dtype")?,
            true,
        );
        data_fields.push(field);
    }

    let schema = Arc::new(Schema::new(data_fields));
    let batch = RecordBatch::try_new(schema, data_arrays)
        .context("Failed to create record batch from data arrays")?;

    Ok(batch)
}

pub fn match_discriminators(instr_data: &[u8], discriminator: &[u8]) -> Result<Vec<u8>> {
    let discriminator_len = discriminator.len();
    if instr_data.len() < discriminator_len {
        return Err(anyhow::anyhow!(
            "Instruction data is too short to contain discriminator. Expected at least {} bytes, got {} bytes",
            discriminator_len,
            instr_data.len()
        ));
    }
    let disc = &instr_data[..discriminator_len].to_vec();
    let ix_data = &instr_data[discriminator_len..];
    if !disc.eq(discriminator) {
        return Err(anyhow::anyhow!(
            "Instruction data discriminator doesn't match signature discriminator"
        ));
    }
    Ok(ix_data.to_vec())
}

pub fn instruction_signature_to_arrow_schema(signature: &InstructionSignature) -> Result<Schema> {
    let mut fields = Vec::new();

    for param in &signature.params {
        let field = Field::new(
            param.name.clone(),
            to_arrow_dtype(&param.param_type)
                .context("unable to convert instruction param type to arrow dtype")?,
            true,
        );
        fields.push(field);
    }

    for account in &signature.accounts_names {
        let field = Field::new(account.clone(), DataType::Binary, true);
        fields.push(field);
    }

    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deserialize::{DynType, ParamInput};
    use std::fs::File;

    #[test]
    #[ignore]
    fn test_instructions_with_real_data() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open("jup.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let instructions = reader.next().unwrap().unwrap();
        let ix_signature = InstructionSignature {
            // // SPL Token Transfer
            // discriminator: &[3],
            // params: vec![ParamInput {
            //     name: "Amount".to_string(),
            //     param_type: DynType::U64,
            // }],
            // accounts: vec![
            //     "Source".to_string(),
            //     "Destination".to_string(),
            //     "Authority".to_string(),
            // ],

            // // JUP SwapEvent
            // discriminator: &[
            //     228, 69, 165, 46, 81, 203, 154, 29, 64, 198, 205, 232, 38, 8, 113, 226,
            // ],
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
            // accounts: vec![],

            // JUP Route
            discriminator: vec![229, 23, 203, 151, 122, 227, 173, 42],
            params: vec![
                ParamInput {
                    name: "RoutePlan".to_string(),
                    param_type: DynType::Array(Box::new(DynType::Struct(vec![
                        (
                            "Swap".to_string(),
                            DynType::Enum(vec![
                                ("Saber".to_string(), None),
                                ("SaberAddDecimalsDeposit".to_string(), None),
                                ("SaberAddDecimalsWithdraw".to_string(), None),
                                ("TokenSwap".to_string(), None),
                                ("Sencha".to_string(), None),
                                ("Step".to_string(), None),
                                ("Cropper".to_string(), None),
                                ("Raydium".to_string(), None),
                                (
                                    "Crema".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "a_to_b".to_string(),
                                        DynType::Bool,
                                    )])),
                                ),
                                ("Lifinity".to_string(), None),
                                ("Mercurial".to_string(), None),
                                ("Cykura".to_string(), None),
                                (
                                    "Serum".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), None),
                                            ("Ask".to_string(), None),
                                        ]),
                                    )])),
                                ),
                                ("MarinadeDeposit".to_string(), None),
                                ("MarinadeUnstake".to_string(), None),
                                (
                                    "Aldrin".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), None),
                                            ("Ask".to_string(), None),
                                        ]),
                                    )])),
                                ),
                                (
                                    "AldrinV2".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), None),
                                            ("Ask".to_string(), None),
                                        ]),
                                    )])),
                                ),
                                (
                                    "Whirlpool".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "a_to_b".to_string(),
                                        DynType::Bool,
                                    )])),
                                ),
                                (
                                    "Invariant".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "x_to_y".to_string(),
                                        DynType::Bool,
                                    )])),
                                ),
                                ("Meteora".to_string(), None),
                                ("GooseFX".to_string(), None),
                                (
                                    "DeltaFi".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "stable".to_string(),
                                        DynType::Bool,
                                    )])),
                                ),
                                ("Balansol".to_string(), None),
                                (
                                    "MarcoPolo".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "x_to_y".to_string(),
                                        DynType::Bool,
                                    )])),
                                ),
                                (
                                    "Dradex".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), None),
                                            ("Ask".to_string(), None),
                                        ]),
                                    )])),
                                ),
                                ("LifinityV2".to_string(), None),
                                ("RaydiumClmm".to_string(), None),
                                (
                                    "Openbook".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), None),
                                            ("Ask".to_string(), None),
                                        ]),
                                    )])),
                                ),
                                (
                                    "Phoenix".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), None),
                                            ("Ask".to_string(), None),
                                        ]),
                                    )])),
                                ),
                                (
                                    "Symmetry".to_string(),
                                    Some(DynType::Struct(vec![
                                        ("from_token_id".to_string(), DynType::U64),
                                        ("to_token_id".to_string(), DynType::U64),
                                    ])),
                                ),
                                ("TokenSwapV2".to_string(), None),
                                ("HeliumTreasuryManagementRedeemV0".to_string(), None),
                                ("StakeDexStakeWrappedSol".to_string(), None),
                                (
                                    "StakeDexSwapViaStake".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "bridge_stake_seed".to_string(),
                                        DynType::U32,
                                    )])),
                                ),
                                ("GooseFXV2".to_string(), None),
                                ("Perps".to_string(), None),
                                ("PerpsAddLiquidity".to_string(), None),
                                ("PerpsRemoveLiquidity".to_string(), None),
                                ("MeteoraDlmm".to_string(), None),
                                (
                                    "OpenBookV2".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), None),
                                            ("Ask".to_string(), None),
                                        ]),
                                    )])),
                                ),
                                ("RaydiumClmmV2".to_string(), None),
                                (
                                    "StakeDexPrefundWithdrawStakeAndDepositStake".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "bridge_stake_seed".to_string(),
                                        DynType::U32,
                                    )])),
                                ),
                                (
                                    "Clone".to_string(),
                                    Some(DynType::Struct(vec![
                                        ("pool_index".to_string(), DynType::U8),
                                        ("quantity_is_input".to_string(), DynType::Bool),
                                        ("quantity_is_collateral".to_string(), DynType::Bool),
                                    ])),
                                ),
                                (
                                    "SanctumS".to_string(),
                                    Some(DynType::Struct(vec![
                                        ("src_lst_value_calc_accs".to_string(), DynType::U8),
                                        ("dst_lst_value_calc_accs".to_string(), DynType::U8),
                                        ("src_lst_index".to_string(), DynType::U32),
                                        ("dst_lst_index".to_string(), DynType::U32),
                                    ])),
                                ),
                                (
                                    "SanctumSAddLiquidity".to_string(),
                                    Some(DynType::Struct(vec![
                                        ("lst_value_calc_accs".to_string(), DynType::U8),
                                        ("lst_index".to_string(), DynType::U32),
                                    ])),
                                ),
                                (
                                    "SanctumSRemoveLiquidity".to_string(),
                                    Some(DynType::Struct(vec![
                                        ("lst_value_calc_accs".to_string(), DynType::U8),
                                        ("lst_index".to_string(), DynType::U32),
                                    ])),
                                ),
                                ("RaydiumCP".to_string(), None),
                                (
                                    "WhirlpoolSwapV2".to_string(),
                                    Some(DynType::Struct(vec![
                                        ("a_to_b".to_string(), DynType::Bool),
                                        (
                                            "remaining_accounts_info".to_string(),
                                            DynType::Struct(vec![(
                                                "slices".to_string(),
                                                DynType::Array(Box::new(DynType::Struct(vec![(
                                                    "remaining_accounts_slice".to_string(),
                                                    DynType::Struct(vec![
                                                        ("accounts_type".to_string(), DynType::U8),
                                                        ("length".to_string(), DynType::U8),
                                                    ]),
                                                )]))),
                                            )]),
                                        ),
                                    ])),
                                ),
                                ("OneIntro".to_string(), None),
                                ("PumpdotfunWrappedBuy".to_string(), None),
                                ("PumpdotfunWrappedSell".to_string(), None),
                                ("PerpsV2".to_string(), None),
                                ("PerpsV2AddLiquidity".to_string(), None),
                                ("PerpsV2RemoveLiquidity".to_string(), None),
                                ("MoonshotWrappedBuy".to_string(), None),
                                ("MoonshotWrappedSell".to_string(), None),
                                ("StabbleStableSwap".to_string(), None),
                                ("StabbleWeightedSwap".to_string(), None),
                                (
                                    "Obric".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "x_to_y".to_string(),
                                        DynType::Bool,
                                    )])),
                                ),
                                ("FoxBuyFromEstimatedCost".to_string(), None),
                                (
                                    "FoxClaimPartial".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "is_y".to_string(),
                                        DynType::Bool,
                                    )])),
                                ),
                                (
                                    "SolFi".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "is_quote_to_base".to_string(),
                                        DynType::Bool,
                                    )])),
                                ),
                                ("SolayerDelegateNoInit".to_string(), None),
                                ("SolayerUndelegateNoInit".to_string(), None),
                                (
                                    "TokenMill".to_string(),
                                    Some(DynType::Struct(vec![(
                                        "side".to_string(),
                                        DynType::Enum(vec![
                                            ("Bid".to_string(), None),
                                            ("Ask".to_string(), None),
                                        ]),
                                    )])),
                                ),
                                ("DaosFunBuy".to_string(), None),
                                ("DaosFunSell".to_string(), None),
                                ("ZeroFi".to_string(), None),
                                ("StakeDexWithdrawWrappedSol".to_string(), None),
                                ("VirtualsBuy".to_string(), None),
                                ("VirtualsSell".to_string(), None),
                                (
                                    "Peren".to_string(),
                                    Some(DynType::Struct(vec![
                                        ("in_index".to_string(), DynType::U8),
                                        ("out_index".to_string(), DynType::U8),
                                    ])),
                                ),
                                ("PumpdotfunAmmBuy".to_string(), None),
                                ("PumpdotfunAmmSell".to_string(), None),
                                ("Gamma".to_string(), None),
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
            accounts_names: vec![
                "TokenProgram".to_string(),
                "UserTransferAuthority".to_string(),
                "UserSourceTokenAccount".to_string(),
                "UserDestinationTokenAccount".to_string(),
                "DestinationTokenAccount".to_string(),
                "PlatformFeeAccount".to_string(),
                "EventAuthority".to_string(),
                "Program".to_string(),
                "test8".to_string(),
                "test9".to_string(),
            ],
        };

        let result = decode_instructions_batch(ix_signature, &instructions, true)
            .context("decode failed")
            .unwrap();

        // Save the filtered instructions to a new parquet file
        let mut file = File::create("decoded_instructions.parquet").unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, result.schema(), None).unwrap();
        writer.write(&result).unwrap();
        writer.close().unwrap();
    }

    #[test]
    #[ignore]
    fn test_decode_logs_with_real_data() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open("logs.parquet").unwrap()).unwrap();
        let mut reader = builder.build().unwrap();
        let logs = reader.next().unwrap().unwrap();

        let signature = LogSignature {
            params: vec![
                ParamInput {
                    name: "whirlpool".to_string(),
                    param_type: DynType::FixedArray(Box::new(DynType::U8), 32),
                },
                ParamInput {
                    name: "a_to_b".to_string(),
                    param_type: DynType::Bool,
                },
                ParamInput {
                    name: "pre_sqrt_price".to_string(),
                    param_type: DynType::U128,
                },
                ParamInput {
                    name: "post_sqrt_price".to_string(),
                    param_type: DynType::U128,
                },
                ParamInput {
                    name: "x".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "input_amount".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "output_amount".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "input_transfer_fee".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "output_transfer_fee".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "lp_fee".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "protocol_fee".to_string(),
                    param_type: DynType::U64,
                },
            ],
        };

        let result = decode_logs_batch(signature, &logs, true)
            .context("decode failed")
            .unwrap();

        // Save the filtered instructions to a new parquet file
        let mut file = File::create("decoded_logs.parquet").unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, result.schema(), None).unwrap();
        writer.write(&result).unwrap();
        writer.close().unwrap();
    }

    #[test]
    #[ignore]
    fn test_instruction_signature_to_arrow_schema() {
        // Create a test instruction signature
        let signature = InstructionSignature {
            discriminator: vec![],
            params: vec![
                ParamInput {
                    name: "amount".to_string(),
                    param_type: DynType::U64,
                },
                ParamInput {
                    name: "is_valid".to_string(),
                    param_type: DynType::Bool,
                },
                ParamInput {
                    name: "amm".to_string(),
                    param_type: DynType::FixedArray(Box::new(DynType::U8), 32),
                },
            ],
            accounts_names: vec!["source".to_string(), "destination".to_string()],
        };

        // Convert to schema
        let schema = instruction_signature_to_arrow_schema(&signature).unwrap();

        // Verify the schema has the correct number of fields
        assert_eq!(schema.fields().len(), 5); // 2 params + 2 accounts

        // Verify param fields
        let amount_field = schema.field_with_name("amount").unwrap();
        assert_eq!(amount_field.name(), "amount");
        assert!(amount_field.is_nullable());

        let is_valid_field = schema.field_with_name("is_valid").unwrap();
        assert_eq!(is_valid_field.name(), "is_valid");
        assert!(is_valid_field.is_nullable());

        let amm_field = schema.field_with_name("amm").unwrap();
        assert_eq!(amm_field.name(), "amm");
        assert!(amm_field.is_nullable());

        // Verify account fields
        let source_field = schema.field_with_name("source").unwrap();
        assert_eq!(source_field.name(), "source");
        assert_eq!(source_field.data_type(), &DataType::Binary);
        assert!(source_field.is_nullable());

        let dest_field = schema.field_with_name("destination").unwrap();
        assert_eq!(dest_field.name(), "destination");
        assert_eq!(dest_field.data_type(), &DataType::Binary);
        assert!(dest_field.is_nullable());
    }
}
