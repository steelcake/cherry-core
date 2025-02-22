use anyhow::{anyhow, Context, Result};
use serde::Serialize;

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    pub fields: Fields,
    pub instructions: Vec<InstructionRequest>,
    pub transactions: Vec<TransactionRequest>,
    pub logs: Vec<LogRequest>,
    pub balances: Vec<BalanceRequest>,
    pub token_balances: Vec<TokenBalanceRequest>,
    pub rewards: Vec<RewardRequest>,
}

#[derive(Debug, Clone, Copy)]
pub struct Address(pub [u8; 32]);

#[derive(Debug, Clone, Copy)]
pub struct D1(pub [u8; 1]);

#[derive(Debug, Clone, Copy)]
pub struct D2(pub [u8; 2]);

#[derive(Debug, Clone, Copy)]
pub struct D3(pub [u8; 3]);

#[derive(Debug, Clone, Copy)]
pub struct D4(pub [u8; 4]);

#[derive(Debug, Clone, Copy)]
pub struct D8(pub [u8; 8]);

#[cfg(feature = "pyo3")]
fn extract_base58<const N: usize>(ob: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<[u8; N]> {
    use pyo3::types::PyAnyMethods;

    let s: &str = ob.extract()?;
    let mut out = [0; N];

    bs58::decode(s)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .onto(&mut out)
        .context("decode base58")?;

    Ok(out)
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for Address {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_base58(ob)?;
        Ok(Self(out))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for D1 {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_base58(ob)?;
        Ok(Self(out))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for D2 {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_base58(ob)?;
        Ok(Self(out))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for D3 {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_base58(ob)?;
        Ok(Self(out))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for D4 {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_base58(ob)?;
        Ok(Self(out))
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for D8 {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let out = extract_base58(ob)?;
        Ok(Self(out))
    }
}

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct InstructionRequest {
    pub program_id: Vec<Address>,
    pub d1: Vec<D1>,
    pub d2: Vec<D2>,
    pub d3: Vec<D3>,
    pub d4: Vec<D4>,
    pub d8: Vec<D8>,
    pub a0: Vec<Address>,
    pub a1: Vec<Address>,
    pub a2: Vec<Address>,
    pub a3: Vec<Address>,
    pub a4: Vec<Address>,
    pub a5: Vec<Address>,
    pub a6: Vec<Address>,
    pub a7: Vec<Address>,
    pub a8: Vec<Address>,
    pub a9: Vec<Address>,
    pub is_committed: bool,
    pub transaction: bool,
    pub transaction_token_balances: bool,
    pub logs: bool,
    pub inner_instructions: bool,
}

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct TransactionRequest {
    pub fee_payer: Vec<Address>,
    pub instructions: bool,
    pub logs: bool,
}

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct LogRequest {
    pub program_id: Vec<Address>,
    pub kind: Vec<LogKind>,
    pub transaction: bool,
    pub instruction: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum LogKind {
    Log,
    Data,
    Other,
}

impl LogKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Log => "log",
            Self::Data => "data",
            Self::Other => "other",
        }
    }

    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "log" => Ok(Self::Log),
            "data" => Ok(Self::Data),
            "other" => Ok(Self::Other),
            _ => Err(anyhow!("unknown log kind: {}", s)),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for LogKind {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;

        let s: &str = ob.extract().context("extract string")?;

        Ok(Self::from_str(s).context("from str")?)
    }
}

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct BalanceRequest {
    pub account: Vec<Address>,
    pub transaction: bool,
    pub transaction_instructions: bool,
}

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct TokenBalanceRequest {
    pub account: Vec<Address>,
    pub pre_program_id: Vec<Address>,
    pub post_program_id: Vec<Address>,
    pub pre_mint: Vec<Address>,
    pub post_mint: Vec<Address>,
    pub pre_owner: Vec<Address>,
    pub post_owner: Vec<Address>,
    pub transaction: bool,
    pub transaction_instructions: bool,
}

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct RewardRequest {
    pub pubkey: Vec<Address>,
}

#[derive(Serialize, Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct Fields {
    pub instruction: InstructionFields,
    pub transaction: TransactionFields,
    pub log: LogFields,
    pub balance: BalanceFields,
    pub token_balance: TokenBalanceFields,
    pub reward: RewardFields,
    pub block: BlockFields,
}

impl Fields {
    pub fn all() -> Self {
        Self {
            instruction: InstructionFields::all(),
            transaction: TransactionFields::all(),
            log: LogFields::all(),
            balance: BalanceFields::all(),
            token_balance: TokenBalanceFields::all(),
            reward: RewardFields::all(),
            block: BlockFields::all(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct InstructionFields {
    pub block_slot: bool,
    pub block_hash: bool,
    pub transaction_index: bool,
    pub instruction_address: bool,
    pub program_id: bool,
    pub a0: bool,
    pub a1: bool,
    pub a2: bool,
    pub a3: bool,
    pub a4: bool,
    pub a5: bool,
    pub a6: bool,
    pub a7: bool,
    pub a8: bool,
    pub a9: bool,
    pub rest_of_accounts: bool,
    pub data: bool,
    pub d1: bool,
    pub d2: bool,
    pub d4: bool,
    pub d8: bool,
    pub error: bool,
    pub compute_units_consumed: bool,
    pub is_committed: bool,
    pub has_dropped_log_messages: bool,
}

impl InstructionFields {
    pub fn all() -> Self {
        InstructionFields {
            block_slot: true,
            block_hash: true,
            transaction_index: true,
            instruction_address: true,
            program_id: true,
            a0: true,
            a1: true,
            a2: true,
            a3: true,
            a4: true,
            a5: true,
            a6: true,
            a7: true,
            a8: true,
            a9: true,
            rest_of_accounts: true,
            data: true,
            d1: true,
            d2: true,
            d4: true,
            d8: true,
            error: true,
            compute_units_consumed: true,
            is_committed: true,
            has_dropped_log_messages: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct TransactionFields {
    pub block_slot: bool,
    pub block_hash: bool,
    pub transaction_index: bool,
    pub version: bool,
    pub account_keys: bool,
    pub address_table_lookups: bool,
    pub num_readonly_signed_accounts: bool,
    pub num_readonly_unsigned_accounts: bool,
    pub num_required_signatures: bool,
    pub recent_blockhash: bool,
    pub signatures: bool,
    pub err: bool,
    pub fee: bool,
    pub compute_units_consumed: bool,
    pub loaded_readonly_addresses: bool,
    pub loaded_writable_addresses: bool,
    pub fee_payer: bool,
    pub has_dropped_log_messages: bool,
}

impl TransactionFields {
    pub fn all() -> Self {
        TransactionFields {
            block_slot: true,
            block_hash: true,
            transaction_index: true,
            version: true,
            account_keys: true,
            address_table_lookups: true,
            num_readonly_signed_accounts: true,
            num_readonly_unsigned_accounts: true,
            num_required_signatures: true,
            recent_blockhash: true,
            signatures: true,
            err: true,
            fee: true,
            compute_units_consumed: true,
            loaded_readonly_addresses: true,
            loaded_writable_addresses: true,
            fee_payer: true,
            has_dropped_log_messages: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct LogFields {
    pub block_slot: bool,
    pub block_hash: bool,
    pub transaction_index: bool,
    pub log_index: bool,
    pub instruction_address: bool,
    pub program_id: bool,
    pub kind: bool,
    pub message: bool,
}

impl LogFields {
    pub fn all() -> Self {
        LogFields {
            block_slot: true,
            block_hash: true,
            transaction_index: true,
            log_index: true,
            instruction_address: true,
            program_id: true,
            kind: true,
            message: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct BalanceFields {
    pub block_slot: bool,
    pub block_hash: bool,
    pub transaction_index: bool,
    pub account: bool,
    pub pre: bool,
    pub post: bool,
}

impl BalanceFields {
    pub fn all() -> Self {
        BalanceFields {
            block_slot: true,
            block_hash: true,
            transaction_index: true,
            account: true,
            pre: true,
            post: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct TokenBalanceFields {
    pub block_slot: bool,
    pub block_hash: bool,
    pub transaction_index: bool,
    pub account: bool,
    pub pre_mint: bool,
    pub post_mint: bool,
    pub pre_decimals: bool,
    pub post_decimals: bool,
    pub pre_program_id: bool,
    pub post_program_id: bool,
    pub pre_owner: bool,
    pub post_owner: bool,
    pub pre_amount: bool,
    pub post_amount: bool,
}

impl TokenBalanceFields {
    pub fn all() -> Self {
        TokenBalanceFields {
            block_slot: true,
            block_hash: true,
            transaction_index: true,
            account: true,
            pre_mint: true,
            post_mint: true,
            pre_decimals: true,
            post_decimals: true,
            pre_program_id: true,
            post_program_id: true,
            pre_owner: true,
            post_owner: true,
            pre_amount: true,
            post_amount: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct RewardFields {
    pub block_slot: bool,
    pub block_hash: bool,
    pub pubkey: bool,
    pub lamports: bool,
    pub post_balance: bool,
    pub reward_type: bool,
    pub commission: bool,
}

impl RewardFields {
    pub fn all() -> Self {
        RewardFields {
            block_slot: true,
            block_hash: true,
            pubkey: true,
            lamports: true,
            post_balance: true,
            reward_type: true,
            commission: true,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
#[cfg_attr(feature = "pyo3", derive(pyo3::FromPyObject))]
pub struct BlockFields {
    pub slot: bool,
    pub hash: bool,
    pub parent_slot: bool,
    pub parent_hash: bool,
    pub height: bool,
    pub timestamp: bool,
}

impl BlockFields {
    pub fn all() -> Self {
        BlockFields {
            slot: true,
            hash: true,
            parent_slot: true,
            parent_hash: true,
            height: true,
            timestamp: true,
        }
    }
}
