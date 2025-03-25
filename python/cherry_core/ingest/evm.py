from typing import Optional
from dataclasses import dataclass, field


@dataclass
class TransactionRequest:
    from_: list[str] = field(default_factory=list)
    to: list[str] = field(default_factory=list)
    sighash: list[str] = field(default_factory=list)
    status: list[int] = field(default_factory=list)
    type_: list[int] = field(default_factory=list)
    contract_deployment_address: list[str] = field(default_factory=list)
    hash: list[str] = field(default_factory=list)
    include_logs: bool = False
    include_traces: bool = False
    include_blocks: bool = False


@dataclass
class LogRequest:
    address: list[str] = field(default_factory=list)
    topic0: list[str] = field(default_factory=list)
    topic1: list[str] = field(default_factory=list)
    topic2: list[str] = field(default_factory=list)
    topic3: list[str] = field(default_factory=list)
    include_transactions: bool = False
    include_transaction_logs: bool = False
    include_transaction_traces: bool = False
    include_blocks: bool = False


@dataclass
class TraceRequest:
    from_: list[str] = field(default_factory=list)
    to: list[str] = field(default_factory=list)
    address: list[str] = field(default_factory=list)
    call_type: list[str] = field(default_factory=list)
    reward_type: list[str] = field(default_factory=list)
    type_: list[str] = field(default_factory=list)
    sighash: list[str] = field(default_factory=list)
    author: list[str] = field(default_factory=list)
    include_transactions: bool = False
    include_transaction_logs: bool = False
    include_transaction_traces: bool = False
    include_blocks: bool = False


@dataclass
class BlockFields:
    number: bool = False
    hash: bool = False
    parent_hash: bool = False
    nonce: bool = False
    sha3_uncles: bool = False
    logs_bloom: bool = False
    transactions_root: bool = False
    state_root: bool = False
    receipts_root: bool = False
    miner: bool = False
    difficulty: bool = False
    total_difficulty: bool = False
    extra_data: bool = False
    size: bool = False
    gas_limit: bool = False
    gas_used: bool = False
    timestamp: bool = False
    uncles: bool = False
    base_fee_per_gas: bool = False
    blob_gas_used: bool = False
    excess_blob_gas: bool = False
    parent_beacon_block_root: bool = False
    withdrawals_root: bool = False
    withdrawals: bool = False
    l1_block_number: bool = False
    send_count: bool = False
    send_root: bool = False
    mix_hash: bool = False


@dataclass
class TransactionFields:
    block_hash: bool = False
    block_number: bool = False
    from_: bool = False
    gas: bool = False
    gas_price: bool = False
    hash: bool = False
    input: bool = False
    nonce: bool = False
    to: bool = False
    transaction_index: bool = False
    value: bool = False
    v: bool = False
    r: bool = False
    s: bool = False
    max_priority_fee_per_gas: bool = False
    max_fee_per_gas: bool = False
    chain_id: bool = False
    cumulative_gas_used: bool = False
    effective_gas_price: bool = False
    gas_used: bool = False
    contract_address: bool = False
    logs_bloom: bool = False
    type_: bool = False
    root: bool = False
    status: bool = False
    sighash: bool = False
    y_parity: bool = False
    access_list: bool = False
    l1_fee: bool = False
    l1_gas_price: bool = False
    l1_fee_scalar: bool = False
    gas_used_for_l1: bool = False
    max_fee_per_blob_gas: bool = False
    blob_versioned_hashes: bool = False
    deposit_nonce: bool = False
    blob_gas_price: bool = False
    deposit_receipt_version: bool = False
    blob_gas_used: bool = False
    l1_base_fee_scalar: bool = False
    l1_blob_base_fee: bool = False
    l1_blob_base_fee_scalar: bool = False
    l1_block_number: bool = False
    mint: bool = False
    source_hash: bool = False


@dataclass
class LogFields:
    removed: bool = False
    log_index: bool = False
    transaction_index: bool = False
    transaction_hash: bool = False
    block_hash: bool = False
    block_number: bool = False
    address: bool = False
    data: bool = False
    topic0: bool = False
    topic1: bool = False
    topic2: bool = False
    topic3: bool = False


@dataclass
class TraceFields:
    from_: bool = False
    to: bool = False
    call_type: bool = False
    gas: bool = False
    input: bool = False
    init: bool = False
    value: bool = False
    author: bool = False
    reward_type: bool = False
    block_hash: bool = False
    block_number: bool = False
    address: bool = False
    code: bool = False
    gas_used: bool = False
    output: bool = False
    subtraces: bool = False
    trace_address: bool = False
    transaction_hash: bool = False
    transaction_position: bool = False
    type_: bool = False
    error: bool = False
    sighash: bool = False
    action_address: bool = False
    balance: bool = False
    refund_address: bool = False


@dataclass
class Fields:
    block: BlockFields = field(default_factory=BlockFields)
    transaction: TransactionFields = field(default_factory=TransactionFields)
    log: LogFields = field(default_factory=LogFields)
    trace: TraceFields = field(default_factory=TraceFields)


@dataclass
class Query:
    from_block: int = 0
    to_block: Optional[int] = None
    include_all_blocks: bool = False
    transactions: list[TransactionRequest] = field(default_factory=list)
    logs: list[LogRequest] = field(default_factory=list)
    traces: list[TraceRequest] = field(default_factory=list)
    fields: Fields = field(default_factory=Fields)
