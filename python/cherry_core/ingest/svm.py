from typing import Optional 
from dataclasses import dataclass, field 
from enum import Enum

@dataclass
class InstructionFields:
    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    instruction_address: bool = False
    program_id: bool = False
    a0: bool = False
    a1: bool = False
    a2: bool = False
    a3: bool = False
    a4: bool = False
    a5: bool = False
    a6: bool = False
    a7: bool = False
    a8: bool = False
    a9: bool = False
    rest_of_accounts: bool = False
    data: bool = False
    d1: bool = False
    d2: bool = False
    d4: bool = False
    d8: bool = False
    error: bool = False
    compute_units_consumed: bool = False
    is_committed: bool = False
    has_dropped_log_messages: bool = False

@dataclass
class TransactionFields:
    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    version: bool = False
    account_keys: bool = False
    address_table_lookups: bool = False
    num_readonly_signed_accounts: bool = False
    num_readonly_unsigned_accounts: bool = False
    num_required_signatures: bool = False
    recent_blockhash: bool = False
    signatures: bool = False
    err: bool = False
    fee: bool = False
    compute_units_consumed: bool = False
    loaded_readonly_addresses: bool = False
    loaded_writable_addresses: bool = False
    fee_payer: bool = False
    has_dropped_log_messages: bool = False

@dataclass
class LogFields:
    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    log_index: bool = False
    instruction_address: bool = False
    program_id: bool = False
    kind: bool = False
    message: bool = False

@dataclass
class BalanceFields:
    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    account: bool = False
    pre: bool = False
    post: bool = False

@dataclass
class TokenBalanceFields:
    block_slot: bool = False
    block_hash: bool = False
    transaction_index: bool = False
    account: bool = False
    pre_mint: bool = False
    post_mint: bool = False
    pre_decimals: bool = False
    post_decimals: bool = False
    pre_program_id: bool = False
    post_program_id: bool = False
    pre_owner: bool = False
    post_owner: bool = False
    pre_amount: bool = False
    post_amount: bool = False

@dataclass
class RewardFields:
    block_slot: bool = False
    block_hash: bool = False
    pubkey: bool = False
    lamports: bool = False
    post_balance: bool = False
    reward_type: bool = False
    commission: bool = False

@dataclass
class BlockFields:
    slot: bool = False
    hash: bool = False
    parent_slot: bool = False
    parent_hash: bool = False
    height: bool = False
    timestamp: bool = False

@dataclass
class Fields:
    instruction: InstructionFields = field(default_factory=InstructionFields)
    transaction: TransactionFields = field(default_factory=TransactionFields)
    log: LogFields = field(default_factory=LogFields)
    balance: BalanceFields = field(default_factory=BalanceFields)
    token_balance: TokenBalanceFields = field(default_factory=TokenBalanceFields)
    reward: RewardFields = field(default_factory=RewardFields)
    block: BlockFields = field(default_factory=BlockFields)

@dataclass
class InstructionRequest:
    program_id: list[str] = field(default_factory=list)
    d1: list[str] = field(default_factory=list)
    d2: list[str] = field(default_factory=list)
    d3: list[str] = field(default_factory=list)
    d4: list[str] = field(default_factory=list)
    d8: list[str] = field(default_factory=list)
    a0: list[str] = field(default_factory=list)
    a1: list[str] = field(default_factory=list)
    a2: list[str] = field(default_factory=list)
    a3: list[str] = field(default_factory=list)
    a4: list[str] = field(default_factory=list)
    a5: list[str] = field(default_factory=list)
    a6: list[str] = field(default_factory=list)
    a7: list[str] = field(default_factory=list)
    a8: list[str] = field(default_factory=list)
    a9: list[str] = field(default_factory=list)
    is_committed: bool = False
    transaction: bool = False
    transaction_token_balances: bool = False
    logs: bool = False
    inner_instructions: bool = False

@dataclass
class TransactionRequest:
    fee_payer: list[str] = field(default_factory=list)
    instructions: bool = False
    logs: bool = False

class LogKind(str, Enum):
    LOG = "log"
    DATA = "data"
    OTHER = "other"

@dataclass
class LogRequest:
    program_id: list[str] = field(default_factory=list)
    kind: list[LogKind] = field(default_factory=list)  # Assuming LogKind is represented as a string
    transaction: bool = False
    instruction: bool = False

@dataclass
class BalanceRequest:
    account: list[str] = field(default_factory=list)
    transaction: bool = False
    transaction_instructions: bool = False

@dataclass
class TokenBalanceRequest:
    account: list[str] = field(default_factory=list)
    pre_program_id: list[str] = field(default_factory=list)
    post_program_id: list[str] = field(default_factory=list)
    pre_mint: list[str] = field(default_factory=list)
    post_mint: list[str] = field(default_factory=list)
    pre_owner: list[str] = field(default_factory=list)
    post_owner: list[str] = field(default_factory=list)
    transaction: bool = False
    transaction_instructions: bool = False

@dataclass
class RewardRequest:
    pubkey: list[str] = field(default_factory=list)

@dataclass
class Query:
    from_block: int = 0
    to_block: Optional[int] = None
    include_all_blocks: bool = False
    fields: Fields = field(default_factory=Fields)
    instructions: list[InstructionRequest] = field(default_factory=list)
    transactions: list[TransactionRequest] = field(default_factory=list)
    logs: list[LogRequest] = field(default_factory=list)
    balances: list[BalanceRequest] = field(default_factory=list)
    token_balances: list[TokenBalanceRequest] = field(default_factory=list)
    rewards: list[RewardRequest] = field(default_factory=list)

