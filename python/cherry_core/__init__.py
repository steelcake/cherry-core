import cherry_core.cherry_core as cc
from . import svm_decode
from typing import Tuple
import pyarrow


def cast(
    map: list[Tuple[str, pyarrow.DataType]],
    data: pyarrow.RecordBatch,
    allow_cast_fail: bool = False,
) -> pyarrow.RecordBatch:
    return cc.cast(map, data, allow_cast_fail)


def cast_schema(
    map: list[Tuple[str, pyarrow.DataType]], schema: pyarrow.Schema
) -> pyarrow.Schema:
    return cc.cast_schema(map, schema)


def cast_by_type(
    data: pyarrow.RecordBatch,
    from_type: pyarrow.DataType,
    to_type: pyarrow.DataType,
    allow_cast_fail: bool = False,
) -> pyarrow.RecordBatch:
    return cc.cast_by_type(data, from_type, to_type, allow_cast_fail)


def cast_schema_by_type(
    schema: pyarrow.Schema, from_type: pyarrow.DataType, to_type: pyarrow.DataType
) -> pyarrow.Schema:
    return cc.cast_schema_by_type(schema, from_type, to_type)


def base58_encode(data: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    return cc.base58_encode(data)


def hex_encode(data: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    return cc.hex_encode(data)


def prefix_hex_encode(data: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    return cc.prefix_hex_encode(data)


def base58_encode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.base58_encode_column(col)


def hex_encode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.hex_encode_column(col)


def prefix_hex_encode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.prefix_hex_encode_column(col)


def base58_decode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.base58_decode_column(col)


def hex_decode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.hex_decode_column(col)


def prefix_hex_decode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.prefix_hex_decode_column(col)


def u256_column_from_binary(col: pyarrow.Array) -> pyarrow.Array:
    return cc.u256_column_from_binary(col)


def u256_column_to_binary(col: pyarrow.Array) -> pyarrow.Array:
    return cc.u256_column_to_binary(col)


def u256_to_binary(data: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    return cc.u256_to_binary(data)


def svm_decode_instructions(
    signature: svm_decode.InstructionSignature,
    batch: pyarrow.RecordBatch,
    allow_decode_fail: bool = False,
) -> pyarrow.RecordBatch:
    return cc.svm_decode_instructions(signature, batch, allow_decode_fail)


def svm_decode_logs(
    signature: svm_decode.LogSignature,
    batch: pyarrow.RecordBatch,
    allow_decode_fail: bool = False,
) -> pyarrow.RecordBatch:
    return cc.svm_decode_logs(signature, batch, allow_decode_fail)


def instruction_signature_to_arrow_schema(
    signature: svm_decode.InstructionSignature,
) -> pyarrow.Schema:
    return cc.instruction_signature_to_arrow_schema(signature)


def evm_decode_call_inputs(
    signature: str, data: pyarrow.Array, allow_decode_fail: bool = False
) -> pyarrow.RecordBatch:
    return cc.evm_decode_call_inputs(signature, data, allow_decode_fail)


def evm_decode_call_outputs(
    signature: str, data: pyarrow.Array, allow_decode_fail: bool = False
) -> pyarrow.RecordBatch:
    return cc.evm_decode_call_outputs(signature, data, allow_decode_fail)


def evm_decode_events(
    signature: str, data: pyarrow.RecordBatch, allow_decode_fail: bool = False
) -> pyarrow.RecordBatch:
    return cc.evm_decode_events(signature, data, allow_decode_fail)


def evm_event_signature_to_arrow_schema(signature: str) -> pyarrow.Schema:
    return cc.evm_event_signature_to_arrow_schema(signature)


def evm_transaction_signature_to_arrow_schemas(
    signature: str,
) -> Tuple[pyarrow.Schema, pyarrow.Schema]:
    return cc.evm_transaction_signature_to_arrow_schemas(signature)


def evm_validate_block_data(
    blocks: pyarrow.RecordBatch,
    transactions: pyarrow.RecordBatch,
    logs: pyarrow.RecordBatch,
    traces: pyarrow.RecordBatch,
):
    return cc.evm_validate_block_data(blocks, transactions, logs, traces)


def evm_signature_to_topic0(signature: str) -> str:
    return cc.evm_signature_to_topic0(signature)


def base58_encode_bytes(b: bytes) -> str:
    return cc.base58_encode_bytes(b)


def base58_decode_string(s: str) -> bytes:
    return cc.base58_decode_string(s)


def get_token_metadata(rpc_url: str, addresses: list[str]) -> list[dict]:
    return cc.get_token_metadata(rpc_url, addresses)


def get_token_metadata_as_table(rpc_url: str, addresses: list[str]) -> pyarrow.Table:
    return cc.get_token_metadata_as_table(rpc_url, addresses)
