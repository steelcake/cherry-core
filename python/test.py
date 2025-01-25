from cherry_core import cast, evm_decode_events, evm_event_signature_to_arrow_schema, encode_hex
import pyarrow

a = evm_event_signature_to_arrow_schema("Transfer(address indexed from, address indexed to, uint256 amount)")

print(a)
