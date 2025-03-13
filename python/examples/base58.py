from cherry_core import base58_encode_bytes, base58_decode_string

b = b"asdlmsa;dm1123213213:SDMA"

s = base58_encode_bytes(b)

print(s)

b2 = base58_decode_string(s)

print(b)
print(b2)

print(b == b2)
