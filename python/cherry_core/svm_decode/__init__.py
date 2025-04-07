from typing import List
from dataclasses import dataclass
from enum import Enum

class DynType(str, Enum):
    I8 = "i8"
    I16 = "i16"
    I32 = "i32"
    I64 = "i64"
    I128 = "i128"
    U8 = "u8"
    U16 = "u16"
    U32 = "u32"
    U64 = "u64"
    U128 = "u128"
    Bool = "bool"
    FixedArray = "FixedArray"
    Array = "Array"
    Struct = "Struct"
    Enum = "Enum"
    Option = "Option"

@dataclass
class ParamInput:
    name: str
    param_type: str

@dataclass
class InstructionSignature:
    discriminator: bytes
    params: List[ParamInput]
    accounts_names: List[str]
