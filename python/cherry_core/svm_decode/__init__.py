from typing import List, Optional, Union
from dataclasses import dataclass
from enum import Enum

@dataclass
class FixedArray:
    element_type: Union['DynType', 'FixedArray', 'Array', 'Struct', 'Enum', 'Option']
    size: int

    def __str__(self):
        return "FixedArray"

@dataclass
class Array:
    element_type: Union['DynType', 'FixedArray', 'Array', 'Struct', 'Enum', 'Option']

    def __str__(self):
        return "Array"

@dataclass
class Field:
    name: str
    element_type: Union['DynType', 'FixedArray', 'Array', 'Struct', 'Enum', 'Option']

@dataclass
class Struct:
    fields: List[Field]

    def __str__(self):
        return "Struct"

@dataclass
class Variant:
    name: str
    element_type: Optional[Union['DynType', 'FixedArray', 'Array', 'Struct', 'Enum', 'Option']]

@dataclass
class Enum:
    variants: List[Variant]

    def __str__(self):
        return "Enum"

@dataclass
class Option:
    element_type: Union['DynType', 'FixedArray', 'Array', 'Struct', 'Enum', 'Option']
   
@dataclass
class ParamInput:
    name: str
    param_type: Union['DynType', 'FixedArray', 'Array', 'Struct', 'Enum', 'Option']

@dataclass
class InstructionSignature:
    discriminator: bytes
    params: List[ParamInput]
    accounts_names: List[str]

class DynType:
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
    FixedArray = FixedArray
    Array = Array
    Struct = Struct
    Enum = Enum
    Option = Option