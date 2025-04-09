from typing import List, Optional, Union, TypeAlias
from dataclasses import dataclass
from enum import Enum

ElementType: TypeAlias = Union['DynType', 'FixedArray', 'Array', 'Struct', 'Enum', 'Option']

@dataclass
class FixedArray:
    element_type: ElementType
    size: int

@dataclass
class Array:
    element_type: ElementType

@dataclass
class Field:
    name: str
    element_type: ElementType

@dataclass
class Struct:
    fields: List[Field]

@dataclass
class Variant:
    name: str
    element_type: Optional[ElementType]

@dataclass
class Enum:
    variants: List[Variant]

@dataclass
class Option:
    element_type: ElementType
   
@dataclass
class ParamInput:
    name: str
    param_type: ElementType

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