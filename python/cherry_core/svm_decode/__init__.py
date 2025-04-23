from typing import List, Optional, Union, TypeAlias, Literal
from dataclasses import dataclass

PrimitiveType: TypeAlias = Literal[
    "i8", "i16", "i32", "i64", "i128", "u8", "u16", "u32", "u64", "u128", "bool"
]
ElementType: TypeAlias = Union[
    PrimitiveType, "DynType", "FixedArray", "Array", "Struct", "Enum", "Option"
]


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
    discriminator: Union[bytes, str]
    params: List[ParamInput]
    accounts_names: List[str]


@dataclass
class LogSignature:
    params: List[ParamInput]


class DynType:
    I8: PrimitiveType = "i8"
    I16: PrimitiveType = "i16"
    I32: PrimitiveType = "i32"
    I64: PrimitiveType = "i64"
    I128: PrimitiveType = "i128"
    U8: PrimitiveType = "u8"
    U16: PrimitiveType = "u16"
    U32: PrimitiveType = "u32"
    U64: PrimitiveType = "u64"
    U128: PrimitiveType = "u128"
    Bool: PrimitiveType = "bool"
    FixedArray = FixedArray
    Array = Array
    Struct = Struct
    Enum = Enum
    Option = Option
