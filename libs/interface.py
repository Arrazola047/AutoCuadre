from enum import Enum
from dataclasses import dataclass
from typing import Optional, Any, List, TypeVar, Type, Callable, cast


T = TypeVar("T")
EnumT = TypeVar("EnumT", bound=Enum)


def from_int(x: Any) -> int:
    assert isinstance(x, int) and not isinstance(x, bool)
    return x


def from_str(x: Any) -> str:
    assert isinstance(x, str)
    return x


def from_bool(x: Any) -> bool:
    assert isinstance(x, bool)
    return x


def from_none(x: Any) -> Any:
    assert x is None
    return x


def from_union(fs, x):
    for f in fs:
        try:
            return f(x)
        except:
            pass
    assert False


def to_enum(c: Type[EnumT], x: Any) -> EnumT:
    assert isinstance(x, c)
    return x.value


def from_list(f: Callable[[Any], T], x: Any) -> List[T]:
    assert isinstance(x, list)
    return [f(y) for y in x]


def to_class(c: Type[T], x: Any) -> dict:
    assert isinstance(x, c)
    return cast(Any, x).to_dict()


class CalculationType(Enum):
    MATHEMATICAL = "Mathematical"
    SORTING = "Sorting"
    TIME_SHIFT = "TimeShift"


class SubType(Enum):
    NONE = "None"


class TypeEnum(Enum):
    BLOCK = "Block"
    CALCULATION = "Calculation"
    DATA_STORE = "DataStore"
    TABLE = "Table"
    WEB_FORM = "WebForm"
    WEB_REPORT = "WebReport"


@dataclass
class ContactResponseElement:
    id: int
    parent_block_id: int
    name: str
    type: TypeEnum
    sub_type: SubType
    calculation_type: CalculationType
    can_view: bool
    details: str
    block_id: Optional[int] = None

    @staticmethod
    def from_dict(obj: Any) -> 'ContactResponseElement':
        assert isinstance(obj, dict)
        id = from_int(obj.get("id"))
        parent_block_id = from_int(obj.get("parentBlockId"))
        name = from_str(obj.get("name"))
        type = TypeEnum(obj.get("type"))
        sub_type = SubType(obj.get("subType"))
        calculation_type = CalculationType(obj.get("calculationType"))
        can_view = from_bool(obj.get("canView"))
        details = from_str(obj.get("details"))
        block_id = from_union([from_int, from_none], obj.get("blockId"))
        return ContactResponseElement(id, parent_block_id, name, type, sub_type, calculation_type, can_view, details, block_id)

    def to_dict(self) -> dict:
        result: dict = {}
        result["id"] = from_int(self.id)
        result["parentBlockId"] = from_int(self.parent_block_id)
        result["name"] = from_str(self.name)
        result["type"] = to_enum(TypeEnum, self.type)
        result["subType"] = to_enum(SubType, self.sub_type)
        result["calculationType"] = to_enum(CalculationType, self.calculation_type)
        result["canView"] = from_bool(self.can_view)
        result["details"] = from_str(self.details)
        if self.block_id is not None:
            result["blockId"] = from_union([from_int, from_none], self.block_id)
        return result


def contact_response_from_dict(s: Any) -> List[ContactResponseElement]:
    return from_list(ContactResponseElement.from_dict, s)


def contact_response_to_dict(x: List[ContactResponseElement]) -> Any:
    return from_list(lambda x: to_class(ContactResponseElement, x), x)
