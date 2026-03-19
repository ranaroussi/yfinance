from abc import ABC, abstractmethod
import numbers
from typing import Collection, Dict, List, Mapping, Sequence, TypeVar, Union

from yfinance.const import EQUITY_SCREENER_EQ_MAP, EQUITY_SCREENER_FIELDS
from yfinance.const import FUND_SCREENER_EQ_MAP, FUND_SCREENER_FIELDS
from yfinance.exceptions import YFNotImplementedError
from ..utils import dynamic_docstring, generate_list_table_from_dict_universal

OperandValue = Union[str, int, float]
OperandItem = Union['QueryBase', OperandValue]
ValidValueGroup = Union[Collection[OperandValue], Mapping[str, Collection[OperandValue]]]
T = TypeVar('T', bound=OperandValue)

class QueryBase(ABC):
    def __init__(self, operator: str, operand: List[OperandItem]):
        operator = operator.upper()

        if not isinstance(operand, list):
            raise TypeError('Invalid operand type')
        if len(operand) <= 0:
            raise ValueError('Invalid field for EquityQuery')
            
        if operator == 'IS-IN':
            self._validate_isin_operand(operand)
        elif operator in {'OR','AND'}: 
            self._validate_or_and_operand(operand)
        elif operator == 'EQ': 
            self._validate_eq_operand(operand)
        elif operator == 'BTWN': 
            self._validate_btwn_operand(operand)
        elif operator in {'GT','LT','GTE','LTE'}: 
            self._validate_gt_lt(operand)
        else: 
            raise ValueError('Invalid Operator Value')

        self.operator = operator
        self.operands = operand

    @property
    @abstractmethod
    def valid_fields(self) -> Dict[str, set[str]]:
        raise YFNotImplementedError('valid_fields() needs to be implemented by child')

    @property
    @abstractmethod
    def valid_values(self) -> Mapping[str, ValidValueGroup]:
        raise YFNotImplementedError('valid_values() needs to be implemented by child')

    @staticmethod
    def _validate_field(field: OperandItem) -> str:
        if not isinstance(field, str):
            raise TypeError('Field name must be str')
        return field

    @staticmethod
    def _flatten_valid_values(vv: ValidValueGroup) -> set[OperandValue]:
        if isinstance(vv, Mapping):
            flattened: set[OperandValue] = set()
            for values in vv.values():
                flattened.update(values)
            return flattened
        return set(vv)

    def _validate_or_and_operand(self, operand: Sequence[OperandItem]) -> None:
        if len(operand) <= 1: 
            raise ValueError('Operand must be length longer than 1')
        if all(isinstance(e, QueryBase) for e in operand) is False: 
            raise TypeError(f'Operand must be type {type(self)} for OR/AND')

    def _validate_eq_operand(self, operand: Sequence[OperandItem]) -> None:
        if len(operand) != 2:
            raise ValueError('Operand must be length 2 for EQ')

        field = self._validate_field(operand[0])
        value = operand[1]

        if not any(field in fields_by_type for fields_by_type in self.valid_fields.values()):
            raise ValueError(f'Invalid field for {type(self)} "{field}"')
        if field in self.valid_values:
            vv = self._flatten_valid_values(self.valid_values[field])
            if not isinstance(value, (str, numbers.Real)) or value not in vv:
                raise ValueError(f'Invalid EQ value "{value}"')

    def _validate_btwn_operand(self, operand: Sequence[OperandItem]) -> None:
        if len(operand) != 3: 
            raise ValueError('Operand must be length 3 for BTWN')
        field = self._validate_field(operand[0])
        if not any(field in fields_by_type for fields_by_type in self.valid_fields.values()):
            raise ValueError(f'Invalid field for {type(self)}')
        if isinstance(operand[1], numbers.Real) is False:
            raise TypeError('Invalid comparison type for BTWN')
        if isinstance(operand[2], numbers.Real) is False:
            raise TypeError('Invalid comparison type for BTWN')

    def _validate_gt_lt(self, operand: Sequence[OperandItem]) -> None:
        if len(operand) != 2:
            raise ValueError('Operand must be length 2 for GT/LT')
        field = self._validate_field(operand[0])
        if not any(field in fields_by_type for fields_by_type in self.valid_fields.values()):
            raise ValueError(f'Invalid field for {type(self)} "{field}"')
        if isinstance(operand[1], numbers.Real) is False:
            raise TypeError('Invalid comparison type for GT/LT')

    def _validate_isin_operand(self, operand: Sequence[OperandItem]) -> None:
        if len(operand) < 2:
            raise ValueError('Operand must be length 2+ for IS-IN')

        field = self._validate_field(operand[0])

        if not any(field in fields_by_type for fields_by_type in self.valid_fields.values()):
            raise ValueError(f'Invalid field for {type(self)} "{field}"')
        if field in self.valid_values:
            vv = self._flatten_valid_values(self.valid_values[field])
            for i in range(1, len(operand)):
                value = operand[i]
                if not isinstance(value, (str, numbers.Real)) or value not in vv:
                    raise ValueError(f'Invalid EQ value "{operand[i]}"')

    def to_dict(self) -> Dict:
        op = self.operator
        ops = self.operands
        if self.operator == 'IS-IN':
            # Expand to OR of EQ queries
            op = 'OR'
            ops = [type(self)('EQ', [self.operands[0], v]) for v in self.operands[1:]]
        return {
            "operator": op,
            "operands": [o.to_dict() if isinstance(o, QueryBase) else o for o in ops]
        }

    def __repr__(self, indent=0) -> str:
        indent_str = "  " * indent
        class_name = self.__class__.__name__

        if isinstance(self.operands, list):
            # For list operands, check if they contain any QueryBase objects
            if any(isinstance(op, QueryBase) for op in self.operands):
                # If there are nested queries, format them with newlines
                operands_str = ",\n".join(
                    f"{indent_str}  {op.__repr__(indent + 1) if isinstance(op, QueryBase) else repr(op)}"
                    for op in self.operands
                )
                return f"{class_name}({self.operator}, [\n{operands_str}\n{indent_str}])"
            else:
                # For lists of simple types, keep them on one line
                return f"{class_name}({self.operator}, {repr(self.operands)})"
        else:
            # Handle single operand
            return f"{class_name}({self.operator}, {repr(self.operands)})"

    def __str__(self) -> str:
        return self.__repr__()


class EquityQuery(QueryBase):
    """
    The `EquityQuery` class constructs filters for stocks based on specific criteria such as region, sector, exchange, and peer group.

    Start with value operations: `EQ` (equals), `IS-IN` (is in), `BTWN` (between), `GT` (greater than), `LT` (less than), `GTE` (greater or equal), `LTE` (less or equal).

    Combine them with logical operations: `AND`, `OR`.

    Example:
        Predefined Yahoo query `aggressive_small_caps`:
        
        .. code-block:: python

            from yfinance import EquityQuery

            EquityQuery('and', [
                EquityQuery('is-in', ['exchange', 'NMS', 'NYQ']), 
                EquityQuery('lt', ["epsgrowth.lasttwelvemonths", 15])
            ])
    """

    @dynamic_docstring({"valid_operand_fields_table": generate_list_table_from_dict_universal(EQUITY_SCREENER_FIELDS)})
    @property
    def valid_fields(self) -> Dict[str, set[str]]:
        """
        Valid operands, grouped by category.
        {valid_operand_fields_table}
        """
        return EQUITY_SCREENER_FIELDS
    
    @dynamic_docstring({"valid_values_table": generate_list_table_from_dict_universal(EQUITY_SCREENER_EQ_MAP, concat_keys=['exchange', 'industry'])})
    @property
    def valid_values(self) -> Mapping[str, ValidValueGroup]:
        """
        Most operands take number values, but some have a restricted set of valid values.
        {valid_values_table}
        """
        return EQUITY_SCREENER_EQ_MAP


class FundQuery(QueryBase):
    """
    The `FundQuery` class constructs filters for mutual funds based on specific criteria such as region, sector, exchange, and peer group.

    Start with value operations: `EQ` (equals), `IS-IN` (is in), `BTWN` (between), `GT` (greater than), `LT` (less than), `GTE` (greater or equal), `LTE` (less or equal).

    Combine them with logical operations: `AND`, `OR`.

    Example:
        Predefined Yahoo query `solid_large_growth_funds`:
        
        .. code-block:: python

            from yfinance import FundQuery
            
            FundQuery('and', [
                FundQuery('eq', ['categoryname', 'Large Growth']), 
                FundQuery('is-in', ['performanceratingoverall', 4, 5]), 
                FundQuery('lt', ['initialinvestment', 100001]), 
                FundQuery('lt', ['annualreturnnavy1categoryrank', 50]), 
                FundQuery('eq', ['exchange', 'NAS'])
            ])
    """
    @dynamic_docstring({"valid_operand_fields_table": generate_list_table_from_dict_universal(FUND_SCREENER_FIELDS)})
    @property
    def valid_fields(self) -> Dict[str, set[str]]:
        """
        Valid operands, grouped by category.
        {valid_operand_fields_table}
        """
        return FUND_SCREENER_FIELDS
    
    @dynamic_docstring({"valid_values_table": generate_list_table_from_dict_universal(FUND_SCREENER_EQ_MAP)})
    @property
    def valid_values(self) -> Mapping[str, ValidValueGroup]:
        """
        Most operands take number values, but some have a restricted set of valid values.
        {valid_values_table}
        """
        return FUND_SCREENER_EQ_MAP
