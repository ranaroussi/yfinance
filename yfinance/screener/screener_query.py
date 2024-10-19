import numbers
from typing import List, Union, Dict, Set

from yfinance.const import EQUITY_SCREENER_EQ_MAP, EQUITY_SCREENER_FIELDS
from yfinance.exceptions import YFNotImplementedError

class Query:
    def __init__(self, operator: str, operand: Union[numbers.Real, str, List['Query']]):
        self.operator = operator
        self.operands = operand
    
    def to_dict(self) -> Dict:
        raise YFNotImplementedError('to_dict() needs to be implemented by children classes')

class EquityQuery(Query):
    def __init__(self, operator: str, operand: Union[numbers.Real, str, List['EquityQuery']]):
        operator = operator.upper()

        if not isinstance(operand, list):
            raise TypeError('Invalid operand type')
        if len(operand) <= 0:
            raise ValueError('Invalid field for Screener')
            
        if operator in {'OR','AND'}: 
            self._validate_or_and_operand(operand)
        elif operator == 'EQ': 
            self._validate_eq_operand(operand)
        elif operator == 'BTWN': 
            self._validate_btwn_operand(operand)
        elif operator in {'GT','LT'}: 
            self._validate_gt_lt(operand)
        else: 
            raise ValueError('Invalid Operator Value')

        self.operator = operator
        self.operands = operand
        self._valid_eq_map = EQUITY_SCREENER_EQ_MAP
        self._valid_fields = EQUITY_SCREENER_FIELDS
        
    @property
    def valid_eq_map(self) -> Dict:
        return self._valid_eq_map

    @property
    def valid_fields(self) -> Set:
        return self._valid_fields
    
    def _validate_or_and_operand(self, operand: List['EquityQuery']) -> None:
        if len(operand) <= 1: 
            raise ValueError('Operand must be length longer than 1')
        if all(isinstance(e, EquityQuery) for e in operand) is False: 
            raise TypeError('Operand must be type EquityQuery for OR/AND')

    def _validate_eq_operand(self, operand: List[Union[str, numbers.Real]]) -> None:
        if len(operand) != 2:
            raise ValueError('Operand must be length 2 for EQ')
        if operand[0] not in EQUITY_SCREENER_FIELDS:
            raise ValueError('Invalid field for Screener')
        if operand[0] not in EQUITY_SCREENER_EQ_MAP:
            raise ValueError('Invalid EQ key')
        if operand[1] not in EQUITY_SCREENER_EQ_MAP[operand[0]]:
            raise ValueError('Invalid EQ value')
    
    def _validate_btwn_operand(self, operand: List[Union[str, numbers.Real]]) -> None:
        if len(operand) != 3: 
            raise ValueError('Operand must be length 3 for BTWN')
        if operand[0] not in EQUITY_SCREENER_FIELDS:
            raise ValueError('Invalid field for Screener')
        if isinstance(operand[1], numbers.Real) is False:
            raise TypeError('Invalid comparison type for BTWN')
        if isinstance(operand[2], numbers.Real) is False:
            raise TypeError('Invalid comparison type for BTWN')

    def _validate_gt_lt(self, operand: List[Union[str, numbers.Real]]) -> None:
        if len(operand) != 2:
            raise ValueError('Operand must be length 2 for GT/LT')
        if operand[0] not in EQUITY_SCREENER_FIELDS:
            raise ValueError('Invalid field for Screener')
        if isinstance(operand[1], numbers.Real) is False:
            raise TypeError('Invalid comparison type for GT/LT')

    def to_dict(self) -> Dict:
        return {
            "operator": self.operator,
            "operands": [operand.to_dict() if isinstance(operand, EquityQuery) else operand for operand in self.operands]
        }