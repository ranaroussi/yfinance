from typing import Any, Union, Literal, Optional, overload, TypeVar

from yfinance.utils import pop

T = TypeVar('T')

OPERATORS = ["OR", "AND", "EQ", "BTWN", "GT", "LT", "GTE", "LTE"]
_OPERATORS = Literal["OR", "AND", "EQ", "BTWN", "GT", "LT", "GTE", "LTE"]

class Query:
    """
    A class representing a query structure for the Yahoo Finance screener.
    
    This class manages a collection of validators that form a complete screener query.
    
    Attributes:
        children (list[Validator]): List of validator objects that make up the query
    """
    
    def __init__(self, child: 'Validator'):
        self.validator:'Validator' = child
        
    def set_property(self, property: 'str', value: 'Any'):
        """
        Sets a property value for all child validators.
        
        Args:
            property (str): The property name to set
            value (Any): The value to set for the property
        """
        raise NotImplemented
            
    def to_dict(self) -> 'dict':
        """
        Converts the query to a dictionary format.
        
        Returns:
            dict: A dictionary representation of the query with AND operator and operands
        """
        return self.validator.to_dict()
        
    @classmethod
    def from_dict(cls, data: 'dict') -> 'Query':
        """
        Create a new Query instance from a dictionary representation.
        
        Args:
            data (dict): The dictionary representation of the query
            
        Returns:
            Query: A new Query instance with the specified parameters
            
        Raises:
            Exception: If the dictionary does not contain a valid query
        """
      
        child = Validator.from_dict(data)        
        return Query(child)
        
            

        

class Validator:
    """
    A class that validates and processes screener query conditions.
    
    This class handles the validation and processing of individual query conditions,
    supporting various operators and comparison types.
    
    Attributes:
        _operator (Optional[_OPERATORS]): The comparison operator
        _value (Any): The value to compare against
        _primary (Any): The primary key or field to check
    """
    
    def __init__(self, operator: 'Optional[_OPERATORS]'=None, primary:'Any'=None, operands: 'Union[list[Union[Any, Query]], Union[Any, Query]]'=None):
        """
        Initialize a new Validator instance.
        
        Args:
            operator (Optional[_OPERATORS]): The comparison operator to use
            primary (Any, optional): The primary key or field to check
            value (Union[Any, Query], optional): The value to compare against
            
        Raises:
            ValueError: If an invalid operator is provided
        """
        operands = [operands] if not isinstance(operands, list) else operands

        if operator == None:
            pass
        elif operator.upper() not in OPERATORS:
            raise ValueError(f"Invalid operator '{operator}'. Must be one of {OPERATORS}")
        else:
            operator = operator.upper() # type: ignore
        self._operator = operator
        self._operands = operands
        self._primary = primary

    @property
    def primary_key(self) -> 'Any':
        """The primary key or field being checked."""
        return self._primary
    
    @primary_key.setter
    def primary_key(self, value: 'Any'):
        """Set the primary key."""
        self._primary = value
    
    @property
    def other(self) -> 'Any':
        """The other operands."""
        return self._operands
    
    @other.setter
    def other(self, value: 'Any'):
        """Set the other operands. Usually use `other.append()`"""
        self._operands = value

    @property
    def operator(self) -> 'Union[_OPERATORS, None]':
        """The comparison operator."""
        return self._operator # type: ignore
    
    @operator.setter
    def operator(self, value: '_OPERATORS'):
        """Set the comparison operator."""
        self._operator = value
    
    @property
    def operands(self) -> 'list[Union[Any, Query]]':
        """List of operands for the comparison."""
        return [self.primary_key, *self.other]
    
    @operands.setter
    def operands(self, value: 'list[Union[Any, Query]]'):
        """
        Raises:
            Exception: Always raises an exception as operands cannot be set directly
        """
        raise Exception("Cannot set operands directly: set primary_key or value instead")

    def to_dict(self) -> 'dict':
        """
        Convert the validator to a dictionary format.
        
        Returns:
            dict: Dictionary representation of the validator
            
        Raises:
            Exception: If any required fields are missing
        """
        if None in self.operands:
            raise Exception("Can not use to_dict() on a base validator, use empty string for single operand")
        
        def validate(operand):
            if isinstance(operand, Validator):
                print("VALIDATOR: ", operand)
                return operand.to_dict()
            else:
                print("OTHER: ", operand)
                return operand
            

        print("OPERANDS: ", self.operands)
        return {
            "operator": self.operator,
            "operands": [validate(operand) for operand in self.operands if operand != ""]
        }
    
    @classmethod
    def from_dict(cls, data: 'Union[dict, T]', allow: 'bool'=True, raise_errors: 'bool'=True) -> 'Union[Validator, T]':
        """
        Create a new Validator instance from a dictionary representation.
        
        Args:
            data (dict): The dictionary representation of the validator
            allow (bool, optional): Whether to allow the non dictionary representation of the validator: allows for return of int or str. Defaults to True.
            raise_errors (bool, optional): Whether to raise errors (excludes errors handled by allow=True). Defaults to True.

        Returns:
            Validator: A new Validator instance with the specified parameters
            
        Raises:
            Exception: If the dictionary does not contain a valid validator
        """
        try:
            if isinstance(data, dict):
                if not list(data.keys()) == ["operator", "operands"]:
                    raise Exception("Invalid validator: must contain 'operator' and 'operands' keys")
                return Validator(
                    operator=data["operator"],
                    primary=Validator.from_dict(pop(data["operands"], 0, ""), raise_errors=False),
                    operands=[Validator.from_dict(value, raise_errors=False) for value in data["operands"]] # Entire list is used because primary is removed above
                    )
            else:
                if allow == False:
                    raise Exception("Invalid validator: must be a dictionary when allow=False")
                
                return data
        except Exception as e:
            if raise_errors:
                raise e
            return ""
    
    @overload
    def __call__(self, *, operator:'_OPERATORS'=None, value: 'Union[Any, Query]'=None) -> 'Validator': ...

    @overload
    def __call__(self, *, value: 'Union[Any, Query]'=None) -> 'Validator': ...

    @overload
    def __call__(self, *, primary:'Any'=None, value: 'Union[Any, Query]'=None) -> 'Validator': ...

    @overload
    def __call__(self, *, primary:'Any'=None): ...

    def __call__(self, *, operator: 'Optional[_OPERATORS]'=None, primary:'Any'=None, operands: 'Union[list[Union[Any, Query]], Union[Any, Query]]'=None) -> 'Validator':
        """
        Create a new Validator with the provided arguments.

        Parameters
        ----------
        operator : _OPERATORS, optional
            The operator to use for comparison. Only valid when value is provided.
        primary : Any, optional
            The primary key to check against. Only valid when value is provided.
        value : Union[Any, Query], optional
            The value to compare against.

        Returns
        -------
        Validator
            A new Validator instance with the specified parameters.

        Raises
        ------
        ValueError
            If invalid combination of arguments is provided.

        Examples
        --------
        # Method 1: Operator and value
        EPSGrowth(operator='GT', value=100)

        # Method 2: Primary and value 
        EQ(primary='price', value=50)

        # Method 3: Value only
        Market(value='NASDAQ')
        """
        operands = [operands] if not isinstance(operands, list) else operands
        if operator is not None and operands is not None:
            # Operator and value provided
            # Method 1: Return a new Validator for checking the primary (`EPSGrowth` etc was called)
            return Validator(
                operator=operator,
                primary=self.primary_key,
                operands=operands
            )
        elif operands is not None:
            # operands provided
            # Method 2: Return a new Validator for checking primary key exactly (`Market` etc was called)
            return Validator(
                operator=self.operator,
                primary=self.primary_key,
                operands=operands
                )
        elif primary is not None and operands is not None:
            # Primary and operands provided
            # Method 3: Return a new Validator for the operand (`EQ` etc was called)
            return Validator(
                operator=self.operator,
                primary=primary,
                operands=operands
                )
        elif primary is not None and self.operator == "EQ":
            # Primary provided
            # Method 4: Return a new Validator for use in Method 2
            # For use internally only
            return Validator(
                operator="EQ",
                primary=primary,
                operands=None
            )
        elif primary is not None and self.operator is None:
            # Primary provided
            # Method 5: Return a new Validator for use in Method 1
            # For use internally only
            return Validator(
                operator=None,
                primary=primary,
                operands=None
            )
        else:
            # Invalid arguments provided
            # Raise error
            raise operandsError(
                f"Invalid arguments provided: {primary=}, {operands=} and {operator=}\n"
                "Valid arguments are:\n"
                "  - operator and operands: Return a new Validator for checking the primary key (`EPSGrowth` etc)\n"
                "  - primary and operands: Return a new Validator for the operand (`EQ`, `AND` etc)\n"
                "  - operands: Return a new Validator for checking primary key exactly (`Market` etc)\n"
                )


    def check(self, value:'Any', this:'bool' = False) -> 'bool':
        """
        Check if a value matches the validation criteria.
        
        Args:
            value (Any): The value to check
            this (bool, optional): If True, only check exact primary key match for this Validator. If false, will check all child validators. Defaults to False.
            
        Returns:
            bool: True if the value matches the criteria, False otherwise
        """
        if this:
            return self.primary_key == value
        
        if self.primary_key == value:
            return True
        
        for operand in self.operands:
            if isinstance(operand, Validator):
                if operand.check(value):
                    return True

        return False

    def set(self, key: 'str', value: 'Any') -> 'Validator':
        """
        Set a property value for this validator and its operands.
        
        Args:
            key (str): The property key to set
            value (Any): The value to set for the property
            
        Returns:
            Validator: The validator instance for method chaining
        """
        if key == self.primary_key:
            self._operands = [value]
        else:
            for operand in self.operands:
                if isinstance(operand, Validator):
                    operand.set(key, value)

# Common query operators
AND = Validator("AND")  # Logical AND operator
OR = Validator("OR")    # Logical OR operator
EQ = Validator("EQ")    # Equals operator
BTWN = Validator("BTWN")  # Between operator
GT = Validator("GT")    # Greater than operator
LT = Validator("LT")    # Less than operator
GTE = Validator("GTE")  # Greater than or equal operator
LTE = Validator("LTE")  # Less than or equal operator

# Common field validators
Market = EQ(primary="exchange")         # Market/Exchange filter
Region = EQ(primary="region")           # Region filter
Category = EQ(primary="categoryname")   # Category filter
Sector = EQ(primary="sector")          # Sector filter
Industry = EQ(primary="industry")       # Industry filter
Exchange = EQ(primary="exchange")       # Exchange filter
PeerGroup = EQ(primary="peer_group")    # Peer group filter

_Base = Validator(None)  # Base validator for creating custom filters

# Common financial metrics validators
QuarterlyRevenueGrowth = _Base(primary="quarterlyrevenuegrowth.quarterly")
EpsGrowth = _Base(primary="epsgrowth.lasttwelvemonths")
IntradayMarketCap = _Base(primary="intradaymarketcap")
IntradayPrice = _Base(primary="intradayprice")
DayVolume = _Base(primary="dayvolume")
PercentChange = _Base(primary="percentchange")
PeRatio = _Base(primary="peratio.lasttwelvemonths")
PegRatio = _Base(primary="pegratio_5y")
InitialInvestment = _Base(primary="initialinvestment")
PerformanceRating = _Base(primary="performanceratingoverall")
RiskRating = _Base(primary="riskratingoverall")
AnnualReturnRank = _Base(primary="annualreturnnavy1categoryrank")
FundNetAssets = _Base(primary="fundnetassets")
