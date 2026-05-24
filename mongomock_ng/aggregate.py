"""Module to handle the operations within the aggregate pipeline."""

import bisect
import calendar
import collections
import contextlib
import copy
import datetime
import decimal
import functools
import itertools
import math
import numbers
import random
import re
import statistics
import warnings
from typing import Any
from typing import ClassVar
from typing import Optional

import pytz
from packaging import version
from sentinels import NOTHING  # type: ignore[import-untyped]

import mongomock_ng

from . import command_cursor
from . import filtering
from . import helpers
from . import OperationFailure


# bson types - available only if bson is installed
Regex: Optional[type[Any]] = None
InvalidDocument: type[Exception] = OperationFailure
InvalidId: type[Exception] = OperationFailure
decimal_support: bool = False

try:
    from bson import decimal128
    from bson import Regex as _Regex
    from bson.errors import InvalidDocument as _InvalidDocument
    from bson.errors import InvalidId as _InvalidId

    Regex = _Regex  # type: ignore[misc]
    InvalidDocument = _InvalidDocument  # type: ignore[misc]
    InvalidId = _InvalidId  # type: ignore[misc]
    decimal_support = True
except ImportError:
    pass

_RE_TYPES: tuple[type[Any], ...] = (
    (helpers.RE_TYPE, Regex) if Regex is not None else (helpers.RE_TYPE,)
)  # type: ignore[assignment]

_random = random.Random()  # noqa: S311


def _is_nat(value: Any) -> bool:
    return type(value).__name__ == 'NaTType'


group_operators = [
    '$addToSet',
    '$avg',
    '$first',
    '$last',
    '$max',
    '$mergeObjects',
    '$min',
    '$push',
    '$stdDevPop',
    '$stdDevSamp',
    '$sum',
]
set_window_fields_operators = [
    '$addToSet',
    '$avg',
    '$count',
    '$covariancePop',
    '$covarianceSamp',
    '$derivative',
    '$expMovingAvg',
    '$integral',
    '$max',
    '$min',
    '$push',
    '$stdDevSamp',
    '$stdDevPop',
    '$sum',
    '$first',
    '$last',
    '$shift',
    '$denseRank',
    '$documentNumber',
    '$rank',
]
unary_arithmetic_operators = {
    '$abs',
    '$ceil',
    '$exp',
    '$floor',
    '$ln',
    '$log10',
    '$sqrt',
    '$trunc',
}
binary_arithmetic_operators_with_optional_second_number = {'$round'}
binary_arithmetic_operators = {
    '$divide',
    '$log',
    '$mod',
    '$pow',
    '$subtract',
} | binary_arithmetic_operators_with_optional_second_number
binary_bitwise_operators = {
    '$bitAnd',
    '$bitOr',
    '$bitXor',
}
unary_bitwise_operators = {
    '$bitNot',
}
binary_bitwise_operators = {
    '$bitAnd',
    '$bitOr',
    '$bitXor',
}
unary_bitwise_operators = {
    '$bitNot',
}
arithmetic_operators = (
    unary_arithmetic_operators
    | binary_arithmetic_operators
    | binary_bitwise_operators
    | unary_bitwise_operators
    | binary_bitwise_operators
    | unary_bitwise_operators
    | {
        '$add',
        '$multiply',
    }
)
project_operators = [
    '$max',
    '$min',
    '$avg',
    '$sum',
    '$stdDevPop',
    '$stdDevSamp',
    '$arrayElemAt',
    '$first',
    '$last',
]
control_flow_operators = [
    '$switch',
]
projection_operators = [
    '$let',
    '$literal',
]
date_operators = [
    '$dateAdd',
    '$dateDiff',
    '$dateFromString',
    '$dateSubtract',
    '$dateToString',
    '$dateTrunc',
    '$dateFromParts',
    '$dayOfMonth',
    '$dayOfWeek',
    '$dayOfYear',
    '$hour',
    '$isoDayOfWeek',
    '$isoWeek',
    '$isoWeekYear',
    '$millisecond',
    '$minute',
    '$month',
    '$second',
    '$week',
    '$year',
]
conditional_operators = ['$cond', '$ifNull']
array_operators = [
    '$concatArrays',
    '$filter',
    '$indexOfArray',
    '$map',
    '$range',
    '$reduce',
    '$reverseArray',
    '$size',
    '$slice',
    '$zip',
]
object_operators = [
    '$mergeObjects',
]
text_search_operators = ['$meta']
string_operators = [
    '$concat',
    '$indexOfBytes',
    '$indexOfCP',
    '$regexMatch',
    '$split',
    '$strcasecmp',
    '$strLenBytes',
    '$strLenCP',
    '$substr',
    '$substrBytes',
    '$substrCP',
    '$toLower',
    '$toUpper',
    '$ltrim',
    '$rtrim',
    '$trim',
]
comparison_operators = [
    '$cmp',
    '$eq',
    '$ne',
    *list(filtering.SORTING_OPERATOR_MAP.keys()),
]
boolean_operators = ['$and', '$or', '$not']
set_operators = [
    '$in',
    '$setEquals',
    '$setIntersection',
    '$setDifference',
    '$setUnion',
    '$setIsSubset',
    '$anyElementTrue',
    '$allElementsTrue',
]
field_operators = [
    '$getField',
]
custom_operators = [
    '$function',
]

type_convertion_operators = [
    '$convert',
    '$toString',
    '$toInt',
    '$toDecimal',
    '$toLong',
    '$toDouble',
    '$toObjectId',
    '$toDate',
    '$arrayToObject',
    '$objectToArray',
]
type_operators = ['$isNumber', '$isArray', '$type']


def _parse_iso_datetime_string(value: str) -> datetime.datetime:
    normalized = value.replace('Z', '+00:00') if value.endswith('Z') else value
    try:
        parsed = datetime.datetime.fromisoformat(normalized)
    except ValueError as err:
        raise OperationFailure(f"'{value}' is not a valid ISO date string") from err
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def _add_months(value: datetime.datetime, months: int) -> datetime.datetime:
    total_months = value.year * 12 + (value.month - 1) + months
    year = total_months // 12
    month = total_months % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _add_to_date(value: datetime.datetime, unit: str, amount: int) -> datetime.datetime:
    if unit == 'millisecond':
        return value + datetime.timedelta(milliseconds=amount)
    if unit == 'second':
        return value + datetime.timedelta(seconds=amount)
    if unit == 'minute':
        return value + datetime.timedelta(minutes=amount)
    if unit == 'hour':
        return value + datetime.timedelta(hours=amount)
    if unit == 'day':
        return value + datetime.timedelta(days=amount)
    if unit == 'week':
        return value + datetime.timedelta(weeks=amount)
    if unit == 'month':
        return _add_months(value, amount)
    if unit == 'quarter':
        return _add_months(value, amount * 3)
    if unit == 'year':
        return _add_months(value, amount * 12)
    raise OperationFailure(f'{unit} is not a valid value for the "unit" field')


def _truncate_date(value: datetime.datetime, unit: str) -> datetime.datetime:
    if unit == 'millisecond':
        return value.replace(microsecond=(value.microsecond // 1000) * 1000)
    if unit == 'second':
        return value.replace(microsecond=0)
    if unit == 'minute':
        return value.replace(second=0, microsecond=0)
    if unit == 'hour':
        return value.replace(minute=0, second=0, microsecond=0)
    if unit == 'day':
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == 'week':
        day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
        return day_start - datetime.timedelta(days=(value.weekday() + 1) % 7)
    if unit == 'month':
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == 'quarter':
        month = ((value.month - 1) // 3) * 3 + 1
        return value.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == 'year':
        return value.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise OperationFailure(f'{unit} is not a valid value for the "unit" field in $dateTrunc')


def _handle_date_add_operator(
    operator: str, values: Any, out_value: dict[str, Any]
) -> Optional[datetime.datetime]:
    if not isinstance(values, dict) or not {'startDate', 'amount', 'unit'} <= set(values):
        raise OperationFailure(
            f'{operator} operator must correspond a dict'
            'that has "startDate", "amount" and "unit" fields.'
        )
    if 'timezone' in values:
        raise NotImplementedError(
            f'Although timezone is a valid field for the '
            f'{operator} operator, it is currently not implemented '
            'in Mongomock-ng.'
        )

    if _is_nat(out_value['startDate']):
        return None

    amount = out_value.get('amount')
    if not isinstance(amount, int) or isinstance(amount, bool):
        raise OperationFailure(
            f'{out_value.get("amount")} is an invalid "amount" value. Must be an integer'
        )

    if operator == '$dateSubtract':
        amount = -amount

    return _add_to_date(out_value['startDate'], out_value['unit'], amount)


def _handle_date_diff_operator(values: Any, out_value: dict[str, Any]) -> Optional[int]:
    if not isinstance(values, dict) or not {'startDate', 'endDate', 'unit'} <= set(values):
        raise OperationFailure(
            '$dateDiff operator must correspond a dict'
            'that has "startDate", "endDate" and "unit" fields.'
        )
    if 'timezone' in values:
        raise NotImplementedError(
            'Although timezone is a valid field for the '
            '$dateDiff operator, it is currently not implemented '
            'in Mongomock-ng.'
        )
    if 'startOfWeek' in values:
        raise NotImplementedError(
            'Although startOfWeek is a valid field for the '
            '$dateDiff operator, it is currently not implemented '
            'in Mongomock-ng.'
        )

    start_date = out_value['startDate']
    end_date = out_value['endDate']

    if _is_nat(start_date) or _is_nat(end_date):
        return None

    unit = out_value['unit']
    delta = end_date - start_date
    if unit == 'millisecond':
        result = delta.total_seconds() * 1000
    elif unit == 'second':
        result = delta.total_seconds()
    elif unit == 'minute':
        result = delta.total_seconds() / 60
    elif unit == 'hour':
        result = delta.total_seconds() / (60 * 60)
    elif unit == 'day':
        result = delta.days
    elif unit == 'week':
        raise NotImplementedError(
            'Although {"unit": "week"} is a valid field for the '
            '$dateDiff operator, it is currently not implemented '
            'in Mongomock-ng.'
        )
    elif unit == 'month':
        result = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
    elif unit == 'quarter':
        result = ((end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)) / 3
    elif unit == 'year':
        result = end_date.year - start_date.year
    else:
        raise OperationFailure(f'{unit} is not a valid value for the "unit" field in $dateDiff')
    return math.floor(result)


def _handle_date_trunc_operator(
    values: Any, out_value: dict[str, Any]
) -> Optional[datetime.datetime]:
    if not isinstance(values, dict) or not {'date', 'unit'} <= set(values):
        raise OperationFailure(
            '$dateTrunc operator must correspond a dictthat has "unit" and "date" fields.'
        )
    unsupported_fields = {'binSize', 'startOfWeek', 'timezone'} & set(values)
    if unsupported_fields:
        unsupported_field = next(iter(sorted(unsupported_fields)))
        raise NotImplementedError(
            f'Although {unsupported_field} is a valid field for the '
            '$dateTrunc operator, it is currently not implemented '
            'in Mongomock-ng.'
        )

    if _is_nat(out_value['date']):
        return None
    return _truncate_date(out_value['date'], out_value['unit'])


def _handle_date_from_string_operator(values: Any, out_value: dict[str, Any]) -> Any:
    if not isinstance(values, dict) or 'dateString' not in values:
        raise OperationFailure(
            '$dateFromString operator must correspond a dictthat has "dateString" field.'
        )
    unsupported_fields = {'format', 'timezone'} & set(values)
    if unsupported_fields:
        unsupported_field = next(iter(sorted(unsupported_fields)))
        raise NotImplementedError(
            f'Although {unsupported_field} is a valid field for the '
            '$dateFromString operator, it is currently not implemented '
            'in Mongomock-ng.'
        )

    date_string = out_value.get('dateString')
    if date_string is None:
        return out_value.get('onNull')
    if not isinstance(date_string, str):
        raise OperationFailure('$dateFromString requires that dateString be a string')
    try:
        return _parse_iso_datetime_string(date_string)
    except OperationFailure:
        if 'onError' in out_value:
            return out_value['onError']
        raise


def _avg_operation(values):
    values_list = [v for v in values if isinstance(v, numbers.Number)]
    if not values_list:
        return None
    return sum(values_list) / float(len(list(values_list)))


def _group_operation(values, operator):
    values_list = [v for v in values if v is not None]
    if not values_list:
        return None
    return operator(values_list)


def _sum_operation(values):
    values_list = []
    if decimal_support:
        for v in values:
            if isinstance(v, numbers.Number):
                values_list.append(v)
            elif isinstance(v, decimal128.Decimal128):
                values_list.append(v.to_decimal())
    else:
        values_list = [v for v in values if isinstance(v, numbers.Number)]
    sum_value = sum(values_list)
    return decimal128.Decimal128(sum_value) if isinstance(sum_value, decimal.Decimal) else sum_value


def _parse_and_execute_trim(operator, values, parser):
    if isinstance(values, str):
        input_str = parser.parse(values)
        chars = None
    elif isinstance(values, dict):
        input_str = parser.parse(values.get('input', ''))
        chars = values.get('chars')
        if chars is not None:
            chars = parser.parse(chars)
    else:
        raise OperationFailure(f'${operator} expects a string or an object, got {type(values)}')
    if input_str is None:
        return None
    if not isinstance(input_str, str):
        raise OperationFailure(
            f'${operator} requires input to be of type string, got {type(input_str).__name__}'
            f'${operator} requires input to be of type string, got {type(input_str).__name__}'
        )
    if chars is not None and not isinstance(chars, str):
        raise OperationFailure(
            f'${operator} requires chars to be of type string, got {type(chars).__name__}'
            f'${operator} requires chars to be of type string, got {type(chars).__name__}'
        )
    strip_chars = chars if chars else None
    if operator == '$trim':
        return input_str.strip(strip_chars)
    if operator == '$ltrim':
        return input_str.lstrip(strip_chars)
    return input_str.rstrip(strip_chars)


def _std_dev_pop_operation(values):
    values_list = [v for v in values if isinstance(v, numbers.Number)]
    if not values_list:
        return None
    return statistics.pstdev(values_list)


def _std_dev_samp_operation(values):
    values_list = [v for v in values if isinstance(v, numbers.Number)]
    if len(values_list) < 2:
        return None
    return statistics.stdev(values_list)


def _merge_objects_operation(values):
    merged_doc = {}
    for v in values:
        if isinstance(v, dict):
            merged_doc.update(v)
    return merged_doc


_GROUPING_OPERATOR_MAP = {
    '$sum': _sum_operation,
    '$avg': _avg_operation,
    '$mergeObjects': _merge_objects_operation,
    '$min': lambda values: _group_operation(values, min),
    '$max': lambda values: _group_operation(values, max),
    '$first': lambda values: values[0] if values else None,
    '$last': lambda values: values[-1] if values else None,
    '$stdDevPop': _std_dev_pop_operation,
    '$stdDevSamp': _std_dev_samp_operation,
}


class _Parser:
    """Helper to parse expressions within the aggregate pipeline."""

    def __init__(self, doc_dict, user_vars=None, ignore_missing_keys=False):
        self._doc_dict = doc_dict
        self._ignore_missing_keys = ignore_missing_keys
        self._user_vars = user_vars or {}

    def parse(self, expression):
        """Parse a MongoDB expression."""
        if isinstance(expression, list):
            return list(self.parse_many(expression))
        if not isinstance(expression, dict):
            # May raise a KeyError despite the ignore missing key.
            return self._parse_basic_expression(expression)

        if len(expression) > 1 and any(key.startswith('$') for key in expression):
            raise OperationFailure(
                f'an expression specification must contain exactly one field, '
                f'the name of the expression. Found {len(expression)} fields in {expression}'
            )

        value_dict = {}
        for k, v in expression.items():
            if k in arithmetic_operators:
                return self._handle_arithmetic_operator(k, v)
            if k in project_operators:
                return self._handle_project_operator(k, v)
            if k in projection_operators:
                return self._handle_projection_operator(k, v)
            if k in comparison_operators:
                return self._handle_comparison_operator(k, v)
            if k in date_operators:
                return self._handle_date_operator(k, v)
            if k in array_operators:
                return self._handle_array_operator(k, v)
            if k in conditional_operators:
                return self._handle_conditional_operator(k, v)
            if k in control_flow_operators:
                return self._handle_control_flow_operator(k, v)
            if k in set_operators:
                return self._handle_set_operator(k, v)
            if k in string_operators:
                return self._handle_string_operator(k, v)
            if k in type_convertion_operators:
                return self._handle_type_convertion_operator(k, v)
            if k in type_operators:
                return self._handle_type_operator(k, v)
            if k in boolean_operators:
                return self._handle_boolean_operator(k, v)
            if k in object_operators:
                return self._handle_object_operator(k, v)
            if k in field_operators:
                return self._handle_field_operator(k, v)
            if k in custom_operators:
                return self._handle_custom_operator(k, v)
            if k in text_search_operators + projection_operators + object_operators:
                raise NotImplementedError(
                    f"'{k}' is a valid operation but it is not supported by Mongomock-ng yet."
                )
            if k.startswith('$'):
                raise OperationFailure(f"Unrecognized expression '{k}'")
            try:
                value = self.parse(v)
            except KeyError:
                if self._ignore_missing_keys:
                    continue
                raise
            value_dict[k] = value

        return value_dict

    def parse_many(self, values):
        for value in values:
            try:
                yield self.parse(value)
            except KeyError:
                if self._ignore_missing_keys:
                    yield None
                else:
                    raise

    def _parse_to_bool(self, expression):
        """Parse a MongoDB expression and then convert it to bool"""
        # handles converting `undefined` (in form of KeyError) to False
        try:
            return helpers.mongodb_to_bool(self.parse(expression))
        except KeyError:
            return False

    def _parse_or_nothing(self, expression):
        try:
            return self.parse(expression)
        except KeyError:
            return NOTHING

    def _parse_or_none(self, expression):
        value = self._parse_or_nothing(expression)
        if value is NOTHING:
            return None
        return value

    def _parse_array_or_nothing(self, expression):
        if isinstance(expression, (list, tuple)):
            return [self._parse_or_none(value) for value in expression]
        return self._parse_or_nothing(expression)

    def _parse_array_or_none(self, expression):
        value = self._parse_array_or_nothing(expression)
        if value is NOTHING:
            return None
        return value

    def _parse_basic_expression(self, expression):
        if isinstance(expression, str) and expression.startswith('$'):
            if expression.startswith('$$'):
                return helpers.get_value_by_dot(
                    dict(
                        {
                            'ROOT': self._doc_dict,
                            'CURRENT': self._doc_dict,
                        },
                        **self._user_vars,
                    ),
                    expression[2:],
                    can_generate_array=True,
                )
            return helpers.get_value_by_dot(self._doc_dict, expression[1:], can_generate_array=True)
        return expression

    def _handle_boolean_operator(self, operator, values):
        if operator == '$and':
            return all(self._parse_to_bool(value) for value in values)
        if operator == '$or':
            return any(self._parse_to_bool(value) for value in values)
        if operator == '$not':
            return not self._parse_to_bool(values)
        # This should never happen: it is only a safe fallback if something went wrong.
        raise NotImplementedError(  # pragma: no cover
            f"Although '{operator}' is a valid boolean operator for the "
            f'aggregation pipeline, it is currently not implemented'
            f' in Mongomock-ng.'
        )

    def _handle_arithmetic_operator(self, operator, values):
        try:
            return self._eval_arithmetic_operator(operator, values)
        except (OverflowError, ValueError) as e:
            raise OperationFailure(f'Error processing {operator}: {e}') from e

    def _eval_arithmetic_operator(self, operator, values):
        if operator in unary_arithmetic_operators | unary_bitwise_operators:
            return self._eval_unary_arithmetic_operator(operator, values)
        if operator in binary_arithmetic_operators | binary_bitwise_operators:
            return self._eval_binary_arithmetic_operator(operator, values)
        # N-ary operators
        assert isinstance(
            values, (tuple, list)
        ), f"Parameter to {operator} must evaluate to a list, got '{type(values)}'"

        parsed_values = list(self.parse_many(values))
        assert parsed_values, f'{operator} must have at least one parameter'
        for value in parsed_values:
            if value is None:
                return None
            assert isinstance(value, numbers.Number), f'{operator} only uses numbers'
        if operator == '$add':
            return sum(parsed_values)
        if operator == '$multiply':
            return functools.reduce(lambda x, y: x * y, parsed_values)

        raise NotImplementedError(  # pragma: no cover
            f"Although '{operator}' is a valid aritmetic operator for the aggregation "
            f'pipeline, it is currently not implemented  in Mongomock-ng.'
        )

    def _eval_unary_arithmetic_operator(self, operator, values):
        try:
            number = self.parse(values)
        except KeyError:
            return None
        if number is None:
            return None
        if not isinstance(number, numbers.Number):
            raise OperationFailure(
                f"Parameter to {operator} must evaluate to a number, got '{type(number)}'"
            )
        if operator == '$abs':
            return abs(number)
        if operator == '$ceil':
            return math.ceil(number)
        if operator == '$exp':
            return math.exp(number)
        if operator == '$floor':
            return math.floor(number)
        if operator == '$ln':
            return math.log(number)
        if operator == '$log10':
            return math.log10(number)
        if operator == '$sqrt':
            return math.sqrt(number)
        if operator == '$trunc':
            return math.trunc(number)
        if operator == '$bitNot':
            if not isinstance(number, int):
                raise OperationFailure(
                    f'Parameter to {operator} must evaluate to an integer, '
                    f"got '{type(number).__name__}'"
                )
            return ~number

    def _eval_binary_arithmetic_operator(self, operator, values):
        if not isinstance(values, (tuple, list)):
            raise OperationFailure(
                f"Parameter to {operator} must evaluate to a list, got '{type(values)}'"
            )

        supports_optional_number_2 = (
            operator in binary_arithmetic_operators_with_optional_second_number
        )
        if operator in binary_bitwise_operators:
            if len(values) != 2:
                raise OperationFailure(f'{operator} must have only 2 parameters')
        elif supports_optional_number_2:
            if len(values) not in [1, 2]:
                raise OperationFailure(f'{operator} must have 1 or 2 parameters')
        else:
            if len(values) != 2:
                raise OperationFailure(f'{operator} must have only 2 parameters')

        number_0, number_1, *_ = list(self.parse_many(values)) + [None] * 2
        if operator in binary_bitwise_operators:
            if number_0 is None or number_1 is None:
                return None
        elif number_0 is None or (number_1 is None and not supports_optional_number_2):
            return None

        if operator == '$divide':
            return number_0 / number_1
        if operator == '$log':
            return math.log(number_0, number_1)
        if operator == '$mod':
            try:
                return math.fmod(number_0, number_1)
            except OverflowError as e:
                raise OperationFailure(str(e)) from e
        if operator == '$pow':
            try:
                return math.pow(number_0, number_1)
            except OverflowError as e:
                raise OperationFailure(str(e)) from e
        if operator == '$round':
            return round(number_0, number_1)
        if operator == '$subtract':
            if isinstance(number_0, datetime.datetime) and isinstance(number_1, (int, float)):
                number_1 = datetime.timedelta(milliseconds=number_1)
            res = number_0 - number_1
            if isinstance(res, datetime.timedelta):
                return round(res.total_seconds() * 1000)
            return res
        if operator in binary_bitwise_operators:
            if not isinstance(number_0, int) or not isinstance(number_1, int):
                raise OperationFailure(
                    f'Parameter to {operator} must evaluate to an integer, '
                    f"got types '{type(number_0).__name__}' and '{type(number_1).__name__}'"
                )
            if operator == '$bitAnd':
                return number_0 & number_1
            if operator == '$bitOr':
                return number_0 | number_1
            if operator == '$bitXor':
                return number_0 ^ number_1

    def _handle_project_operator(self, operator, values):
        if operator in _GROUPING_OPERATOR_MAP:
            values = self.parse(values) if isinstance(values, str) else self.parse_many(values)
            return _GROUPING_OPERATOR_MAP[operator](values)
        if operator == '$arrayElemAt':
            key, value = values
            array = self.parse(key)
            index = self.parse(value)
            try:
                return array[index]
            except IndexError as error:
                raise KeyError('Array have length less than index value') from error

        raise NotImplementedError(
            f"Although '{operator}' is a valid project operator for the "
            f'aggregation pipeline, it is currently not implemented '
            f'in Mongomock-ng.'
        )

    def _handle_projection_operator(self, operator, value):
        if operator == '$literal':
            return value
        if operator == '$let':
            if not isinstance(value, dict):
                raise InvalidDocument('$let only supports an object as its argument')
            for field in ('vars', 'in'):
                if field not in value:
                    raise OperationFailure(f"Missing '{field}' parameter to $let")
            if not isinstance(value['vars'], dict):
                raise OperationFailure('invalid parameter: expected an object (vars)')
            user_vars = {
                var_key: self.parse(var_value) for var_key, var_value in value['vars'].items()
            }
            return _Parser(
                self._doc_dict,
                dict(self._user_vars, **user_vars),
            ).parse(value['in'])
        raise NotImplementedError(
            f"Although '{operator}' is a valid project operator for the "
            f'aggregation pipeline, it is currently not implemented '
            f'in Mongomock-ng.'
        )

    def _handle_comparison_operator(self, operator, values):
        assert len(values) == 2, 'Comparison requires two expressions'
        a = self.parse(values[0])
        b = self.parse(values[1])
        if operator == '$eq':
            return a == b
        if operator == '$ne':
            return a != b
        if operator in filtering.SORTING_OPERATOR_MAP:
            return filtering.bson_compare(filtering.SORTING_OPERATOR_MAP[operator], a, b)
        raise NotImplementedError(
            f"Although '{operator}' is a valid comparison operator for the "
            f'aggregation pipeline, it is currently not implemented '
            f' in Mongomock-ng.'
        )

    def _handle_string_operator(self, operator, values):
        if operator == '$toLower':
            parsed = self.parse(values)
            return str(parsed).lower() if parsed is not None else ''
        if operator == '$toUpper':
            parsed = self.parse(values)
            return str(parsed).upper() if parsed is not None else ''
        if operator == '$concat':
            parsed_list = list(self.parse_many(values))
            return None if None in parsed_list else ''.join([str(x) for x in parsed_list])
        if operator == '$split':
            if len(values) != 2:
                raise OperationFailure('split must have 2 items')
            try:
                string = self.parse(values[0])
                delimiter = self.parse(values[1])
            except KeyError:
                return None

            if string is None or delimiter is None:
                return None
            if not isinstance(string, str):
                raise TypeError('split first argument must evaluate to string')
            if not isinstance(delimiter, str):
                raise TypeError('split second argument must evaluate to string')
            return string.split(delimiter)
        if operator == '$substr':
            if len(values) != 3:
                raise OperationFailure('substr must have 3 items')
            string = str(self.parse(values[0]))
            first = self.parse(values[1])
            length = self.parse(values[2])
            if string is None:
                return ''
            if first < 0:
                warnings.warn(
                    'Negative starting point given to $substr is accepted only until '
                    'MongoDB 3.7. This behavior will change in the future.',
                    stacklevel=2,
                )
                return ''
            if length < 0:
                warnings.warn(
                    'Negative length given to $substr is accepted only until '
                    'MongoDB 3.7. This behavior will change in the future.',
                    stacklevel=2,
                )
            second = len(string) if length < 0 else first + length
            return string[first:second]
        if operator == '$strcasecmp':
            if len(values) != 2:
                raise OperationFailure('strcasecmp must have 2 items')
            a, b = str(self.parse(values[0])), str(self.parse(values[1]))
            return 0 if a == b else -1 if a < b else 1
        if operator == '$regexMatch':
            if not isinstance(values, dict):
                raise OperationFailure(
                    f'$regexMatch expects an object of named arguments but found: {type(values)}'
                )
            for field in ('input', 'regex'):
                if field not in values:
                    raise OperationFailure(f"$regexMatch requires '{field}' parameter")
            unknown_args = set(values) - {'input', 'regex', 'options'}
            if unknown_args:
                raise OperationFailure(
                    f'$regexMatch found an unknown argument: {next(iter(unknown_args))}'
                )

            try:
                input_value = self.parse(values['input'])
            except KeyError:
                return False
            if not isinstance(input_value, str):
                raise OperationFailure("$regexMatch needs 'input' to be of type string")

            try:
                regex_val = self.parse(values['regex'])
            except KeyError:
                return False
            options = None
            raw_options = values.get('options', '').lower()
            for option in raw_options:
                if option not in 'imxs':
                    raise OperationFailure(f'$regexMatch invalid flag in regex options: {option}')
                re_option = getattr(re, option.upper())
                if options is None:
                    options = re_option
                else:
                    options |= re_option
            if isinstance(regex_val, str):
                regex = re.compile(regex_val, options) if options else re.compile(regex_val)
            elif 'options' in values and regex_val.flags:
                raise OperationFailure(
                    "$regexMatch: regex option(s) specified in both 'regex' and 'option' fields"
                )
            elif isinstance(regex_val, helpers.RE_TYPE):
                if options and not regex_val.flags:
                    regex = re.compile(regex_val.pattern, options)
                elif regex_val.flags & ~(re.I | re.M | re.X | re.S | re.U):
                    raise OperationFailure(
                        f'$regexMatch invalid flag in regex options: {regex_val.flags}'
                    )
                else:
                    regex = regex_val
            elif isinstance(regex_val, _RE_TYPES):
                # bson.Regex
                if regex_val.flags & ~(re.I | re.M | re.X | re.S | re.U):
                    raise OperationFailure(
                        f'$regexMatch invalid flag in regex options: {regex_val.flags}'
                    )
                regex = re.compile(regex_val.pattern, regex_val.flags or options)
            else:
                raise OperationFailure("$regexMatch needs 'regex' to be of type string or regex")

            return bool(regex.search(input_value))

        if operator in ('$trim', '$ltrim', '$rtrim'):
            return _parse_and_execute_trim(operator, values, self)

        # This should never happen: it is only a safe fallback if something went wrong.
        raise NotImplementedError(  # pragma: no cover
            f"Although '{operator}' is a valid string operator for the aggregation "
            f'pipeline, it is currently not implemented  in Mongomock-ng.'
        )

    def _handle_date_operator(self, operator, values):
        if isinstance(values, dict) and values.keys() == {'date', 'timezone'}:
            value = self.parse(values['date'])
            tz = self.parse(values['timezone'])
            target_tz = pytz.timezone(tz)
            out_value = value.replace(tzinfo=pytz.utc).astimezone(target_tz)
        else:
            out_value = self.parse(values)

        if _is_nat(out_value):
            return None

        if operator == '$dayOfYear':
            return out_value.timetuple().tm_yday
        if operator == '$dayOfMonth':
            return out_value.day
        if operator == '$dayOfWeek':
            return (out_value.isoweekday() % 7) + 1
        if operator == '$year':
            return out_value.year
        if operator == '$month':
            return out_value.month
        if operator == '$week':
            return int(out_value.strftime('%U'))
        if operator == '$hour':
            return out_value.hour
        if operator == '$minute':
            return out_value.minute
        if operator == '$second':
            return out_value.second
        if operator == '$millisecond':
            return int(out_value.microsecond / 1000)
        if operator == '$dateToString':
            if not isinstance(values, dict):
                raise OperationFailure(
                    '$dateToString operator must correspond a dict'
                    'that has "format" and "date" field.'
                )
            if not isinstance(values, dict) or not {'format', 'date'} <= set(values):
                raise OperationFailure(
                    '$dateToString operator must correspond a dict'
                    'that has "format" and "date" field.'
                )
            if '%L' in out_value['format']:
                raise NotImplementedError(
                    'Although %L is a valid date format for the '
                    '$dateToString operator, it is currently not implemented '
                    ' in Mongomock-ng.'
                )
            if 'onNull' in values:
                raise NotImplementedError(
                    'Although onNull is a valid field for the '
                    '$dateToString operator, it is currently not implemented '
                    ' in Mongomock-ng.'
                )
            if 'timezone' in values:
                raise NotImplementedError(
                    'Although timezone is a valid field for the '
                    '$dateToString operator, it is currently not implemented '
                    ' in Mongomock-ng.'
                )
            if _is_nat(out_value['date']):
                return None
            return out_value['date'].strftime(out_value['format'])
        if operator == '$dateFromParts':
            if not isinstance(out_value, dict):
                raise OperationFailure(
                    f'{operator} operator must correspond a dict '
                    'that has "year" or "isoWeekYear" field.'
                )
            if len(set(out_value) & {'year', 'isoWeekYear'}) != 1:
                raise OperationFailure(
                    f'{operator} operator must correspond a dict '
                    'that has "year" or "isoWeekYear" field.'
                )
            for field in ('isoWeekYear', 'isoWeek', 'isoDayOfWeek', 'timezone'):
                if field in out_value:
                    raise NotImplementedError(
                        f'Although {field} is a valid field for the '
                        f'{operator} operator, it is currently not implemented '
                        'in Mongomock-ng.'
                    )

            year = out_value['year']
            month = out_value.get('month', 1) or 1
            day = out_value.get('day', 1) or 1
            hour = out_value.get('hour', 0) or 0
            minute = out_value.get('minute', 0) or 0
            second = out_value.get('second', 0) or 0
            millisecond = out_value.get('millisecond', 0) or 0

            return datetime.datetime(
                year=year,
                month=month,
                day=day,
                hour=hour,
                minute=minute,
                second=second,
                microsecond=millisecond,
            )
        if operator in {'$dateAdd', '$dateSubtract'}:
            return _handle_date_add_operator(operator, values, out_value)
        if operator == '$dateDiff':
            return _handle_date_diff_operator(values, out_value)
        if operator == '$dateTrunc':
            return _handle_date_trunc_operator(values, out_value)
        if operator == '$dateFromString':
            return _handle_date_from_string_operator(values, out_value)

        raise NotImplementedError(
            f"Although '{operator}' is a valid date operator for the "
            f'aggregation pipeline, it is currently not implemented '
            f' in Mongomock-ng.'
        )

    def _handle_index_of_array_operator(self, value):
        if not isinstance(value, (list, tuple)):
            value = [value]
        if len(value) < 2 or len(value) > 4:
            raise OperationFailure(
                f'Expression $indexOfArray takes at least 2 arguments, and at most '
                f'4, but {len(value)} were passed in.'
            )

        array_value = self._parse_array_or_nothing(value[0])
        search_value = self._parse_or_nothing(value[1])
        start = self.parse(value[2]) if len(value) > 2 else 0
        end = self.parse(value[3]) if len(value) > 3 else None

        if array_value is NOTHING or array_value is None:
            return None
        if search_value is NOTHING:
            return -1
        if not isinstance(array_value, list):
            raise OperationFailure(
                '$indexOfArray requires an array as a first argument, '
                f'found: {type(type(array_value))}'
            )
        if len(value) > 2:
            if not isinstance(start, int):
                raise OperationFailure(
                    '$indexOfArrayrequires an integral starting index, found a '
                    f'value of type: {type(start)}, with value: "{start!r}"'
                )
            if start < 0:
                raise OperationFailure(
                    f'$indexOfArray requires a nonnegative starting index, found: {start!r}'
                )
        if len(value) > 3:
            if not isinstance(end, int):
                raise OperationFailure(
                    '$indexOfArrayrequires an integral ending index, found a '
                    f'value of type: {type(end)}, with value: "{end!r}"'
                )
            if end < 0:
                raise OperationFailure(
                    f'$indexOfArray requires a nonnegative ending index, found: {end!r}'
                )

        stop = len(array_value) if end is None else end
        try:
            return array_value.index(search_value, start, stop)
        except ValueError:
            return -1

    def _handle_array_operator(self, operator, value):
        if operator == '$concatArrays':
            if not isinstance(value, (list, tuple)):
                value = [value]

            parsed_list = [self._parse_array_or_none(item) for item in value]
            for parsed_item in parsed_list:
                if parsed_item is not None and not isinstance(parsed_item, (list, tuple)):
                    raise OperationFailure(
                        f'$concatArrays only supports arrays, not {type(parsed_item)}'
                    )

            if not parsed_list:
                raise OperationFailure('$concatArrays requires at least one operand')
            return None if None in parsed_list else list(itertools.chain.from_iterable(parsed_list))

        if operator == '$indexOfArray':
            return self._handle_index_of_array_operator(value)

        if operator == '$map':
            if not isinstance(value, dict):
                raise OperationFailure('$map only supports an object as its argument')

            # NOTE: while the two validations below could be achieved with
            # one-liner set operations (e.g. set(value) - {'input', 'as',
            # 'in'}), we prefer the iteration-based approaches in order to
            # mimic MongoDB's behavior regarding the order of evaluation. For
            # example, MongoDB complains about 'input' parameter missing before
            # 'in'.
            for k in ('input', 'in'):
                if k not in value:
                    raise OperationFailure(f"Missing '{k}' parameter to $map")

            for k in value:
                if k not in {'input', 'as', 'in'}:
                    raise OperationFailure(f'Unrecognized parameter to $map: {k}')

            input_array = self._parse_or_nothing(value['input'])

            if input_array is None or input_array is NOTHING:
                return None

            if not isinstance(input_array, (list, tuple)):
                raise OperationFailure(f'input to $map must be an array not {type(input_array)}')

            fieldname = value.get('as', 'this')
            in_expr = value['in']
            return [
                _Parser(
                    self._doc_dict,
                    dict(self._user_vars, **{fieldname: item}),
                    ignore_missing_keys=self._ignore_missing_keys,
                ).parse(in_expr)
                for item in input_array
            ]

        if operator == '$reduce':
            for k in ('input', 'initialValue', 'in'):
                if k not in value:
                    raise OperationFailure(f"Missing '{k}' parameter to $reduce")

            input_array = self._parse_or_nothing(value['input'])
            if input_array is None or input_array is NOTHING:
                return None

            if not isinstance(input_array, (list, tuple)):
                raise OperationFailure(f'input to $reduce must be an array not {type(input_array)}')

            current = self.parse(value['initialValue'])
            in_expr = value['in']
            for item in input_array:
                current = _Parser(
                    self._doc_dict,
                    dict(self._user_vars, this=item, value=current),
                    ignore_missing_keys=self._ignore_missing_keys,
                ).parse(in_expr)
            return current

        if operator == '$size':
            if isinstance(value, list):
                if len(value) != 1:
                    raise OperationFailure(
                        f'Expression $size takes exactly 1 arguments. {len(value)} were passed in.'
                    )
                value = value[0]
            array_value = self._parse_or_nothing(value)
            if not isinstance(array_value, (list, tuple)):
                raise OperationFailure(
                    'The argument to $size must be an array, but was of type: %s'
                    % ('missing' if array_value is NOTHING else type(array_value))
                )
            return len(array_value)

        if operator == '$filter':
            if not isinstance(value, dict):
                raise OperationFailure('$filter only supports an object as its argument')
            extra_params = set(value) - {'input', 'cond', 'as'}
            if extra_params:
                raise OperationFailure(f'Unrecognized parameter to $filter: {extra_params.pop()}')
            missing_params = {'input', 'cond'} - set(value)
            if missing_params:
                raise OperationFailure(f"Missing '{missing_params.pop()}' parameter to $filter")

            input_array = self.parse(value['input'])
            fieldname = value.get('as', 'this')
            cond = value['cond']
            return [
                item
                for item in input_array
                if _Parser(
                    self._doc_dict,
                    dict(self._user_vars, **{fieldname: item}),
                    ignore_missing_keys=self._ignore_missing_keys,
                ).parse(cond)
            ]
        if operator == '$slice':
            if not isinstance(value, list):
                raise OperationFailure('$slice only supports a list as its argument')
            if len(value) < 2 or len(value) > 3:
                raise OperationFailure(
                    f'Expression $slice takes at least 2 arguments, and at most '
                    f'3, but {len(value)} were passed in'
                )
            out_value = list(self.parse_many(value))
            array_value = out_value[0]
            if not isinstance(array_value, list):
                raise OperationFailure(
                    f'First argument to $slice must be an array, but is of '
                    f'type: {type(array_value)}'
                )
            for num, v in zip(('Second', 'Third'), out_value[1:]):
                if not isinstance(v, int):
                    raise OperationFailure(
                        f'{num} argument to $slice must be numeric, but is of type: {type(v)}'
                    )
            if len(out_value) > 2 and out_value[2] <= 0:
                raise OperationFailure(f'Third argument to $slice must be positive: {out_value[2]}')

            start = out_value[1]
            stop = None
            if start < 0:
                if len(out_value) > 2:
                    stop = len(array_value) + start + out_value[2]
            elif len(out_value) > 2:
                stop = start + out_value[2]
            else:
                stop = start
                start = 0
            return array_value[start:stop]

        raise NotImplementedError(
            f"Although '{operator}' is a valid array operator for the "
            f'aggregation pipeline, it is currently not implemented '
            f'in Mongomock-ng.'
        )

    def _handle_type_convertion_operator(self, operator, values):
        handler = self._TYPE_CONVERTION_HANDLERS[operator]
        return handler(self, values)

    def _handle_type_convertion_to_string(self, values):
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        if isinstance(parsed, bool):
            return str(parsed).lower()
        if _is_nat(parsed):
            return None
        if isinstance(parsed, datetime.datetime):
            return parsed.isoformat()[:-3] + 'Z'
        return str(parsed)

    def _handle_type_convertion_to_int(self, values):
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        if decimal_support:
            if isinstance(parsed, decimal128.Decimal128):
                return int(parsed.to_decimal())
            return int(parsed)
        raise NotImplementedError(
            'You need to import the pymongo library to support decimal128 type.'
        )

    def _handle_type_convertion_to_long(self, values):
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        if decimal_support:
            if isinstance(parsed, decimal128.Decimal128):
                return int(parsed.to_decimal())
            return int(parsed)
        raise NotImplementedError(
            'You need to import the pymongo library to support decimal128 type.'
        )

    def _handle_type_convertion_to_decimal(self, values):
        # Document: https://docs.mongodb.com/manual/reference/operator/aggregation/toDecimal/
        if not decimal_support:
            raise NotImplementedError(
                'You need to import the pymongo library to support decimal128 type.'
            )
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        if isinstance(parsed, bool):
            parsed = '1' if parsed is True else '0'
            decimal_value = decimal128.Decimal128(parsed)
        elif isinstance(parsed, int):
            decimal_value = decimal128.Decimal128(str(parsed))
        elif isinstance(parsed, float):
            exp = decimal.Decimal('.00000000000000')
            decimal_value = decimal.Decimal(str(parsed)).quantize(exp)
            decimal_value = decimal128.Decimal128(decimal_value)
        elif isinstance(parsed, decimal128.Decimal128):
            decimal_value = parsed
        elif isinstance(parsed, str):
            try:
                decimal_value = decimal128.Decimal128(parsed)
            except decimal.InvalidOperation as err:
                raise OperationFailure(
                    f"Failed to parse number '{parsed}' in $convert with no onError value:"
                    f'Failed to parse string to decimal'
                ) from err
        elif isinstance(parsed, datetime.datetime):
            epoch = datetime.datetime(1970, 1, 1)
            string_micro_seconds = str((parsed - epoch).total_seconds() * 1000).split('.', 1)[0]
            decimal_value = decimal128.Decimal128(string_micro_seconds)
        else:
            raise TypeError(f"'{type(parsed)}' type is not supported")
        return decimal_value

    def _handle_type_convertion_array_to_object(self, values):
        # Document: https://docs.mongodb.com/manual/reference/operator/aggregation/arrayToObject/
        try:
            parsed = self.parse(values)
        except KeyError:
            return None

        if parsed is None:
            return None

        if not isinstance(parsed, (list, tuple)):
            raise OperationFailure(f'$arrayToObject requires an array input, found: {type(parsed)}')

        if all(isinstance(x, dict) and set(x.keys()) == {'k', 'v'} for x in parsed):
            return {d['k']: d['v'] for d in parsed}

        if all(isinstance(x, (list, tuple)) and len(x) == 2 for x in parsed):
            return dict(parsed)

        raise OperationFailure(
            'arrays used with $arrayToObject must contain documents '
            'with k and v fields or two-element arrays'
        )

    def _handle_type_convertion_object_to_array(self, values):
        # Document: https://docs.mongodb.com/manual/reference/operator/aggregation/objectToArray/
        try:
            parsed = self.parse(values)
        except KeyError:
            return None

        if parsed is None:
            return None

        if not isinstance(parsed, (dict, collections.OrderedDict)):
            raise OperationFailure(
                f'$objectToArray requires an object input, found: {type(parsed)}'
            )

        return [{'k': k, 'v': v} for k, v in parsed.items()]

    def _handle_type_convertion_to_object_id(self, values):
        # Document: https://docs.mongodb.com/manual/reference/operator/aggregation/toObjectId/
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        if parsed is None:
            return None
        if isinstance(parsed, helpers.ObjectId):
            return parsed
        if isinstance(parsed, str):
            try:
                return helpers.ObjectId(parsed)
            except (ValueError, TypeError, InvalidId) as err:
                raise OperationFailure(
                    f"Failed to parse objectId '{parsed}' in $convert with no onError value"
                ) from err
        raise OperationFailure(
            '$toObjectId requires a string, ObjectId, or null input,'
            f' found: {type(parsed).__name__}'
        )

    def _handle_type_convertion_to_double(self, values):
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        if parsed is None:
            return None
        if isinstance(parsed, bool):
            return 1.0 if parsed else 0.0
        if isinstance(parsed, int):
            try:
                return float(parsed)
            except OverflowError as e:
                raise OperationFailure(str(e)) from e
        if isinstance(parsed, float):
            return parsed
        if isinstance(parsed, str):
            try:
                return float(parsed)
            except ValueError as err:
                raise OperationFailure(
                    f"Failed to parse number '{parsed}' in $convert with no onError value: "
                    f'Failed to parse string to double'
                ) from err
        if decimal_support and isinstance(parsed, decimal128.Decimal128):
            return float(parsed.to_decimal())
        raise OperationFailure(
            f"Unsupported conversion to double from type '{type(parsed).__name__}' in $convert."
        )

    def _handle_type_convertion_to_bool(self, values):
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        if parsed is None:
            return None
        if isinstance(parsed, bool):
            return parsed
        if isinstance(parsed, (int, float)):
            return parsed != 0
        if isinstance(parsed, str):
            return True
        if isinstance(parsed, (dict, list, tuple)):
            return True
        if decimal_support and isinstance(parsed, decimal128.Decimal128):
            return parsed.to_decimal() != 0
        return True

    def _handle_type_convertion_to_date(self, values):
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        if parsed is None:
            return None
        if _is_nat(parsed):
            return None
        if isinstance(parsed, datetime.datetime):
            return parsed
        if isinstance(parsed, (int, float)):
            return datetime.datetime.fromtimestamp(
                parsed / 1000.0, tz=datetime.timezone.utc
            ).replace(tzinfo=None)
        if isinstance(parsed, str):
            s = parsed.replace('Z', '+00:00') if parsed.endswith('Z') else parsed
            try:
                return datetime.datetime.fromisoformat(s)
            except ValueError as err:
                raise OperationFailure(
                    f"Failed to parse date '{parsed}' in $convert with no onError value: "
                    f'Failed to parse string to date'
                ) from err
        if isinstance(parsed, helpers.ObjectId):
            return parsed.generation_time
        raise OperationFailure(
            f"Unsupported conversion to date from type '{type(parsed).__name__}' in $convert."
        )

    def _handle_convert(self, values):
        try:
            parsed = self.parse(values)
        except KeyError:
            return None
        input_ = parsed['input']
        to_ = parsed['to']
        on_error = parsed.get('onError')
        on_null = parsed.get('onNull')

        if input_ is None:
            if on_null is not None:
                return on_null
            return None

        try:
            handler = self._CONVERT_TO_HANDLERS[to_]
        except KeyError as err:
            raise OperationFailure(f"'{to_}' is not a valid type for '$convert'.") from err

        try:
            return handler(self, input_)
        except Exception:
            if on_error is not None:
                return on_error
            raise

    @staticmethod
    def _raise_convert_not_implemented(to_):
        def handler(_self, _values):
            raise NotImplementedError(
                f"Although {to_} is a valid identifier for the '$convert' operator's 'to' field, "
                'it is currently not implemented in Mongomock-ng.'
            )

        return handler

    _TYPE_CONVERTION_HANDLERS: ClassVar[dict[str, Any]] = {
        '$toString': _handle_type_convertion_to_string,
        '$toInt': _handle_type_convertion_to_int,
        '$toLong': _handle_type_convertion_to_long,
        '$toDouble': _handle_type_convertion_to_double,
        '$toDecimal': _handle_type_convertion_to_decimal,
        '$toObjectId': _handle_type_convertion_to_object_id,
        '$toDate': _handle_type_convertion_to_date,
        '$arrayToObject': _handle_type_convertion_array_to_object,
        '$objectToArray': _handle_type_convertion_object_to_array,
        '$convert': _handle_convert,
    }

    # Document: https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/#syntax
    _CONVERT_TO_HANDLERS: ClassVar[dict[str | int, Any]] = {
        'double': _handle_type_convertion_to_double,
        'string': _handle_type_convertion_to_string,
        'objectId': _handle_type_convertion_to_object_id,
        'bool': _handle_type_convertion_to_bool,
        'date': _handle_type_convertion_to_date,
        'int': _handle_type_convertion_to_int,
        'long': _handle_type_convertion_to_long,
        'decimal': _handle_type_convertion_to_decimal,
        1: _handle_type_convertion_to_double,
        2: _handle_type_convertion_to_string,
        7: _handle_type_convertion_to_object_id,
        8: _handle_type_convertion_to_bool,
        9: _handle_type_convertion_to_date,
        16: _handle_type_convertion_to_int,
        18: _handle_type_convertion_to_long,
        19: _handle_type_convertion_to_decimal,
    }

    def _handle_type_operator(self, operator, values):
        # Document: https://docs.mongodb.com/manual/reference/operator/aggregation/isNumber/
        if operator == '$isNumber':
            try:
                parsed = self.parse(values)
            except KeyError:
                return False
            return False if isinstance(parsed, bool) else isinstance(parsed, numbers.Number)

        # Document: https://docs.mongodb.com/manual/reference/operator/aggregation/isArray/
        if operator == '$isArray':
            try:
                parsed = self.parse(values)
            except KeyError:
                return False
            return isinstance(parsed, (tuple, list))

        if operator == '$type':
            try:
                parsed = self.parse(values)
                if isinstance(parsed, bool):
                    return 'bool'
                if isinstance(parsed, str):
                    return 'string'
                if isinstance(parsed, dict):
                    return 'object'
                if isinstance(parsed, (list, tuple)):
                    return 'array'
                if parsed is None:
                    return 'null'
                if isinstance(parsed, float):
                    return 'double'
                if isinstance(parsed, int) and parsed > 2**31 - 1:
                    return 'long'
                if isinstance(parsed, int):
                    return 'int'
                if isinstance(parsed, datetime.datetime):
                    return 'date'
            except KeyError:
                return 'missing'
            raise NotImplementedError(f"Type '{type(parsed)}' is not supported yet")

        raise NotImplementedError(  # pragma: no cover
            f"Although '{operator}' is a valid type operator for the aggregation pipeline, "
            f'it is currently not implemented in Mongomock-ng.'
        )

    def _handle_conditional_operator(self, operator, values):
        if operator == '$ifNull':
            fields = values[:-1]
            if len(fields) > 1 and version.parse(mongomock_ng.SERVER_VERSION) <= version.parse(
                '4.4'
            ):
                raise OperationFailure(
                    '$ifNull supports only one input expression  in MongoDB v4.4 and lower'
                )
            fallback = values[-1]
            for field in fields:
                try:
                    out_value = self.parse(field)
                    if out_value is not None:
                        return out_value
                except KeyError:
                    pass
            return self.parse(fallback)
        if operator == '$cond':
            if isinstance(values, list):
                condition, true_case, false_case = values
            elif isinstance(values, dict):
                condition = values['if']
                true_case = values['then']
                false_case = values['else']
            condition_value = self._parse_to_bool(condition)
            expression = true_case if condition_value else false_case
            return self.parse(expression)
        # This should never happen: it is only a safe fallback if something went wrong.
        raise NotImplementedError(  # pragma: no cover
            f"Although '{operator}' is a valid conditional operator for the "
            f'aggregation pipeline, it is currently not implemented '
            f' in Mongomock-ng.'
        )

    def _handle_control_flow_operator(self, operator, values):
        if operator == '$switch':
            if not isinstance(values, dict):
                raise OperationFailure(
                    f'$switch requires an object as an argument, found: {type(values)}'
                )

            branches = values.get('branches', [])
            if not isinstance(branches, (list, tuple)):
                raise OperationFailure(
                    f"$switch expected an array for 'branches', found: {type(branches)}"
                )
            if not branches:
                raise OperationFailure('$switch requires at least one branch.')

            for branch in branches:
                if not isinstance(branch, dict):
                    raise OperationFailure(
                        f'$switch expected each branch to be an object, found: {type(branch)}'
                    )
                if 'case' not in branch:
                    raise OperationFailure("$switch requires each branch have a 'case' expression")
                if 'then' not in branch:
                    raise OperationFailure("$switch requires each branch have a 'then' expression.")

            for branch in branches:
                if self._parse_to_bool(branch['case']):
                    return self.parse(branch['then'])

            if 'default' not in values:
                raise OperationFailure(
                    '$switch could not find a matching branch for an input, '
                    'and no default was specified.'
                )
            return self.parse(values['default'])

        # This should never happen: it is only a safe fallback if something went wrong.
        raise NotImplementedError(  # pragma: no cover
            f"Although '{operator}' is a valid control flow operator for the "
            f'aggregation pipeline, it is currently not implemented '
            f'in Mongomock-ng.'
        )

    def _handle_set_operator(self, operator, values):
        if operator == '$in':
            expression, array = values
            parsed_expression = self.parse(expression)
            parsed_array = self.parse(array)
            if not isinstance(parsed_array, (list, tuple)):
                raise OperationFailure('$in requires an array')
            from mongomock_ng.filtering import operator_eq

            return any(operator_eq(parsed_expression, item) for item in parsed_array)
        if operator in ('$setUnion', '$setIntersection', '$setEquals'):
            if not isinstance(values, (list, tuple)):
                values = [values]

            min_args = {'$setEquals': 2}
            if len(values) < min_args.get(operator, 0):
                raise OperationFailure(
                    f'{operator} needs at least two arguments had: {len(values)!r}'
                )

            accepted_special_values = {
                '$setUnion': (None, NOTHING),
                '$setIntersection': (None, NOTHING),
            }
            values = [self._parse_or_nothing(v) for v in values]
            for v in values:
                if not isinstance(v, list) and v not in accepted_special_values.get(operator, []):
                    type_ = 'missing' if v is NOTHING else type(v)
                    raise OperationFailure(
                        f'All operands of {operator} must be arrays. '
                        f'One argument is of type: {type_}'
                    )
            input_sets = []
            for v in values:
                if v is None or v is NOTHING:
                    input_sets.append(None)
                else:
                    input_sets.append({helpers.to_hashable(elem) for elem in v})

            default_result = {'$setEquals': True}
            result = default_result.get(operator, set())
            prev_set = None
            for i, s in enumerate(input_sets):
                if s is None:
                    result = None
                    break
                if operator == '$setUnion':
                    if i == 0:
                        result = s
                    else:
                        result |= s
                elif operator == '$setIntersection':
                    if i == 0:
                        result = s
                    else:
                        result &= s
                elif operator == '$setEquals' and i > 0 and s != prev_set:
                    result = False
                    break
                prev_set = s
            if isinstance(result, set):
                result = [v.original for v in result]
            return result

        if operator == '$setDifference':
            values = [self._parse_or_nothing(v) for v in values]
            values = [None if v is NOTHING else v for v in values]
            for v in values:
                if not isinstance(v, list) and v is not None:
                    raise OperationFailure(
                        f'All operands of $setDifference must be arrays. '
                        f'One argument is of type: {type(v)}'
                    )
            input_sets = []
            for v in values:
                if v is None:
                    input_sets.append(None)
                else:
                    input_sets.append({helpers.to_hashable(elem) for elem in v})
            result = None
            for i, s in enumerate(input_sets):
                if s is None:
                    result = None
                    break
                if i == 0:
                    result = s
                else:
                    result -= s
            if result is not None:
                result = [v.original for v in result]
            return result

        if operator == '$setIsSubset':
            set1, set2 = values

            def parse_set(to_parse):
                if isinstance(to_parse, list):
                    return tuple(parse_set(v) for v in to_parse)
                if isinstance(to_parse, dict):
                    return helpers.hashdict({k: parse_set(v) for k, v in to_parse.items()})
                return to_parse

            return set(parse_set(self.parse(set1))).issubset(set(parse_set(self.parse(set2))))

        if operator in ('$anyElementTrue', '$allElementsTrue'):
            if not isinstance(values, (list, tuple)):
                values = [values]
            elements = []
            for v in values:
                parsed = self.parse(v)
                if isinstance(parsed, list):
                    elements.extend(parsed)
                else:
                    elements.append(parsed)
            if operator == '$anyElementTrue':
                return any(bool(v) for v in elements)
            return all(bool(v) for v in elements)

        raise NotImplementedError(
            f"Although '{operator}' is a valid set operator for the aggregation "
            f'pipeline, it is currently not implemented in Mongomock-ng.'
        )

    def _handle_object_operator(self, operator, values):
        if operator == '$mergeObjects':
            values = self.parse(values) if isinstance(values, str) else self.parse_many(values)
            return _merge_objects_operation(values)

        # This should never happen: it is only a safe fallback if something went wrong.
        raise NotImplementedError(
            f"Although '{operator}' is a valid object operator for the aggregation pipeline, "
            'it is currently not implemented in Mongomock-ng.'
        )

    def _handle_field_operator(self, operator, values):
        if operator == '$getField':
            if isinstance(values, str):
                field_name = values.lstrip('$')
                doc = self._doc_dict
            elif isinstance(values, dict):
                field_name = self.parse(values.get('field', ''))
                if not isinstance(field_name, str):
                    raise OperationFailure(
                        f'$getField requires field to be a string, got {type(field_name).__name__}'
                        f'$getField requires field to be a string, got {type(field_name).__name__}'
                    )
                field_name = field_name.lstrip('$')
                doc = (
                    self.parse(values.get('input', self._doc_dict))
                    if isinstance(values.get('input', self._doc_dict), str)
                    else values.get('input', self._doc_dict)
                )
            else:
                raise OperationFailure(
                    f'$getField expects a string or an object, got {type(values)}'
                )
            if not isinstance(doc, dict):
                return None
            return doc.get(field_name)

        raise NotImplementedError(
            f"Although '{operator}' is a valid field operator for the aggregation "
            f'pipeline, it is currently not implemented in Mongomock-ng.'
        )

    def _handle_custom_operator(self, operator, values):
        if operator == '$function':
            raise NotImplementedError(
                'The $function operator is not supported in Mongomock-ng for security reasons. '
                'Use a native aggregation expression instead.'
            )

        raise NotImplementedError(
            f"Although '{operator}' is a valid custom operator for the aggregation "
            f'pipeline, it is currently not implemented in Mongomock-ng.'
        )


def _parse_expression(expression, doc_dict, ignore_missing_keys=False, user_vars=None):
    """Parse an expression.

    Args:
        expression: an Aggregate Expression, see
            https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#aggregation-expressions.
        doc_dict: the document on which to evaluate the expression.
        ignore_missing_keys: if True, missing keys evaluated by the expression are ignored silently
            if it is possible.
    """
    return _Parser(doc_dict, user_vars=user_vars, ignore_missing_keys=ignore_missing_keys).parse(
        expression
    )


filtering.register_parse_expression(_parse_expression)


def _accumulate_group(output_fields, group_list, user_vars):
    doc_dict = {}
    for field, value in output_fields.items():
        if field == '_id':
            continue
        for operator, key in value.items():
            values = []
            for doc in group_list:
                try:
                    values.append(
                        _parse_expression(key, doc, ignore_missing_keys=True, user_vars=user_vars)
                    )
                except KeyError:
                    continue
            if operator in _GROUPING_OPERATOR_MAP:
                doc_dict[field] = _GROUPING_OPERATOR_MAP[operator](values)
            elif operator == '$addToSet':
                value = []
                # Don't use set in case elt in not hashable (like dicts).
                for elt in values:
                    if elt not in value:
                        value.append(elt)
                doc_dict[field] = value
            elif operator == '$push':
                if field not in doc_dict:
                    doc_dict[field] = values
                else:
                    doc_dict[field].extend(values)
            elif operator in group_operators:
                raise NotImplementedError(
                    f'Although {operator} is a valid group operator for the '
                    f'aggregation pipeline, it is currently not implemented '
                    f'in Mongomock-ng.'
                )
            else:
                raise NotImplementedError(
                    f'{operator} is not a valid group operator for the aggregation '
                    f'pipeline. See http://docs.mongodb.org/manual/meta/'
                    f'aggregation-quick-reference/ for a complete list of '
                    f'valid operators.'
                )
    return doc_dict


def _get_window_bounds(window_spec, total_len):
    if not window_spec:
        return [(0, total_len)] * total_len
    documents = window_spec.get('documents')
    if not documents:
        return [(0, total_len)] * total_len
    start_spec, end_spec = documents
    bounds = []
    for current_idx in range(total_len):
        if start_spec == 'unbounded':
            start = 0
        elif start_spec == 'current':
            start = current_idx
        else:
            start = max(0, current_idx + start_spec)

        if end_spec == 'unbounded':
            end = total_len
        elif end_spec == 'current':
            end = current_idx + 1
        else:
            end = min(total_len, current_idx + end_spec + 1)

        bounds.append((start, end))
    return bounds


def _sort_keys_equal(doc1, doc2, sort_by):
    for field in sort_by:
        val1 = helpers.get_value_by_dot(doc1, field)
        val2 = helpers.get_value_by_dot(doc2, field)
        if val1 != val2:
            return False
    return True


def _accumulate_set_window_fields(output_fields, partition, options):
    processed_partition = [dict(item) for item in partition]
    sort_by = options.get('sortBy', {})

    for field, field_value in output_fields.items():
        window_operator = next((x for x in field_value if x.startswith('$')), None)
        if window_operator not in set_window_fields_operators:
            raise OperationFailure(
                f'{window_operator} is not a valid window operator for the aggregation '
                'pipeline. See https://www.mongodb.com/docs/manual/reference/'
                'operator/aggregation/setWindowFields/#std-label-setWindowFields-window-operators'
                'for a complete list of valid operators.'
            )

        operator_value = field_value[window_operator]
        window_spec = field_value.get('window')
        window_bounds = _get_window_bounds(window_spec, len(partition))

        if window_operator == '$shift':
            if 'sortBy' not in options:
                raise OperationFailure(f'The {window_operator} operator requires a "sortBy" field')
            expr = operator_value['output']
            by = operator_value['by']
            default = operator_value.get('default')
            values = [_parse_expression(expr, doc) for doc in partition]
            for index, _item in enumerate(partition):
                by_index = index + by
                value = default if by_index < 0 or by_index >= len(values) else values[by_index]
                processed_partition[index][field] = value
        elif window_operator in ('$documentNumber',):
            for i in range(len(partition)):
                processed_partition[i][field] = i + 1
        elif window_operator in ('$rank', '$denseRank'):
            if not sort_by:
                raise OperationFailure(f'The {window_operator} operator requires a "sortBy" field')
            rank = 1
            for i, _doc in enumerate(partition):
                if i > 0 and not _sort_keys_equal(partition[i - 1], partition[i], sort_by):
                    if window_operator == '$rank':
                        rank = i + 1
                    else:
                        rank += 1
                processed_partition[i][field] = rank
        elif window_operator == '$count':
            for i in range(len(partition)):
                start, end = window_bounds[i]
                processed_partition[i][field] = end - start
        elif window_operator in (
            '$sum',
            '$avg',
            '$min',
            '$max',
            '$first',
            '$last',
            '$push',
            '$addToSet',
        ):
            values = [_parse_expression(operator_value, doc) for doc in partition]
            for i in range(len(partition)):
                start, end = window_bounds[i]
                window_values = values[start:end]
                window_non_null = [v for v in window_values if v is not None]
                if not window_non_null and window_operator in ('$sum', '$avg', '$min', '$max'):
                    processed_partition[i][field] = None
                elif window_operator == '$sum':
                    processed_partition[i][field] = sum(window_non_null)
                elif window_operator == '$avg':
                    processed_partition[i][field] = sum(window_non_null) / len(window_non_null)
                elif window_operator == '$min':
                    processed_partition[i][field] = min(window_non_null)
                elif window_operator == '$max':
                    processed_partition[i][field] = max(window_non_null)
                elif window_operator == '$first':
                    processed_partition[i][field] = window_values[0] if window_values else None
                elif window_operator == '$last':
                    processed_partition[i][field] = window_values[-1] if window_values else None
                elif window_operator == '$push':
                    processed_partition[i][field] = list(window_values)
                elif window_operator == '$addToSet':
                    seen = []
                    for v in window_values:
                        if v not in seen:
                            seen.append(v)
                    processed_partition[i][field] = seen
        else:
            raise NotImplementedError(
                f'Although {window_operator} is a valid window operator for the '
                'aggregation pipeline, it is currently not implemented '
                'in Mongomock-ng.'
            )
    return processed_partition


def _fix_sort_key(key_getter):
    def fixed_getter(doc):
        key = key_getter(doc)
        # Convert dictionaries to make sorted() work in Python 3.
        if isinstance(key, dict):
            return sorted(key.items())
        return key

    return fixed_getter


def _handle_lookup_stage(in_collection, database, options, user_vars):
    required_fields = ['as', 'from']
    if 'pipeline' not in options:
        required_fields.extend(['localField', 'foreignField'])

    for operator in required_fields:
        if operator not in options:
            raise OperationFailure(f"Must specify '{operator}' field for a $lookup")
        if not isinstance(options[operator], str):
            raise OperationFailure('Arguments to $lookup must be strings')
        if operator in ('as', 'localField', 'foreignField') and options[operator].startswith('$'):
            raise OperationFailure("FieldPath field names may not start with '$'")
        if operator == 'as' and '.' in options[operator]:
            raise NotImplementedError(
                "Although '.' is valid in the 'as' "
                'parameters for the lookup stage of the aggregation '
                'pipeline, it is currently not implemented in Mongomock-ng.'
            )

    foreign_name = options['from']
    local_name = options['as']
    local_field = options.get('localField')
    foreign_field = options.get('foreignField')
    pipeline = options.get('pipeline')
    let = options.get('let')
    foreign_collection = database.get_collection(foreign_name)

    for doc in in_collection:
        if local_field and foreign_field:
            try:
                query = helpers.get_value_by_dot(doc, local_field, can_generate_array=True)
            except KeyError:
                query = None
            if isinstance(query, list):
                query = {'$in': query}

            query = {foreign_field: query}
        else:
            query = {}

        matches = foreign_collection.find(query)

        if pipeline:
            if let:
                doc_user_vars = dict(user_vars or {})
                for var, expr in let.items():
                    doc_user_vars[var] = _parse_expression(expr, doc, user_vars=user_vars)
            else:
                doc_user_vars = user_vars

            matches = process_pipeline(
                (doc for doc in matches),
                database,
                pipeline,
                None,
                user_vars=doc_user_vars,
            )

        doc[local_name] = list(matches)

    return in_collection


def _recursive_get(match, nested_fields):
    head = match.get(nested_fields[0])
    remaining_fields = nested_fields[1:]
    if not remaining_fields:
        # Final/last field reached.
        yield head
        return
    # More fields to go, must be list, tuple, or dict.
    if isinstance(head, (list, tuple)):
        for m in head:
            yield from _recursive_get(m, remaining_fields)
    elif isinstance(head, dict):
        yield from _recursive_get(head, remaining_fields)


def _handle_graph_lookup_stage(in_collection, database, options, user_vars):
    if not isinstance(options.get('maxDepth', 0), int):
        raise OperationFailure("Argument 'maxDepth' to $graphLookup must be a number")
    if not isinstance(options.get('restrictSearchWithMatch', {}), dict):
        raise OperationFailure(
            "Argument 'restrictSearchWithMatch' to $graphLookup must be a Dictionary"
        )
    if not isinstance(options.get('depthField', ''), str):
        raise OperationFailure("Argument 'depthField' to $graphlookup must be a string")
    if 'startWith' not in options:
        raise OperationFailure("Must specify 'startWith' field for a $graphLookup")
    for operator in ('as', 'connectFromField', 'connectToField', 'from'):
        if operator not in options:
            raise OperationFailure(f"Must specify '{operator}' field for a $graphLookup")
        if not isinstance(options[operator], str):
            raise OperationFailure(f"Argument '{operator}' to $graphLookup must be string")
        if options[operator].startswith('$'):
            raise OperationFailure("FieldPath field names may not start with '$'")
        if operator == 'as' and '.' in options[operator]:
            raise NotImplementedError(
                f"Although '.' is valid in the '{operator}' "
                f'parameter for the $graphLookup stage of the aggregation '
                f'pipeline, it is currently not implemented in Mongomock-ng.'
            )

    foreign_name = options['from']
    start_with = options['startWith']
    connect_from_field = options['connectFromField']
    connect_to_field = options['connectToField']
    local_name = options['as']
    max_depth = options.get('maxDepth', None)
    depth_field = options.get('depthField', None)
    restrict_search_with_match = options.get('restrictSearchWithMatch', {})
    foreign_collection = database.get_collection(foreign_name)
    out_doc = copy.deepcopy(in_collection)  # TODO(pascal): speed the deep copy

    def _find_matches_for_depth(query):
        if isinstance(query, list):
            query = {'$in': query}
        matches = foreign_collection.find({connect_to_field: query})
        new_matches = []
        for new_match in matches:
            if (
                filtering.filter_applies(restrict_search_with_match, new_match)
                and new_match['_id'] not in found_items
            ):
                if depth_field is not None:
                    new_match = collections.OrderedDict(new_match, **{depth_field: depth})
                new_matches.append(new_match)
                found_items.add(new_match['_id'])
        return new_matches

    for doc in out_doc:
        found_items = set()
        depth = 0
        try:
            result = _parse_expression(start_with, doc, user_vars=user_vars)
        except KeyError:
            continue
        origin_matches = doc[local_name] = _find_matches_for_depth(result)
        while origin_matches and (max_depth is None or depth < max_depth):
            depth += 1
            newly_discovered_matches = []
            for match in origin_matches:
                nested_fields = connect_from_field.split('.')
                for match_target in _recursive_get(match, nested_fields):
                    newly_discovered_matches += _find_matches_for_depth(match_target)
            doc[local_name] += newly_discovered_matches
            origin_matches = newly_discovered_matches
    return out_doc


def _handle_group_stage(in_collection, unused_database, options, user_vars):
    grouped_collection = []
    _id = options['_id']
    if _id:

        def _key_getter(doc):
            try:
                return _parse_expression(_id, doc, ignore_missing_keys=True, user_vars=user_vars)
            except KeyError:
                return None

        def _sort_key_getter(doc):
            return filtering.BsonComparable(_key_getter(doc))

        # Sort the collection only for the itertools.groupby.
        # $group does not order its output document.
        sorted_collection = sorted(in_collection, key=_sort_key_getter)
        grouped = itertools.groupby(sorted_collection, _key_getter)
    else:
        grouped = [(None, in_collection)]

    for doc_id, group in grouped:
        group_list = list(group)
        doc_dict = _accumulate_group(options, group_list, user_vars=user_vars)
        doc_dict['_id'] = doc_id
        grouped_collection.append(doc_dict)

    return grouped_collection


def _handle_set_window_fields_stage(in_collection, unused_database, options, unused_user_vars):
    partition_key = options.get('partitionBy')
    processed_partitions = []
    if partition_key is not None:

        def _key_getter(doc):
            try:
                return _parse_expression(partition_key, doc, ignore_missing_keys=True)
            except KeyError:
                return None

        def _sort_key_getter(doc):
            return filtering.BsonComparable(_key_getter(doc))

        sorted_collection = sorted(in_collection, key=_sort_key_getter)
        partitions = itertools.groupby(sorted_collection, _key_getter)
        partitions = [list(partition[1]) for partition in partitions]
    else:
        partitions = [in_collection]
    sort = options.get('sortBy')
    if sort is not None:
        partitions = [
            _handle_sort_stage(partition, unused_database, sort, unused_user_vars)
            for partition in partitions
        ]
    output_fields = options.get('output')
    if output_fields is None:
        raise OperationFailure('The "output" field is required for $setWindowFields')
    for partition in partitions:
        processed_partition = _accumulate_set_window_fields(output_fields, partition, options)
        processed_partitions.append(processed_partition)
    return list(itertools.chain(*processed_partitions))


def _handle_bucket_stage(in_collection, unused_database, options, user_vars):
    unknown_options = set(options) - {'groupBy', 'boundaries', 'output', 'default'}
    if unknown_options:
        raise OperationFailure(f'Unrecognized option to $bucket: {unknown_options.pop()}.')
    if 'groupBy' not in options or 'boundaries' not in options:
        raise OperationFailure("$bucket requires 'groupBy' and 'boundaries' to be specified.")
    group_by = options['groupBy']
    boundaries = options['boundaries']
    if not isinstance(boundaries, list):
        raise OperationFailure(
            f"The $bucket 'boundaries' field must be an array, but found type: {type(boundaries)}"
        )
    if len(boundaries) < 2:
        raise OperationFailure(
            f"The $bucket 'boundaries' field must have at least 2 values, but "
            f'found {len(boundaries)} value(s).'
        )
    if sorted(boundaries) != boundaries:
        raise OperationFailure(
            "The 'boundaries' option to $bucket must be sorted in ascending order"
        )
    output_fields = options.get('output', {'count': {'$sum': 1}})
    default_value = options.get('default', None)
    try:
        is_default_last = default_value >= boundaries[-1]
    except TypeError:
        is_default_last = True

    def _get_default_bucket():
        try:
            return options['default']
        except KeyError as err:
            raise OperationFailure(
                '$bucket could not find a matching branch for '
                'an input, and no default was specified.'
            ) from err

    def _get_bucket_id(doc):
        """Get the bucket ID for a document.

        Note that it actually returns a tuple with the first
        param being a sort key to sort the default bucket even
        if it's not the same type as the boundaries.
        """
        try:
            value = _parse_expression(group_by, doc, user_vars=user_vars)
        except KeyError:
            return (is_default_last, _get_default_bucket())
        index = bisect.bisect_right(boundaries, value)
        if index and index < len(boundaries):
            return (False, boundaries[index - 1])
        return (is_default_last, _get_default_bucket())

    in_collection = ((_get_bucket_id(doc), doc) for doc in in_collection)
    out_collection = sorted(in_collection, key=lambda kv: kv[0])
    grouped = itertools.groupby(out_collection, lambda kv: kv[0])

    out_collection = []
    for (_, doc_id), group in grouped:
        group_list = [kv[1] for kv in group]
        doc_dict = _accumulate_group(output_fields, group_list, user_vars=user_vars)
        doc_dict['_id'] = doc_id
        out_collection.append(doc_dict)
    return out_collection


def _handle_sample_stage(in_collection, unused_database, options, unused_user_vars):
    if not isinstance(options, dict):
        raise OperationFailure('the $sample stage specification must be an object')
    size = options.pop('size', None)
    if size is None:
        raise OperationFailure('$sample stage must specify a size')
    if options:
        raise OperationFailure(f'unrecognized option to $sample: {set(options).pop()}')
    shuffled = list(in_collection)
    _random.shuffle(shuffled)
    return shuffled[:size]


def _handle_sort_by_count_stage(in_collection, unused_database, options, unused_user_vars):
    if isinstance(options, dict):
        raise NotImplementedError(
            'Although a dictionary is a valid option for the $sortByCount stage, '
            'it is currently not implemented in Mongomock-ng.'
        )
    field_to_count = options.lstrip('$')

    counter = collections.Counter(
        [doc[field_to_count] for doc in in_collection if field_to_count in doc]
    )
    return [
        {'_id': key, 'count': count}
        for key, count in sorted(counter.items(), key=lambda x: (-x[1], str(x[0])))
    ]


def _handle_sort_stage(in_collection, unused_database, options, unused_user_vars):
    sort_array = reversed([{x: y} for x, y in options.items()])
    sorted_collection = in_collection
    for sort_pair in sort_array:
        for sort_key, sort_direction in sort_pair.items():
            sorted_collection = sorted(
                sorted_collection,
                key=lambda x: filtering.resolve_sort_key(sort_key, x),
                reverse=sort_direction < 0,
            )
    return sorted_collection


def _handle_fill(in_collection, unused_database, options, unused_user_vars):
    output_fields = options.get('output', {})
    sort_by = options.get('sortBy')
    partition_by_fields = options.get('partitionByFields')

    if partition_by_fields:

        def _key_getter(doc):
            return tuple(doc.get(f) for f in partition_by_fields)

        sorted_collection = sorted(
            in_collection,
            key=lambda doc: filtering.BsonComparable(_key_getter(doc)),
        )
        partitions = [list(g) for _, g in itertools.groupby(sorted_collection, _key_getter)]
    else:
        partitions = [list(in_collection)]

    if sort_by:
        partitions = [
            _handle_sort_stage(p, unused_database, sort_by, unused_user_vars) for p in partitions
        ]

    result = []
    for partition in partitions:
        partition = [dict(doc) for doc in partition]
        for field, field_spec in output_fields.items():
            method = field_spec.get('method')
            value = field_spec.get('value')

            if method == 'locf':
                last_value = None
                for doc in partition:
                    if field in doc and doc[field] is not None:
                        last_value = doc[field]
                    elif field not in doc or doc[field] is None:
                        if last_value is not None:
                            doc[field] = last_value
                        elif value is not None:
                            doc[field] = value
            elif method == 'linear':
                non_null = [
                    i for i, doc in enumerate(partition) if field in doc and doc[field] is not None
                ]
                for i, doc in enumerate(partition):
                    if field not in doc or doc[field] is None:
                        left = None
                        right = None
                        for pos in non_null:
                            if pos < i:
                                left = pos
                            elif pos > i and right is None:
                                right = pos
                        if left is not None and right is not None:
                            y0 = partition[left][field]
                            y1 = partition[right][field]
                            if isinstance(y0, numbers.Number) and isinstance(y1, numbers.Number):
                                ratio = (i - left) / (right - left)
                                doc[field] = y0 + (y1 - y0) * ratio
                        elif value is not None:
                            doc[field] = value
            else:
                if value is not None:
                    for doc in partition:
                        if field not in doc or doc[field] is None:
                            doc[field] = value

        result.extend(partition)

    return result


def _handle_unwind_stage(in_collection, unused_database, options, unused_user_vars):
    if not isinstance(options, dict):
        options = {'path': options}
    path = options['path']
    if not isinstance(path, str) or path[0] != '$':
        raise ValueError(
            f"$unwind failed: exception: field path references must be prefixed with a '$' '{path}'"
        )
    path = path[1:]
    should_preserve_null_and_empty = options.get('preserveNullAndEmptyArrays')
    include_array_index = options.get('includeArrayIndex')
    unwound_collection = []
    for doc in in_collection:
        try:
            array_value = helpers.get_value_by_dot(doc, path)
        except KeyError:
            if should_preserve_null_and_empty:
                unwound_collection.append(doc)
            continue
        if array_value is None:
            if should_preserve_null_and_empty:
                unwound_collection.append(doc)
            continue
        if array_value == []:
            if should_preserve_null_and_empty:
                new_doc = copy.deepcopy(doc)
                # We just ran a get_value_by_dot so we know the value exists.
                helpers.delete_value_by_dot(new_doc, path)
                unwound_collection.append(new_doc)
            continue
        if isinstance(array_value, list):
            iter_array = enumerate(array_value)
        else:
            iter_array = [(None, array_value)]
        for index, field_item in iter_array:
            new_doc = copy.deepcopy(doc)
            new_doc = helpers.set_value_by_dot(new_doc, path, field_item)
            if include_array_index:
                new_doc = helpers.set_value_by_dot(new_doc, include_array_index, index)
            unwound_collection.append(new_doc)

    return unwound_collection


# TODO(pascal): Combine with the equivalent function in collection but check
# what are the allowed overriding.
def _combine_projection_spec(filter_list, original_filter, prefix=''):
    """Re-format a projection fields spec into a nested dictionary.

    e.g: ['a', 'b.c', 'b.d'] => {'a': 1, 'b': {'c': 1, 'd': 1}}
    """
    if not isinstance(filter_list, list):
        return filter_list

    filter_dict = collections.OrderedDict()

    for key in filter_list:
        field, separator, subkey = key.partition('.')
        if not separator:
            if isinstance(filter_dict.get(field), list):
                other_key = field + '.' + filter_dict[field][0]
                raise OperationFailure(
                    'Invalid $project :: caused by :: specification contains two conflicting paths.'
                    f' Cannot specify both {prefix + field!r} and {prefix + other_key!r}: '
                    f'{original_filter}'
                )
            filter_dict[field] = 1
            continue
        if not isinstance(filter_dict.get(field, []), list):
            raise OperationFailure(
                'Invalid $project :: caused by :: specification contains two conflicting paths.'
                f' Cannot specify both {prefix + field!r} and {prefix + key!r}: {original_filter}'
            )
        filter_dict[field] = [*filter_dict.get(field, []), subkey]

    return collections.OrderedDict(
        (k, _combine_projection_spec(v, original_filter, prefix=f'{prefix}{k}.'))
        for k, v in filter_dict.items()
    )


def _project_by_spec(doc, proj_spec, is_include):
    output = {}
    for key, value in doc.items():
        if key not in proj_spec:
            if not is_include:
                output[key] = value
            continue

        if not isinstance(proj_spec[key], dict):
            if is_include:
                output[key] = value
            continue

        if isinstance(value, dict):
            output[key] = _project_by_spec(value, proj_spec[key], is_include)
        elif isinstance(value, list):
            output[key] = [
                _project_by_spec(array_value, proj_spec[key], is_include)
                for array_value in value
                if isinstance(array_value, dict)
            ]
        elif not is_include:
            output[key] = value

    return output


def _handle_replace_root_stage(in_collection, unused_database, options, user_vars):
    if 'newRoot' not in options:
        raise OperationFailure("Parameter 'newRoot' is missing for $replaceRoot operation.")

    return _replace_root_documents(in_collection, options['newRoot'], user_vars)


def _replace_root_documents(in_collection, expression, user_vars):
    out_collection = []
    for doc in in_collection:
        try:
            new_doc = _parse_expression(
                expression, doc, ignore_missing_keys=True, user_vars=user_vars
            )
        except KeyError:
            new_doc = NOTHING
        if not isinstance(new_doc, dict):
            raise OperationFailure(
                f"'newRoot' expression must evaluate to an object, but resulting value was: "
                f'{new_doc}'
            )
        out_collection.append(new_doc)
    return out_collection


def _handle_replace_with_stage(in_collection, unused_database, options, user_vars):
    return _replace_root_documents(in_collection, options, user_vars)


def _handle_project_stage(in_collection, unused_database, options, user_vars):
    filter_list = []
    method = None
    include_id = options.get('_id')
    # Compute new values for each field, except inclusion/exclusions that are
    # handled in one final step.
    new_fields_collection = None
    for field, value in options.items():
        if method is None and (field != '_id' or value):
            method = 'include' if value else 'exclude'
        elif method == 'include' and not value and field != '_id':
            raise OperationFailure(
                f'Bad projection specification, cannot exclude fields '
                f"other than '_id' in an inclusion projection: {options}"
            )
        elif method == 'exclude' and value and field != '_id':
            raise OperationFailure(
                f'Bad projection specification, cannot include fields '
                f'or add computed fields during an exclusion projection: {options}'
            )
        if value in (0, 1, True, False):
            if field != '_id':
                filter_list.append(field)
            continue
        if not new_fields_collection:
            new_fields_collection = [{} for unused_doc in in_collection]

        for in_doc, out_doc in zip(in_collection, new_fields_collection):
            with contextlib.suppress(KeyError):
                out_doc[field] = _parse_expression(
                    value, in_doc, ignore_missing_keys=True, user_vars=user_vars
                )
    if (method == 'include') == (include_id is not False and include_id != 0):
        filter_list.append('_id')

    if not filter_list:
        return new_fields_collection

    # Final steps: include or exclude fields and merge with newly created fields.
    projection_spec = _combine_projection_spec(filter_list, original_filter=options)
    out_collection = [
        _project_by_spec(doc, projection_spec, is_include=method == 'include')
        for doc in in_collection
    ]
    if new_fields_collection:
        return [dict(a, **b) for a, b in zip(out_collection, new_fields_collection)]
    return out_collection


def _handle_redact_stage(in_collection, unused_database, options, user_vars):
    if not options:
        raise OperationFailure(
            'Invalid $redact :: caused by :: specification must have at least one field'
        )

    out_collection = []
    for doc in in_collection:
        out_doc = _handle_redact_stage_expression(options, doc)
        if out_doc is not None:
            out_collection.append(out_doc)
    return out_collection


def _handle_redact_stage_expression(expression, doc):
    redact_vars = {i: i for i in ['PRUNE', 'KEEP', 'DESCEND']}
    try:
        expr_result = _parse_expression(expression, doc, user_vars=redact_vars)
    except KeyError as ex:
        raise OperationFailure(f'Invalid $redact :: caused by :: {ex}') from ex

    if expr_result == 'PRUNE':
        return None
    elif expr_result == 'KEEP':
        return doc
    elif expr_result == 'DESCEND':
        return {k: _handle_redact_descend_values(expression, v) for k, v in doc.items()}


def _handle_redact_descend_values(expression, value):
    if isinstance(value, dict):
        return _handle_redact_stage_expression(expression, value)
    elif isinstance(value, list):
        out_value = []
        for item in value:
            new_item = _handle_redact_descend_values(expression, item)
            if new_item is not None:
                out_value.append(new_item)
        return out_value

    return value


def _handle_add_fields_stage(in_collection, unused_database, options, user_vars):
    if not options:
        raise OperationFailure(
            'Invalid $addFields :: caused by :: specification must have at least one field'
        )
    out_collection = [dict(doc) for doc in in_collection]
    for field, value in options.items():
        for in_doc, out_doc in zip(in_collection, out_collection):
            try:
                out_value = _parse_expression(
                    value, in_doc, user_vars=user_vars, ignore_missing_keys=True
                )
            except KeyError:
                continue
            parts = field.split('.')
            for subfield in parts[:-1]:
                out_doc[subfield] = out_doc.get(subfield, {})
                if not isinstance(out_doc[subfield], dict):
                    out_doc[subfield] = {}
                out_doc = out_doc[subfield]
            out_doc[parts[-1]] = out_value
    return out_collection


def _handle_out_stage(in_collection, database, options, unused_user_vars):
    # TODO(MetrodataTeam): should leave the origin collection unchanged
    out_collection = database.get_collection(options)
    if out_collection.find_one():
        out_collection.drop()
    if in_collection:
        out_collection.insert_many(in_collection)
    return in_collection


def _get_merge_collection(database, into):
    if isinstance(into, str):
        return database.get_collection(into)
    if not isinstance(into, dict):
        raise OperationFailure("$merge 'into' field must be a string or object")

    collection_name = into.get('coll')
    if not isinstance(collection_name, str):
        raise OperationFailure("$merge 'into.coll' field must be a string")

    database_name = into.get('db')
    if database_name is None:
        target_database = database
    elif isinstance(database_name, str):
        target_database = database.client[database_name]
    else:
        raise OperationFailure("$merge 'into.db' field must be a string")

    return target_database.get_collection(collection_name)


def _normalize_merge_on(on):
    if isinstance(on, str):
        return [on]
    if isinstance(on, list) and on and all(isinstance(field, str) for field in on):
        return on
    raise OperationFailure("$merge 'on' field must be a string or non-empty list of strings")


def _build_merge_query(doc, on_fields):
    query = {}
    for field in on_fields:
        try:
            value = helpers.get_value_by_dot(doc, field)
        except KeyError as err:
            raise OperationFailure(
                f"$merge requires the field '{field}' to be present in each input document"
            ) from err
        helpers.set_value_by_dot(query, field, value)
    return query


def _merge_with_existing_document(existing_doc, new_doc, on_fields):
    merged_doc = copy.deepcopy(existing_doc)
    merged_doc.update(copy.deepcopy(new_doc))
    if '_id' not in on_fields and '_id' in existing_doc:
        merged_doc['_id'] = existing_doc['_id']
    return merged_doc


def _get_merge_replacement_document(existing_doc, new_doc, on_fields):
    replacement_doc = copy.deepcopy(new_doc)
    if '_id' not in on_fields and '_id' in existing_doc:
        replacement_doc['_id'] = existing_doc['_id']
    return replacement_doc


def _handle_merge_stage(in_collection, database, options, unused_user_vars):
    if isinstance(options, str):
        options = {'into': options}
    elif not isinstance(options, dict):
        raise OperationFailure('$merge stage specification must be a string or object')

    if 'into' not in options:
        raise OperationFailure("Must specify 'into' field for a $merge")

    when_matched = options.get('whenMatched', 'merge')
    when_not_matched = options.get('whenNotMatched', 'insert')
    valid_when_matched = {'replace', 'keepExisting', 'merge', 'fail', 'pipeline'}
    valid_when_not_matched = {'insert', 'discard', 'fail'}

    if when_matched not in valid_when_matched:
        raise OperationFailure(f"Invalid $merge 'whenMatched' mode: {when_matched}")
    if when_not_matched not in valid_when_not_matched:
        raise OperationFailure(f"Invalid $merge 'whenNotMatched' mode: {when_not_matched}")
    if when_matched == 'pipeline':
        raise NotImplementedError("$merge with 'whenMatched: pipeline' is not implemented")

    target_collection = _get_merge_collection(database, options['into'])
    on_fields = _normalize_merge_on(options.get('on', '_id'))

    for doc in in_collection:
        query = _build_merge_query(doc, on_fields)
        existing_doc = target_collection.find_one(query)

        if existing_doc is None:
            if when_not_matched == 'insert':
                target_collection.insert_one(copy.deepcopy(doc))
            elif when_not_matched == 'fail':
                raise OperationFailure('$merge failed because no matching document was found')
            continue

        if when_matched == 'replace':
            replacement_doc = _get_merge_replacement_document(existing_doc, doc, on_fields)
            target_collection.replace_one({'_id': existing_doc['_id']}, replacement_doc)
        elif when_matched == 'merge':
            merged_doc = _merge_with_existing_document(existing_doc, doc, on_fields)
            target_collection.replace_one({'_id': existing_doc['_id']}, merged_doc)
        elif when_matched == 'keepExisting':
            continue
        elif when_matched == 'fail':
            raise OperationFailure('$merge failed because a matching document already exists')

    return in_collection


def _handle_count_stage(in_collection, database, options, unused_user_vars):
    if not isinstance(options, str) or options == '':
        raise OperationFailure('the count field must be a non-empty string')
    elif options.startswith('$'):
        raise OperationFailure('the count field cannot be a $-prefixed path')
    elif '.' in options:
        raise OperationFailure("the count field cannot contain '.'")
    return [{options: len(in_collection)}]


def _handle_facet_stage(in_collection, database, options, user_vars):
    out_collection_by_pipeline = {}
    for pipeline_title, pipeline in options.items():
        out_collection_by_pipeline[pipeline_title] = list(
            process_pipeline(in_collection, database, pipeline, None, user_vars=user_vars)
        )
    return [out_collection_by_pipeline]


def _handle_union_with_stage(in_collection, database, options, user_vars):
    if isinstance(options, str):
        coll_name = options
        pipeline = None
    elif isinstance(options, dict):
        coll_name = options.get('coll')
        if coll_name is None:
            raise OperationFailure("Must specify 'coll' field for a $unionWith")
        if not isinstance(coll_name, str):
            raise OperationFailure('Arguments to $unionWith must be strings')
        pipeline = options.get('pipeline')
    else:
        raise OperationFailure('$unionWith stage specification must be a string or object')

    foreign_collection = database.get_collection(coll_name)
    foreign_docs = list(foreign_collection.find({}))
    if pipeline:
        foreign_docs = list(
            process_pipeline(foreign_docs, database, pipeline, None, user_vars=user_vars)
        )
    return list(in_collection) + foreign_docs


def _handle_match_stage(in_collection, database, options, user_vars):
    spec = helpers.patch_datetime_awareness_in_document(options)
    return [
        doc
        for doc in in_collection
        if filtering.filter_applies(
            spec, helpers.patch_datetime_awareness_in_document(doc), user_vars=user_vars
        )
    ]


def _handle_unset_stage(in_collection, database, options, user_vars=None):
    out_collection = [dict(doc) for doc in in_collection]

    if isinstance(options, str):
        fields = [options]
    elif isinstance(options, list):
        fields = options
    else:
        raise OperationFailure('$unset options must be a string or list of strings')

    for out_doc in out_collection:
        for field in fields:
            parts = field.split('.')
            sub_doc = out_doc
            for subfield in parts[:-1]:
                sub_doc = sub_doc.get(subfield, {})
                if not isinstance(sub_doc, dict):
                    break
            else:
                sub_doc.pop(parts[-1], None)
    return out_collection


_PIPELINE_HANDLERS = {
    '$addFields': _handle_add_fields_stage,
    '$bucket': _handle_bucket_stage,
    '$bucketAuto': None,
    '$collStats': None,
    '$count': _handle_count_stage,
    '$currentOp': None,
    '$facet': _handle_facet_stage,
    '$geoNear': None,
    '$graphLookup': _handle_graph_lookup_stage,
    '$group': _handle_group_stage,
    '$setWindowFields': _handle_set_window_fields_stage,
    '$indexStats': None,
    '$limit': lambda c, d, o, v: c[:o],
    '$listLocalSessions': None,
    '$listSessions': None,
    '$lookup': _handle_lookup_stage,
    '$match': _handle_match_stage,
    '$merge': _handle_merge_stage,
    '$out': _handle_out_stage,
    '$planCacheStats': None,
    '$project': _handle_project_stage,
    '$redact': _handle_redact_stage,
    '$replaceRoot': _handle_replace_root_stage,
    '$replaceWith': _handle_replace_with_stage,
    '$sample': _handle_sample_stage,
    '$set': _handle_add_fields_stage,
    '$skip': lambda c, d, o, v: c[o:],
    '$sort': _handle_sort_stage,
    '$sortByCount': _handle_sort_by_count_stage,
    '$unset': _handle_unset_stage,
    '$unionWith': _handle_union_with_stage,
    '$unwind': _handle_unwind_stage,
    '$fill': _handle_fill,
}


def process_pipeline(collection, database, pipeline, session, user_vars=None):
    if session:
        raise NotImplementedError('Mongomock-ng does not handle sessions yet')

    for stage in pipeline:
        for operator, options in stage.items():
            try:
                handler = _PIPELINE_HANDLERS[operator]
            except KeyError as err:
                raise NotImplementedError(
                    f'{operator} is not a valid operator for the aggregation pipeline. '
                    f'See http://docs.mongodb.org/manual/meta/aggregation-quick-reference/ '
                    f'for a complete list of valid operators.'
                ) from err
            if not handler:
                raise NotImplementedError(
                    f"Although '{operator}' is a valid operator for the aggregation pipeline, "
                    f'it is currently not implemented in Mongomock-ng.'
                )
            collection = handler(collection, database, options, user_vars)

    return command_cursor.CommandCursor(collection)


def validate_stage_name(name):
    if name not in _PIPELINE_HANDLERS:
        raise OperationFailure(f"Unrecognized pipeline stage name: '{name}'")
