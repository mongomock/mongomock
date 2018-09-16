"""Module to handle the operations within the aggregate pipeline."""

import math
import six

from mongomock import filtering
from mongomock import helpers
from mongomock import OperationFailure


arithmetic_operators = [
    '$abs',
    '$add',
    '$ceil',
    '$divide',
    '$exp',
    '$floor',
    '$ln',
    '$log',
    '$log10',
    '$mod',
    '$multiply',
    '$pow',
    '$sqrt',
    '$subtract',
    '$trunc',
]
project_operators = [
    '$max',
    '$min',
    '$avg',
    '$sum',
    '$stdDevPop',
    '$stdDevSamp',
    '$arrayElemAt',
]
projection_operators = ['$map', '$let', '$literal']
date_operators = [
    '$dayOfYear',
    '$dayOfMonth',
    '$dayOfWeek',
    '$year',
    '$month',
    '$week',
    '$hour',
    '$minute',
    '$second',
    '$millisecond',
    '$dateToString',
]
conditional_operators = ['$cond', '$ifNull']
array_operators = [
    '$concatArrays',
    '$filter',
    '$isArray',
    '$size',
    '$slice',
]
text_search_operators = ['$meta']
string_operators = [
    '$concat',
    '$strcasecmp',
    '$substr',
    '$toLower',
    '$toUpper',
]
comparison_operators = [
    '$cmp',
    '$eq',
    '$ne',
] + list(filtering.SORTING_OPERATOR_MAP.keys())
boolean_operators = ['$and', '$or', '$not']
set_operators = [
    '$setEquals',
    '$setIntersection',
    '$setDifference',
    '$setUnion',
    '$setIsSubset',
    '$anyElementTrue',
    '$allElementsTrue',
]


class Parser(object):
    """Helper to parse expressions within the aggregate pipeline."""

    def __init__(self, doc_dict):
        self._doc_dict = doc_dict

    def parse(self, expression):
        """Parse a MongoDB expression."""
        if not isinstance(expression, dict):
            return self._parse_basic_expression(expression)

        value_dict = {}
        for k, v in six.iteritems(expression):
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
            if k in boolean_operators + set_operators + string_operators + \
                    text_search_operators + projection_operators:
                raise NotImplementedError(
                    "'%s' is a valid operation but it is not supported by Mongomock yet." % k)
            if k.startswith('$'):
                raise OperationFailure("Unrecognized expression '%s'" % k)
            value_dict[k] = self.parse(v)

        return value_dict

    def _parse_basic_expression(self, expression):
        if isinstance(expression, six.string_types) and expression.startswith('$'):
            get_value = helpers.embedded_item_getter(expression.replace('$', ''))
            return get_value(self._doc_dict)
        return expression

    def _handle_arithmetic_operator(self, operator, values):
        if operator == '$abs':
            return abs(self.parse(values))
        if operator == '$ceil':
            return math.ceil(self.parse(values))
        if operator == '$divide':
            assert len(values) == 2, 'divide must have only 2 items'
            return self.parse(values[0]) / self.parse(values[1])
        if operator == '$exp':
            return math.exp(self.parse(values))
        if operator == '$floor':
            return math.floor(self.parse(values))
        if operator == '$ln':
            return math.log(self.parse(values))
        if operator == '$log':
            assert len(values) == 2, 'log must have only 2 items'
            return math.log(self.parse(values[0]), self.parse(values[1]))
        if operator == '$log10':
            return math.log10(self.parse(values))
        if operator == '$mod':
            assert len(values) == 2, 'mod must have only 2 items'
            return math.fmod(self.parse(values[0]), self.parse(values[1]))
        if operator == '$pow':
            assert len(values) == 2, 'pow must have only 2 items'
            return math.pow(self.parse(values[0]), self.parse(values[1]))
        if operator == '$sqrt':
            return math.sqrt(self.parse(values))
        if operator == '$subtract':
            assert len(values) == 2, 'subtract must have only 2 items'
            return self.parse(values[0]) - self.parse(values[1])
        raise NotImplementedError("Although '%s' is a valid aritmetic operator for the "
                                  'aggregation pipeline, it is currently not implemented '
                                  ' in Mongomock.' % operator)

    def _handle_project_operator(self, operator, values):
        if operator == '$min':
            if len(values) > 2:
                raise NotImplementedError('Although %d is a valid amount of elements in '
                                          'aggregation pipeline, it is currently not '
                                          ' implemented in Mongomock' % len(values))
            return min(self.parse(values[0]), self.parse(values[1]))
        if operator == '$arrayElemAt':
            key, index = values
            array = self._parse_basic_expression(key)
            v = array[index]
            return v
        raise NotImplementedError("Although '%s' is a valid project operator for the "
                                  'aggregation pipeline, it is currently not implemented '
                                  'in Mongomock.' % operator)

    def _handle_projection_operator(self, operator, value):
        if operator == '$literal':
            return value
        raise NotImplementedError("Although '%s' is a valid project operator for the "
                                  'aggregation pipeline, it is currently not implemented '
                                  'in Mongomock.' % operator)

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
            "Although '%s' is a valid comparison operator for the "
            'aggregation pipeline, it is currently not implemented '
            ' in Mongomock.' % operator)

    def _handle_date_operator(self, operator, values):
        out_value = self.parse(values)
        if operator == '$dayOfYear':
            return out_value.timetuple().tm_yday
        if operator == '$dayOfMonth':
            return out_value.day
        if operator == '$dayOfWeek':
            return out_value.isoweekday()
        if operator == '$year':
            return out_value.year
        if operator == '$month':
            return out_value.month
        if operator == '$week':
            return out_value.isocalendar()[1]
        if operator == '$hour':
            return out_value.hour
        if operator == '$minute':
            return out_value.minute
        if operator == '$second':
            return out_value.second
        if operator == '$millisecond':
            return int(out_value.microsecond / 1000)
        raise NotImplementedError(
            "Although '%s' is a valid date operator for the "
            'aggregation pipeline, it is currently not implemented '
            ' in Mongomock.' % operator)

    def _handle_array_operator(self, operator, value):
        if operator == '$size':
            if isinstance(value, list):
                if len(value) != 1:
                    raise OperationFailure('Expression $size takes exactly 1 arguments. '
                                           '%d were passed in.' % len(value))
                value = value[0]
            array_value = self.parse(value)
            if not isinstance(array_value, list):
                raise OperationFailure('The argument to $size must be an array, '
                                       'but was of type: %s' % type(array_value))
            return len(array_value)
        raise NotImplementedError(
            "Although '%s' is a valid array operator for the "
            'aggregation pipeline, it is currently not implemented '
            'in Mongomock.' % operator)

    def _handle_conditional_operator(self, operator, values):
        if operator == '$ifNull':
            field, fallback = values
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
            try:
                condition_value = self.parse(condition)
            except KeyError:
                condition_value = False
            expression = true_case if condition_value else false_case
            return self.parse(expression)
        raise NotImplementedError(
            "Although '%s' is a valid conditional operator for the "
            'aggregation pipeline, it is currently not implemented '
            ' in Mongomock.' % operator)


def parse_expression(expression, doc_dict):
    """Parse an expression."""
    return Parser(doc_dict).parse(expression)
