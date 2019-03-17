"""Module to handle the operations within the aggregate pipeline."""

import bisect
import collections
import copy
import datetime
import functools
import itertools
import math
import numbers
import random
import six
from six import moves

from mongomock import command_cursor
from mongomock import filtering
from mongomock import helpers
from mongomock import OperationFailure

_random = random.Random()


group_operators = [
    '$addToSet',
    '$first',
    '$last',
    '$max',
    '$min',
    '$avg',
    '$push',
    '$sum',
    '$stdDevPop',
    '$stdDevSamp']
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
    '$in',
    '$setEquals',
    '$setIntersection',
    '$setDifference',
    '$setUnion',
    '$setIsSubset',
    '$anyElementTrue',
    '$allElementsTrue',
]


def _avg_operation(values):
    values_list = list(v for v in values if isinstance(v, numbers.Number))
    if not values_list:
        return None
    return sum(values_list) / float(len(list(values_list)))


def _group_operation(values, operator):
    values_list = list(v for v in values if v is not None)
    if not values_list:
        return None
    return operator(values_list)


_GROUPING_OPERATOR_MAP = {
    '$sum': lambda values: sum(v for v in values if isinstance(v, numbers.Number)),
    '$avg': _avg_operation,
    '$min': lambda values: _group_operation(values, min),
    '$max': lambda values: _group_operation(values, max),
}


class _Parser(object):
    """Helper to parse expressions within the aggregate pipeline."""

    def __init__(self, doc_dict, user_vars=None):
        self._doc_dict = doc_dict
        self._user_vars = user_vars or {}

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
            if k in set_operators:
                return self._handle_set_operator(k, v)
            if k in boolean_operators + string_operators + \
                    text_search_operators + projection_operators:
                raise NotImplementedError(
                    "'%s' is a valid operation but it is not supported by Mongomock yet." % k)
            if k.startswith('$'):
                raise OperationFailure("Unrecognized expression '%s'" % k)
            value_dict[k] = self.parse(v)

        return value_dict

    def _parse_basic_expression(self, expression):
        if isinstance(expression, six.string_types) and expression.startswith('$'):
            if expression.startswith('$$'):
                return helpers.get_value_by_dot(dict({
                    'ROOT': self._doc_dict,
                    'CURRENT': self._doc_dict,
                }, **self._user_vars), expression[2:])
            return helpers.get_value_by_dot(self._doc_dict, expression[1:], can_generate_array=True)
        return expression

    def _handle_arithmetic_operator(self, operator, values):
        if operator == '$abs':
            return abs(self.parse(values))
        if operator == '$add':
            return sum(self.parse(value) for value in values)
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
        if operator == '$multiply':
            return moves.reduce(
                lambda x, y: x * y,
                (self.parse(value) for value in values))
        if operator == '$pow':
            assert len(values) == 2, 'pow must have only 2 items'
            return math.pow(self.parse(values[0]), self.parse(values[1]))
        if operator == '$sqrt':
            return math.sqrt(self.parse(values))
        if operator == '$subtract':
            assert len(values) == 2, 'subtract must have only 2 items'
            res = self.parse(values[0]) - self.parse(values[1])
            if isinstance(res, datetime.timedelta):
                return round(res.total_seconds() * 1000)
            return res
        if operator == '$trunc':
            return math.trunc(self.parse(values))
        # This should never happen: it is only a safe fallback if something went wrong.
        raise NotImplementedError(  # pragma: no cover
            "Although '%s' is a valid aritmetic operator for the aggregation "
            'pipeline, it is currently not implemented  in Mongomock.' % operator)

    def _handle_project_operator(self, operator, values):
        if operator in _GROUPING_OPERATOR_MAP:
            return _GROUPING_OPERATOR_MAP[operator](self.parse(val) for val in values)
        if operator == '$arrayElemAt':
            key, index = values
            array = self._parse_basic_expression(key)
            return array[index]
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

        if operator == '$filter':
            if not isinstance(value, dict):
                raise OperationFailure('$filter only supports an object as its argument')
            extra_params = set(value) - {'input', 'cond', 'as'}
            if extra_params:
                raise OperationFailure('Unrecognized parameter to $filter: %s' % extra_params.pop())
            missing_params = {'input', 'cond'} - set(value)
            if missing_params:
                raise OperationFailure("Missing '%s' parameter to $filter" % missing_params.pop())

            input_array = self.parse(value['input'])
            fieldname = value.get('as', 'this')
            cond = value['cond']
            return [
                item for item in input_array
                if _Parser(self._doc_dict, dict(self._user_vars, **{fieldname: item})).parse(cond)
            ]

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
        # This should never happen: it is only a safe fallback if something went wrong.
        raise NotImplementedError(  # pragma: no cover
            "Although '%s' is a valid conditional operator for the "
            'aggregation pipeline, it is currently not implemented '
            ' in Mongomock.' % operator)

    def _handle_set_operator(self, operator, values):
        if operator == '$in':
            expression, array = values
            return self.parse(expression) in self.parse(array)
        if operator == '$setUnion':
            result = []
            for set_value in values:
                for value in self.parse(set_value):
                    if value not in result:
                        result.append(value)
            return result
        raise NotImplementedError(
            "Although '%s' is a valid set operator for the aggregation "
            'pipeline, it is currently not implemented in Mongomock.' % operator)


def _parse_expression(expression, doc_dict):
    """Parse an expression."""
    return _Parser(doc_dict).parse(expression)


def _accumulate_group(output_fields, group_list):
    doc_dict = {}
    for field, value in six.iteritems(output_fields):
        if field == '_id':
            continue
        for operator, key in six.iteritems(value):
            key_getter = functools.partial(_parse_expression, key)
            values = [key_getter(doc) for doc in group_list]
            if operator in _GROUPING_OPERATOR_MAP:
                doc_dict[field] = _GROUPING_OPERATOR_MAP[operator](values)
            elif operator == '$first':
                doc_dict[field] = values[0]
            elif operator == '$last':
                doc_dict[field] = values[-1]
            elif operator == '$addToSet':
                value = []
                val_it = (val or None for val in values)
                # Don't use set in case elt in not hashable (like dicts).
                for elt in val_it:
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
                    'Although %s is a valid group operator for the '
                    'aggregation pipeline, it is currently not implemented '
                    'in Mongomock.' % operator)
            else:
                raise NotImplementedError(
                    '%s is not a valid group operator for the aggregation '
                    'pipeline. See http://docs.mongodb.org/manual/meta/'
                    'aggregation-quick-reference/ for a complete list of '
                    'valid operators.' % operator)
    return doc_dict


def _fix_sort_key(key_getter):
    def fixed_getter(doc):
        key = key_getter(doc)
        # Convert dictionaries to make sorted() work in Python 3.
        if isinstance(key, dict):
            return [(k, v) for (k, v) in sorted(key.items())]
        return key
    return fixed_getter


def _handle_lookup_stage(in_collection, database, options):
    for operator in ('let', 'pipeline'):
        if operator in options:
            raise NotImplementedError(
                "Although '%s' is a valid lookup operator for the "
                'aggregation pipeline, it is currently not '
                'implemented in Mongomock.' % operator)
    for operator in ('from', 'localField', 'foreignField', 'as'):
        if operator not in options:
            raise OperationFailure(
                "Must specify '%s' field for a $lookup" % operator)
        if not isinstance(options[operator], six.string_types):
            raise OperationFailure(
                'Arguments to $lookup must be strings')
        if operator in ('as', 'localField', 'foreignField') and \
                options[operator].startswith('$'):
            raise OperationFailure(
                "FieldPath field names may not start with '$'")
        if operator in ('localField', 'as') and \
                '.' in options[operator]:
            raise NotImplementedError(
                "Although '.' is valid in the 'localField' and 'as' "
                'parameters for the lookup stage of the aggregation '
                'pipeline, it is currently not implemented in Mongomock.')

    foreign_name = options['from']
    local_field = options['localField']
    foreign_field = options['foreignField']
    local_name = options['as']
    foreign_collection = database.get_collection(foreign_name)
    for doc in in_collection:
        query = doc.get(local_field)
        if isinstance(query, list):
            query = {'$in': query}
        matches = foreign_collection.find({foreign_field: query})
        # TODO(pascal): Do not modify the input collection.
        doc[local_name] = [foreign_doc for foreign_doc in matches]

    return in_collection


def _handle_group_stage(in_collection, unused_database, options):
    grouped_collection = []
    _id = options['_id']
    if _id:

        def _key_getter(doc):
            try:
                return _parse_expression(_id, doc)
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
        group_list = ([x for x in group])
        doc_dict = _accumulate_group(options, group_list)
        doc_dict['_id'] = doc_id
        grouped_collection.append(doc_dict)

    return grouped_collection


def _handle_bucket_stage(in_collection, unused_database, options):
    unknown_options = set(options) - {'groupBy', 'boundaries', 'output', 'default'}
    if unknown_options:
        raise OperationFailure(
            'Unrecognized option to $bucket: %s.' % unknown_options.pop())
    if 'groupBy' not in options or 'boundaries' not in options:
        raise OperationFailure(
            "$bucket requires 'groupBy' and 'boundaries' to be specified.")
    group_by = options['groupBy']
    boundaries = options['boundaries']
    if not isinstance(boundaries, list):
        raise OperationFailure(
            "The $bucket 'boundaries' field must be an array, but found type: %s"
            % type(boundaries))
    if len(boundaries) < 2:
        raise OperationFailure(
            "The $bucket 'boundaries' field must have at least 2 values, but "
            'found %d value(s).' % len(boundaries))
    if sorted(boundaries) != boundaries:
        raise OperationFailure(
            "The 'boundaries' option to $bucket must be sorted in ascending order")
    output_fields = options.get('output', {'count': {'$sum': 1}})
    default_value = options.get('default', None)
    try:
        is_default_last = default_value >= boundaries[-1]
    except TypeError:
        is_default_last = True

    def _get_default_bucket():
        try:
            return options['default']
        except KeyError:
            raise OperationFailure(
                '$bucket could not find a matching branch for '
                'an input, and no default was specified.')

    def _get_bucket_id(doc):
        """Get the bucket ID for a document.

        Note that it actually returns a tuple with the first
        param being a sort key to sort the default bucket even
        if it's not the same type as the boundaries.
        """
        try:
            value = _parse_expression(group_by, doc)
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
    for (unused_key, doc_id), group in grouped:
        group_list = [kv[1] for kv in group]
        doc_dict = _accumulate_group(output_fields, group_list)
        doc_dict['_id'] = doc_id
        out_collection.append(doc_dict)
    return out_collection


def _handle_sample_stage(in_collection, unused_database, options):
    if not isinstance(options, dict):
        raise OperationFailure('the $sample stage specification must be an object')
    size = options.pop('size', None)
    if size is None:
        raise OperationFailure('$sample stage must specify a size')
    if options:
        raise OperationFailure('unrecognized option to $sample: %s' % set(options).pop())
    shuffled = list(in_collection)
    _random.shuffle(shuffled)
    return shuffled[:size]


def _handle_sort_stage(in_collection, unused_database, options):
    sort_array = reversed([{x: y} for x, y in options.items()])
    sorted_collection = in_collection
    for sort_pair in sort_array:
        for sortKey, sortDirection in sort_pair.items():
            sorted_collection = sorted(
                sorted_collection,
                key=lambda x: filtering.resolve_sort_key(sortKey, x),
                reverse=sortDirection < 0)
    return sorted_collection


def _handle_unwind_stage(in_collection, unused_database, options):
    if not isinstance(options, dict):
        options = {'path': options}
    path = options['path']
    if not isinstance(path, six.string_types) or path[0] != '$':
        raise ValueError(
            '$unwind failed: exception: field path references must be prefixed '
            "with a '$' '%s'" % path)
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
                    ' Cannot specify both %s and %s: %s' % (
                        repr(prefix + field), repr(prefix + other_key), original_filter))
            filter_dict[field] = 1
            continue
        if not isinstance(filter_dict.get(field, []), list):
            raise OperationFailure(
                'Invalid $project :: caused by :: specification contains two conflicting paths.'
                ' Cannot specify both %s and %s: %s' % (
                    repr(prefix + field), repr(prefix + key), original_filter))
        filter_dict[field] = filter_dict.get(field, []) + [subkey]

    return collections.OrderedDict(
        (k, _combine_projection_spec(v, original_filter, prefix='%s%s.' % (prefix, k)))
        for k, v in six.iteritems(filter_dict)
    )


def _project_by_spec(doc, proj_spec, is_include):
    output = {}
    for key, value in six.iteritems(doc):
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
        elif not is_include:
            output[key] = value

    return output


def _handle_project_stage(in_collection, unused_database, options):
    filter_list = []
    method = None
    include_id = options.get('_id')
    # Compute new values for each field, except inclusion/exclusions that are
    # handled in one final step.
    new_fields_collection = None
    for field, value in six.iteritems(options):
        if method is None and (field != '_id' or value):
            method = 'include' if value else 'exclude'
        elif method == 'include' and not value and field != '_id':
            raise OperationFailure(
                'Bad projection specification, cannot exclude fields '
                "other than '_id' in an inclusion projection: %s" % options)
        elif method == 'exclude' and value:
            raise OperationFailure(
                'Bad projection specification, cannot include fields '
                'or add computed fields during an exclusion projection: %s' % options)
        if field == '_id':
            continue
        if value in (0, 1, True, False):
            filter_list.append(field)
            continue
        if not new_fields_collection:
            new_fields_collection = [{} for unused_doc in in_collection]

        for in_doc, out_doc in zip(in_collection, new_fields_collection):
            try:
                out_doc[field] = _parse_expression(value, in_doc)
            except KeyError:
                pass
    if (method == 'include') == (include_id is not False and include_id is not 0):
        filter_list.append('_id')

    if not filter_list:
        return new_fields_collection

    # Final steps: include or exclude fields and merge with newly created fields.
    projection_spec = _combine_projection_spec(filter_list, original_filter=options)
    out_collection = [
        _project_by_spec(doc, projection_spec, is_include=(method == 'include'))
        for doc in in_collection
    ]
    if new_fields_collection:
        return [
            dict(a, **b)
            for a, b in zip(out_collection, new_fields_collection)
        ]
    return out_collection


def _handle_out_stage(in_collection, database, options):
    # TODO(MetrodataTeam): should leave the origin collection unchanged
    out_collection = database.get_collection(options)
    if out_collection.count() > 0:
        out_collection.drop()
    out_collection.insert_many(in_collection)
    return in_collection


def _handle_count_stage(in_collection, database, options):
    if not isinstance(options, str) or options == '':
        raise OperationFailure('the count field must be a non-empty string')
    elif options.startswith('$'):
        raise OperationFailure('the count field cannot be a $-prefixed path')
    elif '.' in options:
        raise OperationFailure("the count field cannot contain '.'")
    return [{options: len(in_collection)}]


_PIPELINE_HANDLERS = {
    '$addFields': None,
    '$bucket': _handle_bucket_stage,
    '$bucketAuto': None,
    '$collStats': None,
    '$count': _handle_count_stage,
    '$currentOp': None,
    '$facet': None,
    '$geoNear': None,
    '$graphLookup': None,
    '$group': _handle_group_stage,
    '$indexStats': None,
    '$limit': lambda c, d, o: c[:o],
    '$listLocalSessions': None,
    '$listSessions': None,
    '$lookup': _handle_lookup_stage,
    '$match': lambda c, d, o: [doc for doc in c if filtering.filter_applies(o, doc)],
    '$out': _handle_out_stage,
    '$project': _handle_project_stage,
    '$redact': None,
    '$replaceRoot': None,
    '$sample': _handle_sample_stage,
    '$skip': lambda c, d, o: c[o:],
    '$sort': _handle_sort_stage,
    '$sortByCount': None,
    '$unwind': _handle_unwind_stage,
}


def process_pipeline(collection, database, pipeline, session):
    if session:
        raise NotImplementedError('Mongomock does not handle sessions yet')

    for stage in pipeline:
        for operator, options in six.iteritems(stage):
            try:
                handler = _PIPELINE_HANDLERS[operator]
            except KeyError:
                raise NotImplementedError(
                    '%s is not a valid operator for the aggregation pipeline. '
                    'See http://docs.mongodb.org/manual/meta/aggregation-quick-reference/ '
                    'for a complete list of valid operators.' % operator)
            if not handler:
                raise NotImplementedError(
                    "Although '%s' is a valid operator for the aggregation pipeline, it is "
                    'currently not implemented in Mongomock.' % operator)
            collection = handler(collection, database, options)

    return command_cursor.CommandCursor(collection)
