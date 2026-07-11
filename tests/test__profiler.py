"""Tests for mongomock_ng.profiler — QueryProfiler, normalize_predicate, coverage analysis."""
# ruff: noqa: S603 — subprocess with sys.executable + hardcoded strings is safe in tests

import json
import os
import subprocess
import sys
import tempfile

from mongomock_ng.profiler import _analyze_sort_coverage
from mongomock_ng.profiler import _extract_query_fields
from mongomock_ng.profiler import _matches_partial
from mongomock_ng.profiler import get_profiler
from mongomock_ng.profiler import normalize_predicate
from mongomock_ng.profiler import QueryProfiler
from mongomock_ng.profiler import QueryRecord


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _fresh_profiler():
    """Return a clean QueryProfiler (not the singleton)."""
    p = QueryProfiler()
    p._index_snapshots = {}
    p._records = []
    p._enabled = False
    p._output_path = None
    return p


# ---------------------------------------------------------------------------
# normalize_predicate
# ---------------------------------------------------------------------------


class TestNormalizePredicate:
    def test_basic_value_replacement(self):
        assert normalize_predicate({'x': 1}) == {'x': '?'}

    def test_multiple_fields(self):
        assert normalize_predicate({'x': 1, 'y': 'abc', 'z': None}) == {
            'x': '?',
            'y': '?',
            'z': '?',
        }

    def test_nested_dict(self):
        assert normalize_predicate({'x': {'y': 1}}) == {'x': {'y': '?'}}

    def test_deeply_nested(self):
        assert normalize_predicate({'a': {'b': {'c': 42}}}) == {'a': {'b': {'c': '?'}}}

    def test_logical_or(self):
        result = normalize_predicate({'$or': [{'x': 1}, {'y': 2}]})
        assert result == {'$or': [{'x': '?'}, {'y': '?'}]}

    def test_logical_and(self):
        result = normalize_predicate({'$and': [{'a': 10}, {'b': 20}]})
        assert result == {'$and': [{'a': '?'}, {'b': '?'}]}

    def test_empty_dict(self):
        assert normalize_predicate({}) == {}

    def test_list_values(self):
        assert normalize_predicate({'x': [1, 2, 3]}) == {'x': ['?', '?', '?']}

    def test_empty_list(self):
        assert normalize_predicate({'x': []}) == {'x': []}

    def test_boolean_values(self):
        assert normalize_predicate({'active': True, 'deleted': False}) == {
            'active': '?',
            'deleted': '?',
        }

    def test_operator_filter(self):
        assert normalize_predicate({'age': {'$gt': 18}}) == {'age': {'$gt': '?'}}

    def test_compound_operators(self):
        inp = {'price': {'$gte': 10, '$lte': 100}}
        expected = {'price': {'$gte': '?', '$lte': '?'}}
        assert normalize_predicate(inp) == expected

    def test_mixed_logical_and_field(self):
        inp = {'status': 'active', '$or': [{'x': 1}, {'y': 2}]}
        expected = {'status': '?', '$or': [{'x': '?'}, {'y': '?'}]}
        assert normalize_predicate(inp) == expected

    def test_non_dict_non_list(self):
        assert normalize_predicate(42) == '?'
        assert normalize_predicate('hello') == '?'
        assert normalize_predicate(None) == '?'
        assert normalize_predicate([1, [2, 3]]) == ['?', ['?', '?']]


# ---------------------------------------------------------------------------
# _extract_query_fields  (internal helper)
# ---------------------------------------------------------------------------


class TestExtractQueryFields:
    def test_simple_field(self):
        assert _extract_query_fields({'x': '?'}) == {'x'}

    def test_multiple_fields(self):
        assert _extract_query_fields({'x': '?', 'y': '?'}) == {'x', 'y'}

    def test_logical_or(self):
        assert _extract_query_fields({'$or': [{'x': '?'}, {'y': '?'}]}) == {'x', 'y'}

    def test_logical_and(self):
        assert _extract_query_fields({'$and': [{'a': '?'}, {'b': '?'}]}) == {'a', 'b'}

    def test_operator_filter(self):
        assert _extract_query_fields({'age': {'$gt': '?'}}) == {'age'}

    def test_empty_dict(self):
        assert _extract_query_fields({}) == set()

    def test_mixed_fields_and_logical(self):
        result = _extract_query_fields({'status': '?', '$or': [{'x': '?'}]})
        assert result == {'status', 'x'}

    def test_nested_object_field(self):
        result = _extract_query_fields({'x': {'nested': {'deep': '?'}}})
        assert result == {'x', 'deep'}


# ---------------------------------------------------------------------------
# _analyze_sort_coverage  (internal helper)
# ---------------------------------------------------------------------------


class TestAnalyzeSortCoverage:
    def test_none_when_sort_empty(self):
        assert _analyze_sort_coverage(None, {}) == 'none'

    def test_none_when_sort_empty_list(self):
        assert _analyze_sort_coverage([], {}) == 'none'

    def test_full_single_field_match(self):
        idx = {'a_1': {'key': [('a', 1)]}}
        assert _analyze_sort_coverage([('a', 1)], idx) == 'full'

    def test_full_single_field_reverse_direction(self):
        idx = {'a_1': {'key': [('a', 1)]}}
        assert _analyze_sort_coverage([('a', -1)], idx) == 'full'

    def test_full_compound_prefix_match(self):
        idx = {'a_b_1': {'key': [('a', 1), ('b', 1)]}}
        assert _analyze_sort_coverage([('a', 1)], idx) == 'full'

    def test_full_compound_full_match(self):
        idx = {'a_b_1': {'key': [('a', 1), ('b', 1)]}}
        assert _analyze_sort_coverage([('a', 1), ('b', 1)], idx) == 'full'

    def test_full_compound_full_match_reverse_direction(self):
        idx = {'a_b_1': {'key': [('a', 1), ('b', 1)]}}
        assert _analyze_sort_coverage([('a', -1), ('b', -1)], idx) == 'full'

    def test_partial_field_in_compound_not_prefix(self):
        idx = {'a_b_1': {'key': [('a', 1), ('b', 1)]}}
        assert _analyze_sort_coverage([('b', 1)], idx) == 'partial'

    def test_partial_field_in_larger_compound(self):
        idx = {'a_b_c_1': {'key': [('a', 1), ('b', 1), ('c', 1)]}}
        assert _analyze_sort_coverage([('b', 1)], idx) == 'partial'

    def test_none_field_not_in_index(self):
        idx = {'a_1': {'key': [('a', 1)]}}
        assert _analyze_sort_coverage([('b', 1)], idx) == 'none'

    def test_none_index_too_short(self):
        idx = {'a_1': {'key': [('a', 1)]}}
        assert _analyze_sort_coverage([('a', 1), ('b', 1)], idx) == 'none'

    def test_none_no_indexes(self):
        assert _analyze_sort_coverage([('a', 1)], {}) == 'none'

    def test_full_with_multiple_indexes_first_hit(self):
        idx = {
            'a_1': {'key': [('a', 1)]},
            'b_1': {'key': [('b', 1)]},
        }
        assert _analyze_sort_coverage([('a', 1)], idx) == 'full'

    def test_handles_sort_field_variants(self):
        """Verify sort tuples can be passed as lists of tuples or lists."""
        idx = {'a_1': {'key': [('a', 1)]}}
        assert _analyze_sort_coverage([['a', 1]], idx) == 'full'


# ---------------------------------------------------------------------------
# _matches_partial  (internal helper)
# ---------------------------------------------------------------------------


class TestMatchesPartial:
    def test_simple_eq_match(self):
        assert _matches_partial({'status': 'active'}, {'status': 'active'}) is True

    def test_simple_eq_no_match(self):
        assert _matches_partial({'status': 'inactive'}, {'status': 'active'}) is False

    def test_missing_key_no_match(self):
        assert _matches_partial({'x': 1}, {'status': 'active'}) is False

    def test_operator_eq_match(self):
        assert _matches_partial({'qty': 100}, {'qty': {'$eq': 100}}) is True

    def test_operator_eq_no_match(self):
        assert _matches_partial({'qty': 50}, {'qty': {'$eq': 100}}) is False

    def test_operator_exists_true_match(self):
        assert _matches_partial({'field': 'anything'}, {'field': {'$exists': True}}) is True

    def test_operator_exists_true_no_match(self):
        assert _matches_partial({}, {'field': {'$exists': True}}) is False  # actual_val is None

    def test_operator_gte_match(self):
        assert _matches_partial({'qty': 150}, {'qty': {'$gte': 100}}) is True

    def test_operator_gte_no_match(self):
        assert _matches_partial({'qty': 50}, {'qty': {'$gte': 100}}) is False

    def test_operator_lte_match(self):
        assert _matches_partial({'qty': 50}, {'qty': {'$lte': 100}}) is True

    def test_operator_lte_no_match(self):
        assert _matches_partial({'qty': 150}, {'qty': {'$lte': 100}}) is False

    def test_operator_gt_match(self):
        assert _matches_partial({'qty': 101}, {'qty': {'$gt': 100}}) is True

    def test_operator_gt_no_match(self):
        assert _matches_partial({'qty': 100}, {'qty': {'$gt': 100}}) is False

    def test_operator_lt_match(self):
        assert _matches_partial({'qty': 99}, {'qty': {'$lt': 100}}) is True

    def test_operator_lt_no_match(self):
        assert _matches_partial({'qty': 100}, {'qty': {'$lt': 100}}) is False

    def test_operator_in_match(self):
        res = _matches_partial({'status': 'active'}, {'status': {'$in': ['active', 'pending']}})
        assert res is True

    def test_operator_in_no_match(self):
        res = _matches_partial({'status': 'deleted'}, {'status': {'$in': ['active', 'pending']}})
        assert res is False

    def test_operator_ne_match(self):
        assert _matches_partial({'status': 'active'}, {'status': {'$ne': 'deleted'}}) is True

    def test_operator_ne_no_match(self):
        assert _matches_partial({'status': 'deleted'}, {'status': {'$ne': 'deleted'}}) is False

    def test_operator_nin_match(self):
        res = _matches_partial({'status': 'archived'}, {'status': {'$nin': ['active', 'pending']}})
        assert res is True

    def test_operator_nin_no_match(self):
        res = _matches_partial({'status': 'active'}, {'status': {'$nin': ['active', 'pending']}})
        assert res is False

    def test_multiple_conditions_all_match(self):
        expr = {'status': 'active', 'qty': {'$gte': 10}}
        assert _matches_partial({'status': 'active', 'qty': 50}, expr) is True

    def test_multiple_conditions_one_fails(self):
        expr = {'status': 'active', 'qty': {'$gte': 10}}
        assert _matches_partial({'status': 'active', 'qty': 5}, expr) is False

    def test_operator_exists_match(self):
        assert _matches_partial({'status': 'active'}, {'status': {'$exists': True}}) is True

    def test_operator_exists_no_match(self):
        assert _matches_partial({}, {'status': {'$exists': True}}) is False

    def test_operator_exists_false_match(self):
        assert _matches_partial({}, {'status': {'$exists': False}}) is True

    def test_operator_exists_false_field_present(self):
        assert _matches_partial({'status': 'active'}, {'status': {'$exists': False}}) is False


# ---------------------------------------------------------------------------
# QueryProfiler — record / records / enabled state
# ---------------------------------------------------------------------------


class TestRecordAndRecords:
    def test_record_creates_record_when_enabled(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        assert len(p.records) == 1

    def test_record_stores_correct_fields(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1, 'y': 'hello'}, 'my_coll', 'find')
        rec = p.records[0]
        assert rec.collection == 'my_coll'
        assert rec.operation == 'find'
        assert rec.filter == {'x': 1, 'y': 'hello'}
        assert rec.predicate == {'x': '?', 'y': '?'}

    def test_record_stores_sort(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll', 'find', sort=[('x', 1)])
        assert p.records[0].sort == [('x', 1)]

    def test_record_sort_is_none_by_default(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll', 'find')
        assert p.records[0].sort is None

    def test_record_not_created_when_not_enabled(self):
        p = _fresh_profiler()
        # never started
        p.record({'x': 1}, 'coll', 'find')
        assert len(p.records) == 0

    def test_record_not_created_after_stop(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll', 'find')
        p.stop()
        p.record({'x': 2}, 'coll', 'find')
        assert len(p.records) == 1  # only the first one

    def test_records_returns_copy(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll', 'find')
        r1 = p.records
        r2 = p.records
        assert r1 is not r2  # different list objects

    def test_test_name_contains_test_module_and_function(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll', 'find')
        rec = p.records[0]
        assert rec.test_name == (
            'tests.test__profiler.'
            'TestRecordAndRecords.'
            'test_test_name_contains_test_module_and_function'
        )

    def test_record_multiple_operations(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll', 'find')
        p.record({'x': 2}, 'coll', 'update')
        assert len(p.records) == 2
        assert p.records[0].operation == 'find'
        assert p.records[1].operation == 'update'

    def test_record_multiple_collections(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'orders', 'find')
        p.record({'y': 2}, 'users', 'find')
        assert len(p.records) == 2
        assert p.records[0].collection == 'orders'
        assert p.records[1].collection == 'users'

    def test_test_name_from_standalone_function(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll', 'find')
        rec = p.records[0]
        assert 'test_test_name_from_standalone_function' in rec.test_name


# ---------------------------------------------------------------------------
# QueryProfiler — index snapshots
# ---------------------------------------------------------------------------


class TestIndexSnapshots:
    def test_first_record_per_collection_stores_snapshot(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'_id_': {'key': [('_id', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes)
        assert 'coll1' in p._index_snapshots
        assert p._index_snapshots['coll1'] == indexes

    def test_second_record_does_not_overwrite_snapshot(self):
        p = _fresh_profiler()
        p.start()
        first_idx = {'_id_': {'key': [('_id', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=first_idx)
        second_idx = {'_id_': {'key': [('_id', 1)], 'extra': True}}
        p.record({'x': 2}, 'coll1', 'find', indexes=second_idx)
        assert p._index_snapshots['coll1'] == first_idx

    def test_multiple_collections_each_get_snapshot(self):
        p = _fresh_profiler()
        p.start()
        idx_a = {'_id_': {'key': [('_id', 1)]}}
        idx_b = {'_id_': {'key': [('_id', 1)]}, 'name_1': {'key': [('name', 1)]}}
        p.record({'x': 1}, 'coll_a', 'find', indexes=idx_a)
        p.record({'y': 2}, 'coll_b', 'find', indexes=idx_b)
        assert p._index_snapshots['coll_a'] == idx_a
        assert p._index_snapshots['coll_b'] == idx_b

    def test_no_snapshot_when_indexes_not_provided(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        assert 'coll1' not in p._index_snapshots


# ---------------------------------------------------------------------------
# QueryProfiler — report structure
# ---------------------------------------------------------------------------


class TestReportStructure:
    def test_metadata_total_queries(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        p.record({'y': 2}, 'coll1', 'find')
        report = p.report()
        assert report['metadata']['total_queries'] == 2

    def test_collections_keyed_by_name(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'orders', 'find')
        p.record({'y': 2}, 'users', 'find')
        report = p.report()
        assert 'orders' in report['collections']
        assert 'users' in report['collections']

    def test_patterns_grouped_by_predicate(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        p.record({'x': 1}, 'coll1', 'find')  # same predicate -> same group
        p.record({'y': 2}, 'coll1', 'find')  # different predicate -> different group
        report = p.report()
        patterns = report['collections']['coll1']['patterns']
        assert len(patterns) == 2
        # find the pattern with predicate {'x': '?'}
        x_pattern = next(pat for pat in patterns if pat['predicate'] == {'x': '?'})
        assert x_pattern['count'] == 2

    def test_report_with_no_records(self):
        p = _fresh_profiler()
        report = p.report()
        assert report['metadata']['total_queries'] == 0
        assert report['collections'] == {}

    def test_empty_collections_skipped(self):
        p = _fresh_profiler()
        report = p.report()
        assert report['collections'] == {}

    def test_report_includes_indexes(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'_id_': {'key': [('_id', 1)]}, 'status_1': {'key': [('status', 1)]}}
        p.record({'status': 'active'}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        coll = report['collections']['coll1']
        idx_names = [i['name'] for i in coll['indexes']]
        assert '_id_' in idx_names
        assert 'status_1' in idx_names

    def test_index_extra_properties_in_report(self):
        p = _fresh_profiler()
        p.start()
        indexes = {
            'unique_idx': {
                'key': [('email', 1)],
                'unique': True,
                'sparse': True,
            },
            'ttl_idx': {
                'key': [('created_at', 1)],
                'expireAfterSeconds': 3600,
            },
            'partial_idx': {
                'key': [('status', 1)],
                'partialFilterExpression': {'status': 'active'},
            },
        }
        p.record({'status': 'active'}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        idx_list = report['collections']['coll1']['indexes']
        idx_map = {i['name']: i for i in idx_list}
        assert idx_map['unique_idx'].get('unique') is True
        assert idx_map['unique_idx'].get('sparse') is True
        assert idx_map['ttl_idx'].get('expireAfterSeconds') == 3600
        assert idx_map['partial_idx'].get('partialFilterExpression') == {'status': 'active'}


# ---------------------------------------------------------------------------
# QueryProfiler — filter_coverage analysis
# ---------------------------------------------------------------------------


class TestFilterCoverage:
    def test_full_when_all_fields_indexed(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'_id_': {'key': [('_id', 1)]}, 'x_1': {'key': [('x', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['filter_coverage'] == 'full'

    def test_full_multiple_fields_all_indexed(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'x_y_1': {'key': [('x', 1), ('y', 1)]}}
        p.record({'x': 1, 'y': 2}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['filter_coverage'] == 'full'

    def test_partial_when_some_fields_indexed(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'x_1': {'key': [('x', 1)]}}
        p.record({'x': 1, 'y': 2}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['filter_coverage'] == 'partial'

    def test_none_when_no_indexes(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['filter_coverage'] == 'none'

    def test_none_when_fields_not_in_index(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'_id_': {'key': [('_id', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['filter_coverage'] == 'none'

    def test_empty_predicate_is_none(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'x_1': {'key': [('x', 1)]}}
        p.record({}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['filter_coverage'] == 'none'

    def test_covered_fields_and_uncovered_fields(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'x_1': {'key': [('x', 1)]}}
        p.record({'x': 1, 'y': 2}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['covered_fields'] == ['x']
        assert pat['uncovered_fields'] == ['y']

    def test_matches_partial_filter_expression(self):
        p = _fresh_profiler()
        p.start()
        indexes = {
            'active_orders': {
                'key': [('status', 1), ('qty', 1)],
                'partialFilterExpression': {'status': 'active'},
            }
        }
        # filter matches partialFilterExpression -> index counts
        p.record({'status': 'active', 'qty': 100}, 'orders', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['orders']['patterns'][0]
        assert pat['filter_coverage'] == 'full'

    def test_partial_filter_expression_not_matched_still_counts_current_behavior(self):
        """Current behavior: partialFilterExpression mismatch does NOT
        exclude the index fields from coverage (covered is updated before
        the match check).  This test documents the current behavior."""
        p = _fresh_profiler()
        p.start()
        indexes = {
            'active_orders': {
                'key': [('status', 1), ('qty', 1)],
                'partialFilterExpression': {'status': 'active'},
            }
        }
        # filter does NOT match partialFilterExpression
        p.record({'status': 'inactive', 'qty': 100}, 'orders', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['orders']['patterns'][0]
        # The index still contributes its fields to covered because
        # covered.update(match) happens before the partial expr check.
        assert pat['filter_coverage'] == 'full'

    def test_covering_index_reported(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'x_1': {'key': [('x', 1)]}, 'y_1': {'key': [('y', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['covering_index'] == 'x_1'

    def test_covering_index_first_match_wins(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'idx_a': {'key': [('x', 1)]}, 'idx_b': {'key': [('x', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['covering_index'] == 'idx_a'

    def test_covering_index_none_when_no_match(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'_id_': {'key': [('_id', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['covering_index'] is None

    def test_filter_coverage_via_logical_operators(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'x_1': {'key': [('x', 1)]}}
        p.record({'$or': [{'x': 1}, {'x': 2}]}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        # $or is skipped by _extract_query_fields, fields from inside are extracted
        # so {'x'} should be identified as query field and be covered
        assert pat['filter_coverage'] == 'full'


# ---------------------------------------------------------------------------
# QueryProfiler — sort_coverage (via report)
# ---------------------------------------------------------------------------


class TestSortCoverageViaReport:
    def test_sort_coverage_full(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'a_1': {'key': [('a', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes, sort=[('a', 1)])
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['sort_coverage'] == 'full'

    def test_sort_coverage_partial(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'a_b_1': {'key': [('a', 1), ('b', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes, sort=[('b', 1)])
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['sort_coverage'] == 'partial'

    def test_sort_coverage_none(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'a_1': {'key': [('a', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes, sort=[('b', 1)])
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['sort_coverage'] == 'none'

    def test_sort_coverage_none_when_sort_not_provided(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'a_1': {'key': [('a', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes)
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['sort_coverage'] == 'none'

    def test_sort_in_report(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'a_1': {'key': [('a', 1)]}}
        p.record({'x': 1}, 'coll1', 'find', indexes=indexes, sort=[('a', 1)])
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert pat['sort'] == [('a', 1)]

    def test_sort_summary_counts(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'a_1': {'key': [('a', 1)]}}
        # Use different predicates so they are separate pattern groups.
        p.record({'x': 1}, 'c1', 'find', indexes=indexes, sort=[('a', 1)])  # full
        p.record({'y': 2}, 'c2', 'find', indexes=indexes, sort=[('a', 1)])  # full
        p.record({'z': 3}, 'c1', 'find', indexes=indexes, sort=[('b', 1)])  # none
        report = p.report()
        sort_summary = report['metadata']['sort_coverage_summary']
        assert sort_summary['full'] == 2
        assert sort_summary['none'] == 1


# ---------------------------------------------------------------------------
# QueryProfiler — report metadata summary
# ---------------------------------------------------------------------------


class TestReportMetadata:
    def test_filter_coverage_summary(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'x_1': {'key': [('x', 1)]}}
        p.record({'x': 1}, 'c1', 'find', indexes=indexes)  # full
        p.record({'x': 1, 'y': 2}, 'c1', 'find', indexes=indexes)  # partial
        p.record({'z': 3}, 'c1', 'find', indexes=indexes)  # none
        report = p.report()
        fcs = report['metadata']['filter_coverage_summary']
        assert fcs['full'] == 1
        assert fcs['partial'] == 1
        assert fcs['none'] == 1

    def test_total_patterns(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'c1', 'find')
        p.record({'x': 1}, 'c1', 'find')  # same pattern
        p.record({'y': 2}, 'c1', 'find')  # different pattern
        report = p.report()
        assert report['metadata']['total_patterns'] == 2

    def test_patterns_sorted_by_count_desc(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'c1', 'find')
        p.record({'x': 1}, 'c1', 'find')
        p.record({'x': 1}, 'c1', 'find')
        p.record({'y': 2}, 'c1', 'find')
        report = p.report()
        patterns = report['collections']['c1']['patterns']
        counts = [pat['count'] for pat in patterns]
        assert counts == sorted(counts, reverse=True)


# ---------------------------------------------------------------------------
# QueryProfiler — start / stop / reset
# ---------------------------------------------------------------------------


class TestStateTransitions:
    def test_start_clears_records(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'c1', 'find')
        assert len(p.records) == 1
        p.start()  # restart clears
        assert len(p.records) == 0

    def test_stop_disables(self):
        p = _fresh_profiler()
        assert p.enabled is False
        p.start()
        assert p.enabled is True
        p.stop()
        assert p.enabled is False

    def test_reset_clears_records(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'c1', 'find')
        p.reset()
        assert len(p.records) == 0

    def test_reset_does_not_clear_snapshots_or_disabled(self):
        """reset() only clears records, does not affect enabled or snapshots."""
        p = _fresh_profiler()
        p.start()
        indexes = {'x_1': {'key': [('x', 1)]}}
        p.record({'x': 1}, 'c1', 'find', indexes=indexes)
        p.reset()
        # snapshots survive reset
        assert 'c1' in p._index_snapshots
        # enabled state survives reset
        assert p.enabled is True

    def test_enabled_property(self):
        p = _fresh_profiler()
        assert p.enabled is False
        p.start()
        assert p.enabled is True
        p.stop()
        assert p.enabled is False

    def test_start_with_custom_output_path(self):
        p = _fresh_profiler()
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            custom_path = f.name
            os.unlink(custom_path)
        p.start(custom_path)
        assert p._output_path == custom_path


# ---------------------------------------------------------------------------
# QueryProfiler — export_json
# ---------------------------------------------------------------------------


class TestExportJson:
    def test_writes_valid_json_file(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            outpath = f.name
        try:
            p.export_json(outpath)
            with open(outpath) as f:
                data = json.load(f)
            assert 'metadata' in data
            assert 'collections' in data
            assert data['metadata']['total_queries'] == 1
        finally:
            os.unlink(outpath)

    def test_export_json_default_filename(self):
        """When no path given and _output_path is set, use _output_path."""
        p = _fresh_profiler()
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            default_path = f.name
            os.unlink(default_path)
        p.start(output_path=default_path)
        p.record({'x': 1}, 'coll1', 'find')
        try:
            p.export_json()
            with open(default_path) as f:
                data = json.load(f)
            assert data['metadata']['total_queries'] == 1
        finally:
            if os.path.exists(default_path):
                os.unlink(default_path)

    def test_export_json_path_overrides_default(self):
        p = _fresh_profiler()
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            ignored_path = f.name
            os.unlink(ignored_path)
        p.start(output_path=ignored_path)
        p.record({'x': 1}, 'coll1', 'find')
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            outpath = f.name
        try:
            p.export_json(path=outpath)
            assert os.path.exists(outpath)
            assert not os.path.exists(ignored_path)
        finally:
            if os.path.exists(outpath):
                os.unlink(outpath)
            if os.path.exists(ignored_path):
                os.unlink(ignored_path)

    def test_export_json_report_content(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'orders', 'find')
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            outpath = f.name
        try:
            p.export_json(outpath)
            with open(outpath) as f:
                data = json.load(f)
            assert 'orders' in data['collections']
        finally:
            os.unlink(outpath)


# ---------------------------------------------------------------------------
# QueryProfiler — auto_export
# ---------------------------------------------------------------------------


class TestAutoExport:
    def test_writes_json_if_records_exist(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            outpath = f.name
        p._output_path = outpath
        p.auto_export()
        try:
            assert os.path.exists(outpath)
            with open(outpath) as f:
                data = json.load(f)
            assert data['metadata']['total_queries'] == 1
        finally:
            os.unlink(outpath)

    def test_does_not_write_if_no_records(self):
        p = _fresh_profiler()
        p.start()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            outpath = f.name
        os.unlink(outpath)  # remove so we can check it stays gone
        p._output_path = outpath
        p.auto_export()
        # file should NOT have been created
        assert not os.path.exists(outpath)

    def test_auto_export_no_output_path_set(self):
        """auto_export with no output_path set and no records: no crash."""
        p = _fresh_profiler()
        p.start()
        p.auto_export()  # should be a no-op


# ---------------------------------------------------------------------------
# get_profiler singleton
# ---------------------------------------------------------------------------


class TestGetProfiler:
    def test_returns_same_instance(self):
        p1 = get_profiler()
        p2 = get_profiler()
        assert p1 is p2

    def test_is_query_profiler(self):
        p = get_profiler()
        assert isinstance(p, QueryProfiler)


# ---------------------------------------------------------------------------
# QueryRecord dataclass
# ---------------------------------------------------------------------------


class TestQueryRecord:
    def test_default_sort_is_none(self):
        r = QueryRecord(collection='c', operation='find', test_name='t', filter={}, predicate={})
        assert r.sort is None

    def test_all_fields_stored(self):
        r = QueryRecord(
            collection='orders',
            operation='find',
            test_name='test_foo',
            filter={'x': 1},
            predicate={'x': '?'},
            sort=[('x', 1)],
        )
        assert r.collection == 'orders'
        assert r.operation == 'find'
        assert r.test_name == 'test_foo'
        assert r.filter == {'x': 1}
        assert r.predicate == {'x': '?'}
        assert r.sort == [('x', 1)]

    def test_equality(self):
        r1 = QueryRecord('c', 'find', 't', {}, {})
        r2 = QueryRecord('c', 'find', 't', {}, {})
        assert r1 == r2

    def test_repr(self):
        r = QueryRecord('c', 'find', 't', {'x': 1}, {'x': '?'})
        rep = repr(r)
        assert 'QueryRecord' in rep
        assert 'c' in rep


# ---------------------------------------------------------------------------
# Edge cases: multiple records, same predicate grouped together
# ---------------------------------------------------------------------------


class TestPatternGrouping:
    def test_same_predicate_different_filters_grouped(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        p.record({'x': 999}, 'coll1', 'find')  # same predicate, different value
        report = p.report()
        assert len(report['collections']['coll1']['patterns']) == 1
        assert report['collections']['coll1']['patterns'][0]['count'] == 2

    def test_different_predicates_separate_groups(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        p.record({'y': 2}, 'coll1', 'find')
        report = p.report()
        assert len(report['collections']['coll1']['patterns']) == 2

    def test_pattern_tests_and_operations_recorded(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        p.record({'x': 2}, 'coll1', 'update')
        report = p.report()
        pat = report['collections']['coll1']['patterns'][0]
        assert 'find' in pat['operations']
        assert 'update' in pat['operations']
        assert len(pat['tests']) >= 1


# ---------------------------------------------------------------------------
# Edge cases: no indexes in report
# ---------------------------------------------------------------------------


class TestReportNoIndexes:
    def test_collection_without_indexes_reports_empty_list(self):
        p = _fresh_profiler()
        p.start()
        p.record({'x': 1}, 'coll1', 'find')
        report = p.report()
        assert report['collections']['coll1']['indexes'] == []

    def test_mixed_collections_some_with_snapshots(self):
        p = _fresh_profiler()
        p.start()
        indexes = {'_id_': {'key': [('_id', 1)]}}
        p.record({'a': 1}, 'with_idx', 'find', indexes=indexes)
        p.record({'b': 2}, 'no_idx', 'find')
        report = p.report()
        assert len(report['collections']['with_idx']['indexes']) == 1
        assert report['collections']['no_idx']['indexes'] == []


# ---------------------------------------------------------------------------
# Edge cases: complex filters with operators
# ---------------------------------------------------------------------------


class TestOperatorFilters:
    def test_gt_filter_predicate(self):
        p = _fresh_profiler()
        p.start()
        p.record({'age': {'$gt': 18}}, 'coll1', 'find')
        rec = p.records[0]
        assert rec.predicate == {'age': {'$gt': '?'}}

    def test_multiple_operators_predicate(self):
        p = _fresh_profiler()
        p.start()
        p.record({'price': {'$gte': 10, '$lte': 100}}, 'coll1', 'find')
        rec = p.records[0]
        assert rec.predicate == {'price': {'$gte': '?', '$lte': '?'}}

    def test_in_operator_predicate(self):
        p = _fresh_profiler()
        p.start()
        p.record({'status': {'$in': ['a', 'b']}}, 'coll1', 'find')
        rec = p.records[0]
        # normalize_predicate replaces each list element with '?'.
        assert rec.predicate == {'status': {'$in': ['?', '?']}}


# ---------------------------------------------------------------------------
# Module-level env activation
# ---------------------------------------------------------------------------


class TestModuleLevelActivation:
    def test_env_activates_profiler(self):
        code = (
            'import os; '
            "os.environ['MONGOMOCK_PROFILER'] = '1'; "
            'from mongomock_ng.profiler import get_profiler; '
            'assert get_profiler().enabled'
        )
        result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr

    def test_env_disabled_by_default(self):
        code = 'from mongomock_ng.profiler import get_profiler; assert not get_profiler().enabled'
        result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Standalone test function (not a method) — covers profiler.py lines 74-76
# ---------------------------------------------------------------------------


def test__standalone_profiler_recording():
    p = QueryProfiler()
    p.start()
    p.record({'x': 1}, 'coll', 'find')
    rec = p.records[0]
    assert 'test__standalone_profiler_recording' in rec.test_name


def test__unknown_fallback():
    code = (
        'from mongomock_ng.profiler import _test_name_from_stack; '
        'name = _test_name_from_stack(); '
        'assert name == "<unknown>", name'
    )
    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
