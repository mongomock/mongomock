import atexit
import inspect
import os
from dataclasses import dataclass
from typing import Any


_PLACEHOLDER = '?'
_RECURSIVE_OPS = frozenset({'$and', '$or', '$nor', '$not'})
_LOGICAL_OPS = frozenset({'$and', '$or', '$nor', '$not', '$expr', '$where'})


@dataclass
class QueryRecord:
    collection: str
    operation: str
    test_name: str
    filter: dict
    predicate: dict
    sort: list | None = None


def normalize_predicate(filter: Any) -> Any:
    if isinstance(filter, dict):
        # top-level implicit $eq
        return {k: normalize_predicate(v) for k, v in filter.items()}

    if isinstance(filter, list):
        return [normalize_predicate(v) for v in filter]

    return _PLACEHOLDER


def _extract_query_fields(predicate: dict) -> set[str]:
    fields: set[str] = set()
    for k, v in predicate.items():
        if k in _LOGICAL_OPS:
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        fields.update(_extract_query_fields(item))
            continue
        fields.add(k)
        if isinstance(v, dict):
            for sub_k in v:
                is_op = sub_k.startswith('$')
                if is_op and sub_k not in _RECURSIVE_OPS and sub_k not in _LOGICAL_OPS:
                    continue
                if isinstance(v[sub_k], dict):
                    fields.update(_extract_query_fields(v[sub_k]))
    return fields


def _extract_index_fields(index_config: dict) -> set[str]:
    keys = index_config.get('key', [])
    return {k for k, _ in keys}


def _test_name_from_stack() -> str:
    frame = inspect.currentframe()
    try:
        frames = []
        while frame:
            frames.append(frame)
            frame = frame.f_back
        for f in frames:
            locals = f.f_locals
            if 'self' in locals:
                cls = type(locals['self'])
                if cls.__module__.startswith('test'):
                    return f'{cls.__module__}.{cls.__qualname__}.{f.f_code.co_name}'
            mod_name = f.f_globals.get('__name__', '')
            if mod_name.startswith('test') or mod_name == '__main__':
                fn = f.f_code.co_name
                if fn.startswith('test_'):
                    return f'{mod_name}.{fn}'
    finally:
        del frame
    return '<unknown>'


def _analyze_sort_coverage(sort: list | None, indexes: dict[str, dict]) -> str:
    if not sort:
        return 'none'
    norm_sort = [(f, d) if isinstance(f, str) else (f[0], f[1]) for f, d in sort]
    for idx_cfg in indexes.values():
        idx_keys = idx_cfg.get('key', [])
        if len(idx_keys) < len(norm_sort):
            continue
        prefix = idx_keys[: len(norm_sort)]
        if prefix == norm_sort:
            return 'full'
        if prefix == [(f, d * -1) for f, d in norm_sort]:
            return 'full'
        if len(norm_sort) == 1:
            for k, _d in idx_keys:
                if k == norm_sort[0][0]:
                    return 'partial'
    return 'none'


class QueryProfiler:
    def __init__(self):
        self._enabled = False
        self._records: list[QueryRecord] = []
        self._output_path: str | None = None
        self._index_snapshots: dict[str, dict[str, dict]] = {}

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def records(self) -> list[QueryRecord]:
        return list(self._records)

    def start(self, output_path: str = 'mongomock-profiler-report.json'):
        self._enabled = True
        self._output_path = output_path
        self._records.clear()

    def stop(self):
        self._enabled = False

    def reset(self):
        self._records.clear()

    def record(
        self,
        filter: dict,
        collection: str,
        operation: str,
        indexes: dict | None = None,
        sort: list | None = None,
    ):
        if not self._enabled:
            return
        predicate = normalize_predicate(filter)
        test_name = _test_name_from_stack()
        self._records.append(
            QueryRecord(
                collection=collection,
                operation=operation,
                test_name=test_name,
                filter=filter,
                predicate=predicate,
                sort=sort,
            )
        )
        if indexes is not None and collection not in self._index_snapshots:
            self._index_snapshots[collection] = dict(indexes)

    def _analyze_coverage(self, collection_name: str, indexes: dict[str, dict]) -> list[dict]:
        records_for_coll = [r for r in self._records if r.collection == collection_name]
        grouped: dict[str, dict] = {}
        for rec in records_for_coll:
            key = str(rec.predicate)
            if key not in grouped:
                grouped[key] = {
                    'predicate': rec.predicate,
                    'operations': set(),
                    'count': 0,
                    'tests': set(),
                    'sample_filter': rec.filter,
                    'sort': rec.sort,
                }
            g = grouped[key]
            g['operations'].add(rec.operation)
            g['count'] += 1
            g['tests'].add(rec.test_name)

        cluster_patterns = []
        for g in grouped.values():
            q_fields = _extract_query_fields(g['predicate'])
            covered: set[str] = set()
            uncovered: set[str] = set()
            best_match: str | None = None
            for idx_name, idx_cfg in indexes.items():
                idx_fields = _extract_index_fields(idx_cfg)
                match = q_fields & idx_fields
                covered.update(match)
                partial_expr = idx_cfg.get('partialFilterExpression')
                if partial_expr is not None:
                    if not _matches_partial(g['sample_filter'], partial_expr):
                        match = set()
                    else:
                        covered.update(match)
                if match and best_match is None:
                    best_match = idx_name
            uncovered = q_fields - covered
            if not q_fields:
                filter_cov = 'none'
            elif covered == q_fields:
                filter_cov = 'full'
            elif covered:
                filter_cov = 'partial'
            else:
                filter_cov = 'none'

            sort_cov = _analyze_sort_coverage(g['sort'], indexes)

            cluster_patterns.append(
                {
                    'predicate': g['predicate'],
                    'operations': sorted(g['operations']),
                    'count': g['count'],
                    'tests': sorted(g['tests']),
                    'filter_coverage': filter_cov,
                    'sort_coverage': sort_cov,
                    'covered_fields': sorted(covered),
                    'uncovered_fields': sorted(uncovered),
                    'covering_index': best_match,
                    'sort': g['sort'],
                }
            )
            cluster_patterns.sort(key=lambda x: x['count'], reverse=True)
        return cluster_patterns

    def export_json(self, path: str | None = None):
        import json

        output_path = path or self._output_path or 'mongomock-profiler-report.json'
        report = self.report()
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)

    def report(self) -> dict:
        coll_records: dict[str, list[QueryRecord]] = {}
        for rec in self._records:
            coll_records.setdefault(rec.collection, []).append(rec)

        indexes_by_coll: dict[str, dict[str, dict]] = dict(self._index_snapshots)
        for coll_name in coll_records:
            if coll_name not in indexes_by_coll:
                indexes_by_coll[coll_name] = {}

        collections_data: dict[str, dict] = {}
        for coll_name in coll_records:
            indexes = indexes_by_coll.get(coll_name, {})
            idx_list: list[dict] = []
            for idx_name, idx_cfg in indexes.items():
                entry: dict = {'name': idx_name, 'key': idx_cfg.get('key', [])}
                if 'partialFilterExpression' in idx_cfg:
                    entry['partialFilterExpression'] = idx_cfg['partialFilterExpression']
                if 'unique' in idx_cfg:
                    entry['unique'] = idx_cfg['unique']
                if 'sparse' in idx_cfg:
                    entry['sparse'] = idx_cfg['sparse']
                if 'expireAfterSeconds' in idx_cfg:
                    entry['expireAfterSeconds'] = idx_cfg['expireAfterSeconds']
                idx_list.append(entry)

            patterns = self._analyze_coverage(coll_name, indexes)
            collections_data[coll_name] = {
                'indexes': idx_list,
                'patterns': patterns,
            }

        total_queries = len(self._records)
        all_patterns = [p for c in collections_data.values() for p in c['patterns']]
        full = sum(1 for p in all_patterns if p['filter_coverage'] == 'full')
        partial = sum(1 for p in all_patterns if p['filter_coverage'] == 'partial')
        none = sum(1 for p in all_patterns if p['filter_coverage'] == 'none')
        sort_full = sum(1 for p in all_patterns if p['sort_coverage'] == 'full')
        sort_none = sum(1 for p in all_patterns if p['sort_coverage'] == 'none')

        return {
            'metadata': {
                'total_queries': total_queries,
                'total_patterns': sum(len(c['patterns']) for c in collections_data.values()),
                'filter_coverage_summary': {
                    'full': full,
                    'partial': partial,
                    'none': none,
                },
                'sort_coverage_summary': {
                    'full': sort_full,
                    'none': sort_none,
                },
            },
            'collections': collections_data,
        }

    def auto_export(self):
        if self._records:
            self.export_json()


def _matches_partial(filter: dict, partial_expr: dict) -> bool:
    for key, expected_val in partial_expr.items():
        actual_val = filter.get(key)
        # Check for $exists: False — field absence is a match, not a short-circuit
        if isinstance(expected_val, dict) and expected_val.get('$exists') is False:
            if actual_val is not None:
                return False
            continue
        if actual_val is None:
            return False
        if isinstance(expected_val, dict) and next(iter(expected_val.keys())).startswith('$'):
            op = next(iter(expected_val.keys()))
            expected_operand = expected_val[op]
            if op == '$eq':
                if actual_val != expected_operand:
                    return False
            elif op == '$exists':
                pass
            elif op == '$gte':
                if not (actual_val >= expected_operand):
                    return False
            elif op == '$lte':
                if not (actual_val <= expected_operand):
                    return False
            elif op == '$gt':
                if not (actual_val > expected_operand):
                    return False
            elif op == '$lt':
                if not (actual_val < expected_operand):
                    return False
            elif op == '$in':
                if actual_val not in expected_operand:
                    return False
            elif op == '$ne':
                if actual_val == expected_operand:
                    return False
            elif op == '$nin' and actual_val in expected_operand:
                return False
        elif actual_val != expected_val:
            return False
    return True


_profiler: QueryProfiler | None = None


def get_profiler() -> QueryProfiler:
    global _profiler
    if _profiler is None:
        _profiler = QueryProfiler()
    return _profiler


if os.environ.get('MONGOMOCK_PROFILER'):
    get_profiler().start()
    atexit.register(get_profiler().auto_export)
