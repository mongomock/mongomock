import sys

import mongomock_ng


sys.modules['mongomock'] = mongomock_ng


def pytest_report_header():
    try:
        import pymongo

        return f'pymongo: {pymongo.__version__}'
    except ImportError:
        return 'pymongo: not installed'
