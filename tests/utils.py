import sys

if sys.version_info < (2, 7):
    from unittest2 import TestCase, skipIf
else:
    from unittest import TestCase, skipIf
