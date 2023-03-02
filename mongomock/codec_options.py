"""Tools for specifying BSON codec options."""

import collections
from packaging import version

from mongomock import helpers

try:
    from bson import codec_options
    from pymongo.common import _UUID_REPRESENTATIONS
except ImportError:
    codec_options = None
    _UUID_REPRESENTATIONS = None


class TypeRegistry(object):
    pass


_FIELDS = (
    'document_class', 'tz_aware', 'uuid_representation', 'unicode_decode_error_handler', 'tzinfo',
)

if codec_options and helpers.PYMONGO_VERSION >= version.parse('3.8'):
    _DEFAULT_TYPE_REGISTRY = codec_options.TypeRegistry()
    _FIELDS = _FIELDS + ('type_registry',)
else:
    _DEFAULT_TYPE_REGISTRY = TypeRegistry()

# New default in Pymongo v4:
# https://pymongo.readthedocs.io/en/stable/examples/uuid.html#unspecified
if helpers.PYMONGO_VERSION >= version.parse('4.0'):
    _DEFAULT_UUID_REPRESENTATION = 0
else:
    _DEFAULT_UUID_REPRESENTATION = 3


class CodecOptions(collections.namedtuple('CodecOptions', _FIELDS)):

    def __new__(cls, document_class=dict,
                tz_aware=False,
                uuid_representation=None,
                unicode_decode_error_handler='strict',
                tzinfo=None, type_registry = None):

        if document_class != dict:
            raise NotImplementedError(
                'Mongomock does not implement custom document_class yet: %r' % document_class)

        if not isinstance(tz_aware, bool):
            raise TypeError('tz_aware must be True or False')

        if uuid_representation is None:
            uuid_representation = _DEFAULT_UUID_REPRESENTATION

        if unicode_decode_error_handler not in ('strict', None):
            raise NotImplementedError(
                'Mongomock does not handle custom unicode_decode_error_handler yet')

        if tzinfo:
            raise NotImplementedError('Mongomock does not handle custom tzinfo yet')

        values = (
            document_class, tz_aware, uuid_representation, unicode_decode_error_handler, tzinfo)

        if 'type_registry' in _FIELDS:
            if not type_registry:
                type_registry = _DEFAULT_TYPE_REGISTRY
            values = values + (type_registry,)

        return tuple.__new__(cls, values)

    def with_options(self, **kwargs):
        opts = self._asdict()
        opts.update(kwargs)
        return CodecOptions(**opts)

    def to_pymongo(self):
        if not codec_options:
            return None

        uuid_representation = self.uuid_representation
        if _UUID_REPRESENTATIONS and isinstance(self.uuid_representation, str):
            uuid_representation = _UUID_REPRESENTATIONS[uuid_representation]

        return codec_options.CodecOptions(
            uuid_representation=uuid_representation,
            unicode_decode_error_handler=self.unicode_decode_error_handler,
            type_registry=self.type_registry)


def is_supported(custom_codec_options):

    if not custom_codec_options:
        return None

    return CodecOptions(**custom_codec_options._asdict())
