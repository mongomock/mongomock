from .mongo_client import MongoClient
from mongomock import InvalidURI
import time

try:
    from unittest import mock
    _IMPORT_MOCK_ERROR = None
except ImportError:
    try:
        import mock
        _IMPORT_MOCK_ERROR = None
    except ImportError as error:
        _IMPORT_MOCK_ERROR = error

try:
    import pymongo
    _IMPORT_PYMONGO_ERROR = None
except ImportError as error:
    _IMPORT_PYMONGO_ERROR = error


def patch(servers='localhost', on_new='error'):
    """Patch pymongo.MongoClient.

    This will patch the class MongoClient and use mongomock to mock MongoDB
    servers. It keeps a consistant state of servers across multiple clients so
    you can do:

    ```
    client = pymongo.MongoClient(host='localhost', port=27017)
    client.db.coll.insert_one({'name': 'Pascal'})

    other_client = pymongo.MongoClient('mongodb://localhost:27017')
    client.db.coll.find_one()
    ```

    The data is persisted as long as the patch lives.

    Args:
        on_new: Behavior when accessing a new server (not in servers):
            'create': mock a new empty server, accept any client connection.
            'error': raise a ValueError immediately when trying to access.
            'timeout': behave as pymongo when a server does not exist, raise an
                error after a timeout.
            'pymongo': use an actual pymongo client.
        servers: a list of server that are avaiable.
    """

    if _IMPORT_MOCK_ERROR:
        raise _IMPORT_MOCK_ERROR  # pylint: disable=raising-bad-type

    if _IMPORT_PYMONGO_ERROR:
        PyMongoClient = None
    else:
        PyMongoClient = pymongo.MongoClient

    persisted_clients = {}
    parsed_servers = {
        _parse_host_and_port(server)
        for server in (servers if isinstance(servers, (list, tuple)) else [servers])
    }

    def _create_persistent_client(*args, **kwargs):
        if _IMPORT_PYMONGO_ERROR:
            raise _IMPORT_PYMONGO_ERROR  # pylint: disable=raising-bad-type

        client = MongoClient(*args, **kwargs)

        client_host, client_port = client.address
        host, port = _parse_host_and_port(client_host, client_port)

        try:
            return persisted_clients[(host, port)]
        except KeyError:
            pass

        if (host, port) in parsed_servers or on_new == 'create':
            persisted_clients[(host, port)] = client
            return client

        if on_new == 'timeout':
            # TODO(pcorpet): Only wait when trying to access the server's data.
            time.sleep(kwargs.get('serverSelectionTimeoutMS', 30000))
            raise pymongo.errors.ServerSelectionTimeoutError(
                '%s:%d: [Errno 111] Connection refused' % client.address)

        if on_new == 'pymongo':
            return PyMongoClient(*args, **kwargs)

        raise ValueError(
            'MongoDB server %s:%d does not exist.\n' % client.address + '%s' % parsed_servers)

    return mock.patch('pymongo.MongoClient', _create_persistent_client)


def _parse_host_and_port(uri, default_port=27017):
    """A simplified version of pymongo.uri_parser.parse_uri to get the dbase.

    Returns a tuple of the main host and the port provided in the URI.

    An invalid MongoDB connection URI may raise an InvalidURI exception,
    however, the URI is not fully parsed and some invalid URIs may not result
    in an exception.
    """
    if '://' not in uri:
        return uri, default_port

    uri = uri.split('://', 1)[1]

    if '/' in uri:
        uri = uri.split('/', 1)[0]

    # TODO(pascal): Handle replica sets better. Accessing the secondary hosts
    # should reach the same dataas the primary.
    if ',' in uri:
        uri = uri.split(',', 1)[0]

    if ']:' in uri:
        host, uri = uri.split(']:', 1)
        host = host + ']'
    elif ':' in uri and not uri.endswith(']'):
        host, uri = uri.split(':', 1)
    else:
        return uri, default_port

    if not uri:
        return uri, default_port

    try:
        return host, int(uri)
    except ValueError:
        raise InvalidURI('Invalid URI scheme: could not parse port "%s"' % uri)
