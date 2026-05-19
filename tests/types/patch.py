import mongomock_ng


@mongomock_ng.patch(servers=(('server.example.com', 27017),))
class MyTestA: ...


@mongomock_ng.patch(('mydata.com', 'myprivatedata.com'))
class MyTestB: ...
