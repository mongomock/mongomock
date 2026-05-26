import mongomock


@mongomock.patch(servers=(('server.example.com', 27017),))
class MyTestA: ...


@mongomock.patch(('mydata.com', 'myprivatedata.com'))
class MyTestB: ...
