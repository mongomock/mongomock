#!/bin/bash

/usr/bin/mongod --fork --logpath /data/mongodb.log
until nc -z localhost 27017
do
    echo Waiting for MongoDB
    sleep 1
done
exec "$@"
