#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mongomock
import unittest


class TestDumpAndLoad(unittest.TestCase):

    def test_dump_and_load(self):
        dbpath = "."

        # dump
        client = mongomock.MongoClient(dbpath=dbpath)
        db = client.get_database("test")

        col = db.get_collection("col")
        col.insert_many([
            {"_id": 1, "name": "John"},
            {"_id": 2, "name": "Mike"},
            {"_id": 3, "name": "Paul"},
        ])
        db.dump()

        # load
        client = mongomock.MongoClient(dbpath=dbpath)
        db = client.get_database("test")

        db.load()
        col = db.get_collection("col")
        self.assertListEqual(
            list(col.find({"_id": {"$gte": 3}})), [{"_id": 3, "name": "Paul"}]
        )


unittest.main()
