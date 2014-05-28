
.. container::

   .. image:: http://vmalloc.github.io/mongomock/mongomock-small.png


.. image:: https://travis-ci.org/vmalloc/mongomock.png?branch=master


.. image:: https://pypip.in/d/mongomock/badge.png
        :target: https://crate.io/packages/mongomock


.. image:: https://pypip.in/v/mongomock/badge.png
        :target: https://crate.io/packages/mongomock



What is this?
-------------
Mongomock is a small library to help testing Python code that interacts with MongoDB via Pymongo.

To understand what it's useful for, we can take the following code::

 def increase_votes(collection):
     for document in collection.find():
         collection.update(document, {'$set' : {'votes' : document['votes'] + 1}})

The above code can be tested in several ways:

1. It can be tested against a real mongodb instance with pymongo.
2. It can receive a record-replay style mock as an argument. In this manner we record the expected calls (find, and then a series of updates), and replay them later.
3. It can receive a carefully hand-crafted mock responding to find() and update() appropriately.

Option number 1 is obviously the best approach here, since we are testing against a real mongodb instance. However, a mongodb instance needs to be set up for this, and cleaned before/after the test. You might want to run your tests in continuous integration servers, on your laptop, or other bizarre platforms - which makes the mongodb requirement a liability.

We are left with #2 and #3. Unfortunately they are very high maintenance in real scenarios, since they replicate the series of calls made in the code, violating the DRY rule. Let's see #2 in action - we might right our test like so::

 def test_increase_votes():
     objects = [dict(...), dict(...), ...]
     collection_mock = my_favorite_mock_library.create_mock(Collection)
     record()
     collection_mock.find().AndReturn(objects)
     for obj in objects:
         collection_mock.update(document, {'$set' : {'votes' : document['votes']}})
     replay()
     increase_votes(collection_mock)
     verify()

Let's assume the code changes one day, because the author just learned about the '$inc' instruction::

 def increase_votes(collection):
     collection.update({}, {'$inc' : {'votes' : 1}})

This breaks the test, although the end result being tested is just the same. The test also repeats large portions of the code we already wrote.

We are left, therefore, with option #3 -- you want something to behave like a mongodb database collection, without being one. This is exactly what this library aims to provide. With mongomock, the test simply becomes::

 def test_increase_votes():
     collection = mongomock.Connection().db.collection
     objects = [dict(votes=1), dict(votes=2), ...]
     for obj in objects:
         obj['_id'] = collection.insert(obj)
     increase_votes(collection)
     for obj in objects:
         stored_obj = collection.find_one({'_id' : obj['_id']})
         stored_obj['votes'] -= 1
         assert stored_obj == obj # by comparing all fields we make sure only votes changed

This code checks *increase_votes* with respect to its functionality, not syntax or algorithm, and therefore is much more robust as a test.

To download, setup and perfom tests, run the following commands on Mac / Linux::

 git clone <repo>
 cd <reponame>
 virtualenv venv --distribute
 source venv/bin/activate
 pip install nose
 pip install pymongo
 pip install pyexecjs
 python setup.py install
 nosetests


Important Note About Project Status & Development
-------------------------------------------------

MongoDB is complex. This library aims at a reasonably complete mock of MongoDB for testing purposes, not a perfect replica. This means some features are not likely to make it in any time soon.

Also, since many corner cases are encountered along the way, our goal is to try and TDD our way into completeness. This means that every time we encounter a missing or broken (incompatible) feature, we write a test for it and fix it. There are probably lots of such issues hiding around lurking, so feel free to open issues and/or pull requests and help the project out!

**NOTE**: We don't include pymongo functionality as "stubs" or "placeholders". Since this library is used to validate production code, it is unacceptable to behave differently than the real pymongo implementation. In such cases it is better to throw `NotImplementedError` than implement a modified version of the original behavior.

Contributing
------------

When submitting a PR, please make sure that:

1. You include tests for the feature you are adding or bug you are fixing. Preferably, the test should compare against the real MongoDB engine (See `examples in tests <https://github.com/vmalloc/mongomock/blob/master/tests/test__mongomock.py#L108>`_ for reference).
2. No existing test got deleted or unintentionally castrated
3. The travis build passes on your PR.

Acknowledgements
----------------

Many thanks go to the following people for helping out:

* Alec Perkins
* Austin W Ellis
* Andrey Ovchinnikov
* Arthur Hirata
* Corey Downing
* Craig Hobbs
* Daniel Murray
* David Fischer
* Edward D'Souza
* Emily Rosengren
* Eugene Chernyshov
* Grigoriy Osadchenko
* Israel Teixeira
* Jacob Perkins
* Jason Sommer
* Jeff Browning
* Jeff McGee
* Joël Franusic
* Mike Ho
* Nigel Choi
* Omer Gertel
* Scott Sexton
* Todd Tomkinson 
* baruchoxman
* catty (ca77y _at_ live.com)
* emosenkis
* hthieu1110
* waskew (waskew _at_ narrativescience.com)
* zcarter

