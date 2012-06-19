import os
import itertools
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), "mongomock", "__version__.py")) as version_file:
    exec(version_file.read())

setup(name="mongomock",
      classifiers = [
          "Programming Language :: Python :: 2.7",
          ],
      description="Fake pymongo stub for testing simple MongoDB-dependent code",
      license="BSD",
      author="Rotem Yaari",
      author_email="vmalloc@gmail.com",
      version=__version__,
      packages=find_packages(exclude=["tests"]),
      install_requires=[],
      scripts=[],
      namespace_packages=[]
      )
