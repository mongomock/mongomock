import os
import itertools
import platform
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), "mongomock", "__version__.py")) as version_file:
    exec(version_file.read())

install_requires = ["sentinels", "six"]
if platform.python_version() < '2.7':
    install_requires.append('unittest2')
    install_requires.append('ordereddict')

if os.environ.get("INSTALL_PYMONGO", "false") == "true":
    install_requires.append("pymongo")
if os.environ.get("INSTALL_PYEXECJS", "false") == "true":
    install_requires.append("pyexecjs")


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
      install_requires=install_requires,
      scripts=[],
      namespace_packages=[]
      )
