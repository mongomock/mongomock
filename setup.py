import os
import platform
from setuptools import setup, find_packages


version_file_path = os.path.join(
    os.path.dirname(__file__), "mongomock", "__version__.py")


with open(version_file_path) as version_file:
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
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "Intended Audience :: Developers",
          "Operating System :: MacOS :: MacOS X",
          "Operating System :: Microsoft :: Windows",
          "Operating System :: POSIX",
          "Programming Language :: Python :: 2",
          "Programming Language :: Python :: 2.6",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.3",
          "Programming Language :: Python :: 3.4",
          "Programming Language :: Python :: Implementation :: CPython",
          "Programming Language :: Python :: Implementation :: PyPy",
          "Topic :: Database"],
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
