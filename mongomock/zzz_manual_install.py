#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2014-2016 Sanhe Hu

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import print_function, unicode_literals
import os
import site
import shutil
import hashlib
import platform

__version__ = "0.0.1"
__short_description__ = "Utility script to install your package in one shot, for developer."
__author__ = "Sanhe Hu"

SRC = os.getcwd()
PKG_NAME = os.path.basename(SRC)

SYS_NAME = platform.system()
if SYS_NAME == "Windows":
    BIN_SCRIPTS = "Scripts"
elif SYS_NAME in ["Darwin", "Linux"]:
    BIN_SCRIPTS = "bin"


def is_venv():
    """Check whether if this workspace is a virtualenv.
    """
    dir_path = os.path.dirname(SRC)
    is_venv_flag = True

    if SYS_NAME == "Windows":
        executable_list = ["activate", "pip.exe", "python.exe"]
    elif SYS_NAME in ["Darwin", "Linux"]:
        executable_list = ["activate", "pip", "python"]

    for executable in executable_list:
        path = os.path.join(dir_path, BIN_SCRIPTS, executable)
        if not os.path.exists(path):
            is_venv_flag = False

    return is_venv_flag


def find_linux_venv_py_version():
    """Find python version name used in this virtualenv.

    For example: ``python2.7``, ``python3.4``
    """
    available_python_version = [
        "python2.6",
        "python2.7",
        "python3.3",
        "python3.4",
        "python3.5",
        "python3.6",
    ]
    dir_path = os.path.dirname(SRC)
    for basename in os.listdir(os.path.join(dir_path, BIN_SCRIPTS)):
        for python_version in available_python_version:
            if python_version in basename:
                return python_version
    raise Exception("Can't find virtualenv python version!")


def find_venv_DST():
    """Find where this package should be installed to in this virtualenv.

    For example: ``/path-to-venv/lib/python2.7/site-packages/package-name``
    """
    dir_path = os.path.dirname(SRC)

    if SYS_NAME == "Windows":
        DST = os.path.join(dir_path, "Lib", "site-packages", PKG_NAME)
    elif SYS_NAME in ["Darwin", "Linux"]:
        python_version = find_linux_venv_py_version()
        DST = os.path.join(dir_path, "lib", python_version, "site-packages", PKG_NAME)

    return DST


def find_DST():
    """Find where this package should be installed to.
    """
    if SYS_NAME == "Windows":
        return os.path.join(site.getsitepackages()[1], PKG_NAME)
    elif SYS_NAME in ["Darwin", "Linux"]:
        return os.path.join(site.getsitepackages()[0], PKG_NAME)


if is_venv():
    DST = find_venv_DST()
else:
    DST = find_DST()


def md5_of_file(abspath):
    """Md5 value of a file.
    """
    chunk_size = 1024 * 1024
    m = hashlib.md5()
    with open(abspath, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            m.update(data)
    return m.hexdigest()


def check_need_install():
    """Check if installed package are exactly the same to this one.
    By checking md5 value of all files.
    """
    need_install_flag = False
    for root, _, basename_list in os.walk(SRC):
        if os.path.basename(root) != "__pycache__":
            for basename in basename_list:
                src = os.path.join(root, basename)
                dst = os.path.join(root.replace(SRC, DST), basename)
                if os.path.exists(dst):
                    if md5_of_file(src) != md5_of_file(dst):
                        return True
                else:
                    return True
    return need_install_flag


def install():
    """Manual install main script.
    """
    # check installed package
    print("Compare to '%s' ..." % DST)
    need_install_flag = check_need_install()
    if not need_install_flag:
        print("    package is up-to-date, no need to install.")
        return
    print("Difference been found, start installing ...")

    # remove __pycache__ folder and *.pyc file
    print("Remove *.pyc file ...")
    pyc_folder_list = list()
    for root, _, basename_list in os.walk(SRC):
        if os.path.basename(root) == "__pycache__":
            pyc_folder_list.append(root)

    for folder in pyc_folder_list:
        shutil.rmtree(folder)
    print("    all *.pyc file has been removed.")

    # install this package to all python version
    print("Uninstall %s from %s ..." % (PKG_NAME, DST))
    try:
        shutil.rmtree(DST)
        print("    Successfully uninstall %s" % PKG_NAME)
    except Exception as e:
        print("    %s" % e)

    print("Install %s to %s ..." % (PKG_NAME, DST))
    shutil.copytree(SRC, DST)
    print("    Complete!")


if __name__ == "__main__":
    install()