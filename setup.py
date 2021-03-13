#!/usr/bin/env python3

from setuptools import setup, Extension

setup(
    ext_modules=[
        Extension('bloom._hashc', ['bloom/_hashcmodule.c'])
    ])
