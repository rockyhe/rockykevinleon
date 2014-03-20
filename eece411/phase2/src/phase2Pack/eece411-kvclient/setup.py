#!/usr/bin/env python
from setuptools import setup
from setuptools.command.test import test as TestCommand
import sys
import io

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['tests']
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)

setup(
    name='eece411-kvclient',
    version='0.1.0',
    url='',
    license=io.open('LICENSE').read(),
    author='Tyler Jones',
    author_email='tyler@squirly.ca',
    description='Key Value client for EECE 411.',
    long_description=io.open('README.rst').read(),
    packages=['kvclient'],
    install_requires=[],
    tests_require=['pytest', 'mock'],
    cmdclass={'test': PyTest},
)
