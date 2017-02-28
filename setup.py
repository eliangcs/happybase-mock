try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import codecs
import os
import re


here = os.path.abspath(os.path.dirname(__file__))


# Read the version number from a source file.
# Why read it, and not import?
# See https://groups.google.com/d/topic/pypa-dev/0PkjVpcxTzQ/discussion
def find_version(*file_paths):
    # Open in Latin-1 so that we avoid encoding errors.
    # Use codecs.open for Python 2 compatibility
    with codecs.open(os.path.join(here, *file_paths), 'r', 'latin1') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string')


def read_description(filename):
    with codecs.open(filename, encoding='utf-8') as f:
        return f.read()


def parse_requirements(filename):
    with open(filename) as f:
        content = f.read()
    return filter(lambda x: x and not x.startswith('#'), content.splitlines())


setup(
    name='happybase-mock',
    version=find_version('happybase_mock', '__init__.py'),
    url='https://github.com/eliangcs/happybase-mock',
    description='A mocking library for HappyBase',
    long_description=read_description('README.rst'),
    author='Chang-Hung Liang',
    author_email='eliang.cs@gmail.com',
    license='MIT',
    packages=['happybase_mock'],
    install_requires=parse_requirements('requirements.txt'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Testing'
    ]
)
