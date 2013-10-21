#!/usr/bin/env python

import re

from setuptools import setup, find_packages

module_name = 'celery_tasks_ctl'

VERSIONFILE = "%s/_version.py" % module_name
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
matches = re.search(VSRE, verstrline, re.M)
if matches:
    version = matches.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

setup(
    name=module_name,
    version=version,
    packages=find_packages(),
    install_requires=['pika==0.9.13', 'celery==3.0.23', 'docopt==0.6.1'],
    author='Brendan Maguire',
    author_email='maguire.brendan@gmail.com',
    description='Celery Tasks Controller to list and revoke tasks',
    long_description=open('README.rst').read(),
    scripts=['bin/celery_tasks_ctl'],
)
