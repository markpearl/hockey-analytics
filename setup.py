
from setuptools import setup, find_packages
from os import path

setup(
   name='hockey_analytics',
   version='1.0',
   description='Framework to build hockey analytics platform',
   #long_description=readme,
   author='lixar',
   author_email='markpearl7@gmail.com',
   packages=find_packages(exclude=('jupyter', 'docs'))
)