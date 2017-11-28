import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    read_me = f.read()

requires = [
    'dateutils',
    'parsimonious',
]

setup(
    name='doqapy',
    version='0.0',
    description='Document Querying API in Python',
    long_description=read_me,
    classifiers=[
        'Programming Language :: Python',
        'Framework :: Pyramid',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Application',
    ],
    author='',
    author_email='',
    url='',
    keywords='nosql database',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    extras_require={
        'yaml_io': ['yaml'],
    },
    install_requires=requires,
)
