#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    'girder>=3.0.0a1',
    'globus-sdk'
]

setup(
    author="Zach Mullen",
    author_email='kitware@kitware.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
    description='Expose globus entrypoints within Girder',
    install_requires=requirements,
    license='Apache Software License 2.0',
    long_description=readme,
    include_package_data=True,
    keywords='girder-plugin, globus_endpoints',
    name='globus_endpoints',
    packages=find_packages(exclude=['test', 'test.*']),
    url='https://github.com/girder/globus_endpoints',
    version='0.1.0',
    zip_safe=False,
    entry_points={
        'girder.plugin': [
            'globus_endpoints = globus_endpoints:GirderPlugin'
        ]
    }
)
