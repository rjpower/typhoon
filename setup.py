#!/usr/bin/python

import setuptools

setuptools.setup(
  name='typhoon',
  version='0.01',
  url='http://github.com/rjpower/typhoon',
  install_requires=[],
  description=(''),
  packages=['typhoon'],
  package_dir={
    '' : '.'
  },
  zip_safe=False,
  test_suite = 'nose.collector',
  entry_points='''
  '''
)
