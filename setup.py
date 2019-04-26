from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = [
  'tensorflow',
  'tensorflow-transform', 
  'apache-beam[gcp]', 
  'docopt'
  ]

setup(
  name='dataflow_tutorial',
  version='0.0.1',
  author = 'Calogero Zarbo',
  author_email = 'calogero.zarbo@algoritmica.ai',
  install_requires=REQUIRED_PACKAGES,
  packages=find_packages(),
  include_package_data=True,
  description='Tutorial for Apache Beam with Python 3.5 on Google Dataflow.'
  )