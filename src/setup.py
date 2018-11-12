from setuptools import setup
from os.path import join, dirname

here = dirname(__file__)

setup(name='cryptoapis',
      version='0.0.1',
      description='Financial data access libraries for crypto trading platforms.',
      long_description=open(join(here, 'README.md')).read(),
      author='Jun Ma',
      author_email='junma018@outlook.com',
      url='',
      install_requires=[
          'pandas==0.23.4',
      ],
      packages=['huobi',],
      )
