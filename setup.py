import distutils
from distutils.core import setup

# The main call
setup(name='csv2pq',
      version='1.0.0',
      license="GPL",
      description="csv/parquet tools",
      author="Andrew Hanushevsky",
      author_email="abh@slac.stanford.edu",
      packages=['parquet_tools'],
      package_dir={'': 'python'},
      scripts=["bin.src/csv2pq"],
      data_files=[('tests', ['tests/data/testfile1.csv',
                             'tests/data/testfile2.csv',
                             'tests/data/test.schema'])
)
