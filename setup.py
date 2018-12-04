from setuptools import setup, find_packages

print(find_packages())

# The main call
setup(name='parquet_tools',
      version='1.0.0',
      license="GPL",
      description="parquet tools",
      author="Andrew Hanushevsky",
      author_email="abh@slac.stanford.edu",
      packages=find_packages(),
      scripts=["python/parquet_tools/csv2pq/csv2pq"],
      data_files=[('tests', ['tests/data/testfile1.csv',
                             'tests/data/testfile2.csv',
                             'tests/data/test.schema'])]
)
