import multiprocessing
from setuptools import setup, find_packages

test_requirements = ['sentinels>=0.0.6', 'nose>=1.0', 'python-dateutil>=2.2']

setup(
    name = "schoolbus_wikipedia",
    version = "0.0.1",
    packages = find_packages(),

    # Dependencies on other packages:
    setup_requires   = ['nose>=1.1.2'],
    tests_require    = test_requirements,
    install_requires = [
			'configparser>=3.3.0r2', 
			'argparse>=1.2.1', 
			'unidecode>=0.04.14', 
			'beautifulsoup4>=4.3.2',
			'redis_bus_python>=0.0.2',
			'wikipedia>=1.4.0',
			] + test_requirements,

    # Unit tests; they are initiated via 'python setup.py test'
    test_suite       = 'nose.collector', 

    #data_files = [('pymysql_utils/data', datafiles)],

    package_data = {
        # If any package contains *.txt or *.rst files, include them:
     #   '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
     #   'hello': ['*.msg'],
    },

    # metadata for upload to PyPI
    author = "Andreas Paepcke",
    author_email = "paepcke@cs.stanford.edu",
    description = "Provides wikipedia access on the schoolbus",
    license = "BSD",
    keywords = "Redis, Stanford schoolbus",
    url = "https://github.com/paepcke/schoolbus_wikipedia",   # project home page, if any
)
