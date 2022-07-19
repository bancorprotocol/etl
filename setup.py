import re


from setuptools import find_packages, setup

with open("bancor_etl/__init__.py") as fd:
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE
    ).group(1)

with open("README.md", "r") as fh:
    long_description = fh.read()

extras_require = {}
extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))

setup(
    name='bancor_etl',
    version=version,
    author='Bancor Network',
    author_email='mike@bancor.network',
    description='Databricks ETL for Bancor Protocol V3 dashboard analytics page.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/bancorprotocol/simulator-v3',
    install_requires=open('requirements.txt').readlines(),
    extras_require=extras_require,
    tests_require=open('test-requirements.txt').readlines(),
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
          'bancor_simulator = bancor_research.bancor_simulator.__main__:cli'
        ]
    },
    data_files=[],
)