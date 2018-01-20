from setuptools import setup, find_packages
setup(
    name="async_rethink",
    version="0.1.1",
    license="GPLv3",
    install_requires=[
        'Rx==1.6.0',
        'rethinkdb>=1.14',
        'logzero==1.3.1'
    ],
    packages=find_packages(),
)