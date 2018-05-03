from setuptools import setup, find_packages
setup(
    name="async_rethink",
    version="1.0.0-rc",
    license="GPLv3",
    install_requires=[
        'aioreactive==0.5.0',
        'rethinkdb>=1.14',
        'logzero==1.3.1'
    ],
    packages=find_packages(),
)
