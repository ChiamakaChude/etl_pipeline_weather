from setuptools import setup, find_packages

setup(
    name="weatheretl",
    version="0.1.0",
    packages=find_packages(),
    description="ETL logic for weather ETL project",
)

## pip install -e .