from setuptools import setup, find_packages

setup(
    name="fraud_detection",
    version="0.1",
    packages=find_packages(where="data_generator"),
    package_dir={"": "data_generator"},
)
