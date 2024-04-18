from setuptools import find_packages, setup

setup(
    name="dagster_quickstart",
    packages=find_packages(exclude=["dagster_quickstart_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "boto3",
        "pyarrow"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
