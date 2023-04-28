from setuptools import find_packages,setup
# from typing import List

def get_requirements():
    """
    This function will return list of requirements
    """
    requirement_list = []

    """
    Write a code to read requirements.txt file and append each requirements in requirement_list variable.
    """
    return requirement_list

PROJECT_NAME = 'Census Financial Complaint'
AUTHOR_NAME = 'Sukruth'
DESCRIPTION = ' This is used for classification project whther the issue is been resolved or not'
setup(
    name="census",
    version="0.0.1",
    author="sukruth",
    author_email="sukruthav007@gmail.com",
    packages = find_packages(),
    install_requires=get_requirements(),#["pymongo==4.2.0"],
)