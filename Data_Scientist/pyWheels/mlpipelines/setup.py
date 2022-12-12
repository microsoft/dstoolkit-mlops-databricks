import setuptools
from pathlib import Path

current_file_folder = Path(__file__).parent.absolute()
path_requirements = current_file_folder.joinpath('requirements.txt')

with open(path_requirements, "r") as fh:
    requirements = [l.strip() for l in fh.readlines()]

requirements = [rq for rq in requirements if (rq) and (rq.startswith('#') is False)]

print(requirements)


#with open("README.md", "r") as fh:
#    long_description = fh.read()

setuptools.setup(
    name="helperfunctions",
    version="0.0.1",
    author="Ciaran HD",
    author_email="ciaranh@microsoft.com",
    description="Package to create data ingestion",
    #long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8.8',
    install_requires=requirements
)