import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cloud-utils",
    version="0.0.1",
    author="Alec Gunny",
    author_email="alec.gunny@gmail.com",
    description="Tools for managing cloud resources",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alecgunny/cloud-utils",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
