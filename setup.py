from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="LiveGantt",
    version="0.0.1",
    author="Vivian Hafener",
    author_email="vhafener@lanl.gov",
    description="A package that generates live gantt charts for HPC clusters",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    # include_package_data=True,
    project_urls={
        "Bug Tracker": "",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    # py_modules=["main", "utils", "gantt", "plots"],
    packages=['livegantt'],
    install_requires=[
        "matplotlib",
        "seaborn",
        "yaspin",
        "pandas",
    ],
    # packages=find_packages(include=['prompt-toolkit', 'Click'])
    # install_requires=['prompt-toolkit', 'Click']
    python_requires=">=3.6",
)
