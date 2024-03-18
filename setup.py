import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="task_queue-dander94", # Replace with your own username
    version="0.1.1+d5",
    author="dander94",
    author_email="",
    description="Task Queue Handler for Celery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/dander94/task_queue",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)