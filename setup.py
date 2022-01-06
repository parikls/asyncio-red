import pathlib

from setuptools import setup, find_packages

here = pathlib.Path(__file__).parent
readme_path = here / "README.md"
requirements_path = here / "requirements.txt"


with readme_path.open() as f:
    README = f.read()

with requirements_path.open() as f:
    requirements = [line for line in f.readlines()]


setup(
    name='asyncio-red',
    version='0.2',
    description='asyncio RED',
    long_description=README,
    long_description_content_type='text/markdown',
    author='Dmytro Smyk',
    author_email='porovozls@gmail.com',
    url='https://github.com/parikls/asyncio-red',
    packages=find_packages(),
    package_data={'': ['asyncio_red/backend/streams/xtrim_acked.lua']},
    include_package_data=True,
    classifiers=[
        "Operating System :: OS Independent",
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    install_requires=requirements,
    python_requires='>=3.5.3',
    entry_points={
        'console_scripts': ['asyncio-red = asyncio_red.__main__:main']
    }
)
