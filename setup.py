# encoding: utf-8
import os

from setuptools import setup, find_packages

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='yafs',
    version='0.3.0',
    author='Isaac Lera, Carlos Guerrero',
    author_email='isaac.lera@uib.es, carlos.guerrero@ouib.es',
    description='Yet Another Fog Simulator for Python.',
    long_description=long_description,
    url='https://yafs.readthedocs.io',
    license='MIT License',
    packages=find_packages(where='src', exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=['simpy', 'pandas', 'networkx', 'numpy', 'tqdm'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Scientific/Engineering',
    ],
)
