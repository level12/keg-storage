import os

from setuptools import setup, find_packages

cdir = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(cdir, 'readme.rst')).read()

from keg_storage.version import VERSION


setup(
    name='KegStorage',
    version=VERSION,
    description='A simple storage interface with multiple backends for use in a Keg app',
    author='Level 12',
    author_email='devteam@level12.io',
    url='https://github.com/level12/keg-storage',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(exclude=['tests*']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'boto3',
        'paramiko',
        'six',
        'wrapt',
    ],
    long_descripton=README,
)
