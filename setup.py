import os.path as osp
from setuptools import setup, find_packages


cdir = osp.abspath(osp.dirname(__file__))
README = open(osp.join(cdir, 'README.rst')).read()
CHANGELOG = open(osp.join(cdir, 'changelog.rst')).read()

version = {}
with open(osp.join(cdir, 'keg_storage', 'version.py')) as version_fp:
    exec(version_fp.read(), version)

setup(
    name="KegStorage",
    description="A simple storage interface with multiple backends for use in a Keg app.",
    long_description='\n\n'.join((README, CHANGELOG)),
    author="Level 12 Developers",
    author_email="devteam@level12.io",
    url='https://github.com/level12/keg-storage',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    license='BSD',
    packages=find_packages(),
    zip_safe=True,
    version=version['VERSION'],
    install_requires=[
        'arrow',
        'humanize',
        'BlazeUtils',
        'itsdangerous',
    ],
    extras_require={
        'sftp': [
            'paramiko',
        ],
        'aws': [
            'boto3',
        ],
        'azure': [
            'azure-storage-blob',
        ],
        'keg': [
            'kegelements',
        ],
        'test': [
            'azure-storage-blob',
            'boto3',
            'flake8',
            'flask_webtest',
            'freezegun',
            'paramiko',
            'pytest',
            'pytest-coverage',
            'tox',
            'wrapt',
        ]
    }
)
