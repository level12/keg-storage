import os.path as osp
from setuptools import setup, find_packages


cdir = osp.abspath(osp.dirname(__file__))
README = open(osp.join(cdir, 'README.rst')).read()
CHANGELOG = open(osp.join(cdir, 'CHANGELOG.rst')).read()

setup(
    name="lib",
    setup_requires=['setuptools_scm'],
    use_scm_version=True,
    description="The starting porint for a keg library",
    long_description='\n\n'.join((README, CHANGELOG)),
    author="Level 12 Developers",
    author_email="devteam@level12.io",
    url='https://github.com/level12/keg-baselib',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    license='BSD',
    packages=find_packages(),
    zip_safe=True,
    install_requires=[],
)
