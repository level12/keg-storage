Changelog
=========

0.5.8 released 2022-01-17
-------------------------

- add py.typed for PEP 561 compatibility (c5731f5_)
- ensure paths passed to copy are resolved to the root path (1cab726_)

.. _c5731f5: https://github.com/level12/keg-storage/commit/c5731f5
.. _1cab726: https://github.com/level12/keg-storage/commit/1cab726


0.5.7 released 2021-12-14
-------------------------

- add copy method and related CLI for s3 and local backends (02003a8_)
- CircleCI config fix for python versions present (19c1881_)
- update tox for python versions present in CI (818527b_)

.. _02003a8: https://github.com/level12/keg-storage/commit/02003a8
.. _19c1881: https://github.com/level12/keg-storage/commit/19c1881
.. _818527b: https://github.com/level12/keg-storage/commit/818527b


0.5.6 released 2021-01-26
-------------------------

- Support latest version of Azure SDK (6ffff6a_)
- Added usage notes for URLs created with the S3 `link_to` method  (4c60eb2_)
- Fix typo in documentation (6c6eb8e_)

.. _6ffff6a: https://github.com/level12/keg-storage/commit/6ffff6a
.. _4c60eb2: https://github.com/level12/keg-storage/commit/4c60eb2
.. _6c6eb8e: https://github.com/level12/keg-storage/commit/6c6eb8e


0.5.5 released 2020-05-19
-------------------------

- Allow callers to specify port to use for SFTP connections (a880b3e_)

.. _a880b3e: https://github.com/level12/keg-storage/commit/a880b3e


0.5.4 released 2020-05-14
-------------------------

- Support direct upload / download URL generation for all backends (6975227_)
- Add a local filesystem backend (6242913_)

.. _6975227: https://github.com/level12/keg-storage/commit/6975227
.. _6242913: https://github.com/level12/keg-storage/commit/6242913


0.5.3 released 2020-04-29
-------------------------

- Properly escape paths for SAS token generation (9f126c8_)

.. _9f126c8: https://github.com/level12/keg-storage/commit/9f126c8


0.5.2 released 2020-04-28
-------------------------

- Fix Azure SAS blob upload link permissions (d7a1653_)

.. _d7a1653: https://github.com/level12/keg-storage/commit/d7a1653


0.5.1 released 2020-04-08
-------------------------

- Fix Version Import (fb162f7_)

.. _fb162f7: https://github.com/level12/keg-storage/commit/fb162f7


0.5.0 released 2020-04-08
-------------------------

- Enable AzureStorage to take a blob SAS URL (8e6e478_)
- Allow AzureStorage to be used with a container SAS URL (#28) (f405d21_)
- Add support for download and upload progress callbacks (#34) (468fec8_)
- Fix config variable naming (#33) (9720e63_)
- Make aws dependencies optional (a2146d1_)

.. _8e6e478: https://github.com/level12/keg-storage/commit/8e6e478
.. _f405d21: https://github.com/level12/keg-storage/commit/f405d21
.. _468fec8: https://github.com/level12/keg-storage/commit/468fec8
.. _9720e63: https://github.com/level12/keg-storage/commit/9720e63
.. _a2146d1: https://github.com/level12/keg-storage/commit/a2146d1


0.4.4 released 2020-01-29
-------------------------

- Create Sphinx Documentation (753ef58_)

.. _753ef58: https://github.com/level12/keg-storage/commit/753ef58


0.4.3 released 2020-01-28
-------------------------

- Merge pull request #27 from level12/stdin-stdout-cli-support (aef81c1_)
- Merge pull request #25 from level12/azure-client-version (02c44de_)
- Add noqa to version file to avoid coverage errors (1f4e5c3_)

.. _aef81c1: https://github.com/level12/keg-storage/commit/aef81c1
.. _02c44de: https://github.com/level12/keg-storage/commit/02c44de
.. _1f4e5c3: https://github.com/level12/keg-storage/commit/1f4e5c3


0.4.2 released 2019-11-29
-------------------------

- Stricter Azure Dependencies (adea745_)

.. _adea745: https://github.com/level12/keg-storage/commit/adea745


0.4.1 released 2019-10-17
-------------------------

- feat: Make keg dependency optional and improve backend optional dependency handling (e33139a_)
- bug: Fix bug where small files written to S3 buckets were never flushed (bd6c2c0_)
- feat: Add support for Azure blob storage (4141319_)

.. _e33139a: https://github.com/level12/keg-storage/commit/e33139a
.. _bd6c2c0: https://github.com/level12/keg-storage/commit/bd6c2c0
.. _4141319: https://github.com/level12/keg-storage/commit/4141319


0.4.0 released 2019-09-27
-------------------------

- feat: Update interface to allow file-like access (7c2a1b7_)

.. _7c2a1b7: https://github.com/level12/keg-storage/commit/7c2a1b7


0.3.1 released 2019-08-05
-------------------------

- bug: Make the return type of the list command consistent across backends (53abcda_)

.. _53abcda: https://github.com/level12/keg-storage/commit/53abcda


0.3.0 released 2019-06-02
-------------------------

- feat: Enable Multi-Key Re-encryption Scenarios Possible (a0d74a1_)
- feat: Make CLI Click Group Global (41f062a_)
- bug: ensure `list` operations always returns string (177d4a9_)

.. _a0d74a1: https://github.com/level12/keg-storage/commit/a0d74a1
.. _41f062a: https://github.com/level12/keg-storage/commit/41f062a
.. _177d4a9: https://github.com/level12/keg-storage/commit/177d4a9


0.2.0
=====

* [FEAT] Added Support for AWS Credentials using a Profile
