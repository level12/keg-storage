Configuration
=============

.. toctree::
   :maxdepth: 1
   :caption: Configuration:


Storage Profiles
----------------

Configure storage backends using the ``KEG_STORAGE_PROFILES`` setting. This should be a list of
2-tuples, matching a :py:class:`keg_storage.backends.StorageBackend` with a dict of initialization
arguments.

For an example, refer to :py:class:`keg_storage_ta.config.DefaultProfile`.
