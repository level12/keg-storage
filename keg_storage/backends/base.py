import collections


ListEntry = collections.namedtuple('ListEntry', ['name', 'last_modified', 'size'])


class StorageBackend:
    name = None

    def __init__(self, *args, **kwargs):
        pass

    def list(self, path):
        """Returns an iterator over the given path"""
        raise NotImplementedError()

    def get(self, path, dest):
        raise NotImplementedError()

    def put(self, path, dest):
        raise NotImplementedError()

    def delete(self, path):
        raise NotImplementedError()

    def __str__(self):
        return self.__class__.__name__


class FileNotFoundInStorageError(Exception):
    def __init__(self, storage_type, filename):
        self.storage_type = storage_type
        self.filename = filename

    def __str__(self):
        return "File {} not found in {}.".format(self.filename, str(self.storage_type))
