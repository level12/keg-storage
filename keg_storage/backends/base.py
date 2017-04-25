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
