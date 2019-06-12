class StorageBackend:
    name = None

    def __init__(self, *args, encryption_provider=None, encrypt=True, decrypt=True, **kwargs):
        self.encryption_provider = encryption_provider

        self.should_encrypt = encrypt
        self.should_decrypt = decrypt

    def encrypt(self, readable):
        if self.encryption_provider and self.should_encrypt:
            return self.encryption_provider.encrypt(readable)
        else:
            return readable

    def decrypt(self, readable):
        if self.encryption_provider and self.should_decrypt:
            return self.encryption_provider.decrypt(readable)
        else:
            return readable

    def list(self, path):
        """Returns an iterator over the given path"""
        raise NotImplementedError()

    def get(self, path, dest, decrypt=True):
        raise NotImplementedError()

    def put(self, path, dest, encrypt=True):
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
