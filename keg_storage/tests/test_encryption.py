from keg_storage.encryption import SymmetricKeyProvider


class TestSymmetricMasterKeyProvider:

    def test_cycle(self):
        plaintext = b'hi'
        key = {b'key': b'a' * 32}

        smkp = SymmetricKeyProvider(key)
        cipher = smkp.encrypt(plaintext).read()
        assert smkp.decrypt(cipher).read() == plaintext
        assert plaintext not in cipher
        assert key[b'key'] not in cipher
        assert smkp.encrypt(b'h') != cipher
        assert SymmetricKeyProvider({b'key': b'b' * 32}).encrypt(plaintext) != cipher

    def test_multi_key(self):
        plaintext = b'hi'
        key = {b'key': b'a' * 32, b'key2': b'b' * 32}

        smkp = SymmetricKeyProvider(key)
        cipher = smkp.encrypt(plaintext).read()

        assert SymmetricKeyProvider({b'key': b'a' * 32}).decrypt(cipher).read() == plaintext
        assert SymmetricKeyProvider({b'key2': b'b' * 32}).decrypt(cipher).read() == plaintext
