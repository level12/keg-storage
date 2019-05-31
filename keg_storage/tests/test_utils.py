import io

import pytest
from mock import mock
import keg_elements.crypto as ke_crypto

from keg_storage import utils

DEFAULT_PLAINTEXT = b'data' * 1024 * 1024


class TestReencrypt:
    def mock_storage(self, old_ciphertext, new_ciphertext):
        m_storage = mock.MagicMock()

        def fake_get(src, dest):
            with open(dest, 'wb') as fp:
                fp.write(old_ciphertext)

        m_storage.get.side_effect = fake_get

        def fake_put(src, dest):
            with open(src, 'rb') as fp:
                new_ciphertext.write(fp.read())

        m_storage.put.side_effect = fake_put
        return m_storage

    def create_mock_storage(self, old, new, plaintext=DEFAULT_PLAINTEXT):
        n_ct = io.BytesIO()
        o_ct = b''.join(ke_crypto.encrypt_fileobj(old, io.BytesIO(plaintext)))
        return self.mock_storage(o_ct, n_ct), o_ct, n_ct

    def test_reencrypt_with_bad_keys_throws_key_error(self):
        with pytest.raises(utils.EncryptionKeyException):
            utils.reencrypt(None, 'file', b'', b'')

        with pytest.raises(utils.EncryptionKeyException):
            utils.reencrypt(None, 'file', b'a' * 32, b'')

        with pytest.raises(utils.EncryptionKeyException):
            utils.reencrypt(None, 'file', [b'a', b'b' * 20] * 32, b'')

    def test_reencrypt_takes_multiple_key(self):
        k1, k2, k3, = b'a' * 32, b'b' * 32, b'c' * 32

        store, o_ct, n_ct = self.create_mock_storage(k1, k3)
        utils.reencrypt(store, 'file', [k1, k2], k3)

        n_ct.seek(0)
        new_plaintext = io.BytesIO()
        ke_crypto.decrypt_fileobj(k3, n_ct, new_plaintext, chunksize=1024)

        assert new_plaintext.getvalue() == DEFAULT_PLAINTEXT

    def test_reencrypt_with_single_key(self):
        k1, k2 = b'a' * 32, b'b' * 32

        store, o_ct, n_ct = self.create_mock_storage(k1, k2)
        utils.reencrypt(store, 'file', k1, k2)

        assert store.get.call_count == 1
        args, kwargs = store.get.call_args
        assert len(args) == 2
        assert args[0] == 'file'

        assert store.put.call_count == 1
        args, kwargs = store.put.call_args
        assert len(args) == 2
        assert args[1] == 'file'

        n_ct.seek(0)
        new_plaintext = io.BytesIO()
        ke_crypto.decrypt_fileobj(k2, n_ct, new_plaintext, chunksize=1024)

        assert new_plaintext.getvalue() == DEFAULT_PLAINTEXT

    def test_with_bad_key(self):
        k1, k2, k3, = b'a' * 32, b'b' * 32, b'c' * 32
        store, o_ct, n_ct = self.create_mock_storage(k2, k3)

        with pytest.raises(utils.DecryptionException):
            utils.reencrypt(store, 'file', k1, k3)
