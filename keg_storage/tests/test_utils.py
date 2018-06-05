import io

from mock import mock
import keg_elements.crypto as ke_crypto

from keg_storage import utils


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

    def test_reencrypt(self):
        old_key = b'old-key-old-key-'
        new_key = b'new-key-new-key-'
        plaintext = b'data' * 1024 * 1024  # 4 MB of data

        old_ciphertext = b''.join(ke_crypto.encrypt_fileobj(old_key, io.BytesIO(plaintext)))
        new_ciphertext = io.BytesIO()

        m_storage = self.mock_storage(old_ciphertext, new_ciphertext)

        utils.reencrypt(m_storage, 'foo/bar.enc2', old_key, new_key)

        assert m_storage.get.call_count == 1
        args, kwargs = m_storage.get.call_args
        assert len(args) == 2
        assert args[0] == 'foo/bar.enc2'

        assert m_storage.put.call_count == 1
        args, kwargs = m_storage.put.call_args
        assert len(args) == 2
        assert args[1] == 'foo/bar.enc2'

        new_ciphertext.seek(0)
        new_plaintext = io.BytesIO()
        ke_crypto.decrypt_fileobj(new_key, new_ciphertext, new_plaintext, chunksize=1024)

        assert new_plaintext.getvalue() == plaintext
