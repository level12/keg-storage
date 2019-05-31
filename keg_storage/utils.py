import logging
import tempfile

from blazeutils.helpers import ensure_list
import keg_elements.crypto as ke_crypto

DEFAULT_KEY_SIZE = 32

log = logging.getLogger(__name__)


class DecryptionException(Exception):
    pass


class EncryptionKeyException(Exception):
    pass


def verify_key_length(key, expected=DEFAULT_KEY_SIZE):
    return len(key) == expected


def reencrypt(storage, path, old_key, new_key):
    old_key = ensure_list(old_key)

    # We append the new key just in case the operation was restarted and we have already encrypted
    # some files
    keys = list(filter(verify_key_length, old_key + [new_key]))

    if not keys:
        raise EncryptionKeyException('No Keys Found')
    else:
        log.info('Found {} keys'.format(len(keys)))

    if not verify_key_length(new_key, expected=DEFAULT_KEY_SIZE):
        raise EncryptionKeyException('New key is not the correct size. Got {}, expecting {}'.format(
            len(new_key), DEFAULT_KEY_SIZE
        ))

    with tempfile.NamedTemporaryFile() as old_key_local, \
            tempfile.NamedTemporaryFile() as new_key_local:

        log.info('Fetching {}'.format(path))
        storage.get(path, old_key_local.name)

        old_key_bytes = None
        for idx, key in enumerate(keys):
            try:
                log.info('Trying to decrypt {}.'.format(path, len(keys)))
                old_key_bytes = ke_crypto.decrypt_bytesio(key, old_key_local.name)
                log.info('Successfully Decrypted {} with key {}.'.format(path, idx))
            except Exception as e:
                log.info('Key {} failed for {}'.format(idx, path))

                if str(e) == 'Invalid padding bytes.':
                    continue

                log.error('Unhandled error for decrypt with key {}: {}.'.format(idx, str(e)))

        if old_key_bytes is None:
            raise DecryptionException('Unable to Decrypt File {}'.format(path))

        log.info('Re-encrypting {}'.format(path))
        new_key_bytes = ke_crypto.encrypt_fileobj(new_key, old_key_bytes)

        log.info('Writing newly encrypted data {}.'.format(path))
        for chunk in new_key_bytes:
            new_key_local.write(chunk)
        new_key_local.flush()

        storage.put(new_key_local.name, path)
        log.info('Re-encryption complete for {}.'.format(path))
