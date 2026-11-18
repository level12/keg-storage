from datetime import datetime
import logging
import tempfile
import typing

import arrow
from blazeutils.helpers import ensure_list


try:
    import keg_elements.crypto as ke_crypto
except ImportError:
    ke_crypto = None

DEFAULT_KEY_SIZE = 32

log = logging.getLogger(__name__)


class DecryptionException(Exception):
    pass


class EncryptionKeyException(Exception):
    pass


class MissingDependencyException(Exception):
    pass


def verify_key_length(key, expected=DEFAULT_KEY_SIZE):
    return len(key) == expected


def reencrypt(storage, path, old_key, new_key):
    if ke_crypto is None:
        raise MissingDependencyException('Keg Elements is required for crypto operations')

    old_key = ensure_list(old_key)

    # We append the new key just in case the operation was restarted and we have already encrypted
    # some files
    keys = list(filter(verify_key_length, old_key + [new_key]))

    if not keys:
        raise EncryptionKeyException('No Keys Found')
    else:
        log.info(f'Found {len(keys)} keys')

    if not verify_key_length(new_key, expected=DEFAULT_KEY_SIZE):
        raise EncryptionKeyException(
            f'New key is not the correct size. Got {len(new_key)}, expecting {DEFAULT_KEY_SIZE}',
        )

    with (
        tempfile.NamedTemporaryFile() as old_key_local,
        tempfile.NamedTemporaryFile() as new_key_local,
    ):
        log.info(f'Fetching {path}')
        storage.get(path, old_key_local.name)

        old_key_bytes = None
        for idx, key in enumerate(keys):
            try:
                log.info(f'Trying to decrypt {path}.')
                old_key_bytes = ke_crypto.decrypt_bytesio(key, old_key_local.name)
                log.info(f'Successfully Decrypted {path} with key {idx}.')
                break
            except Exception as e:
                log.info(f'Key {idx} failed for {path}')

                if str(e) == 'Invalid padding bytes.':
                    continue

                log.error(f'Unhandled error for decrypt with key {idx}: {str(e)}.')

        if old_key_bytes is None:
            raise DecryptionException(f'Unable to Decrypt File {path}')

        log.info(f'Re-encrypting {path}')
        new_key_bytes = ke_crypto.encrypt_fileobj(new_key, old_key_bytes)

        log.info(f'Writing newly encrypted data {path}.')
        for chunk in new_key_bytes:
            new_key_local.write(chunk)
        new_key_local.flush()

        storage.put(new_key_local.name, path)
        log.info(f'Re-encryption complete for {path}.')


def expire_time_to_seconds(
    expire_time: arrow.Arrow | datetime,
    *,
    now: typing.Callable[[], arrow.Arrow] = arrow.utcnow,
):
    _now = now()
    if isinstance(expire_time, datetime):
        expire_time = arrow.get(expire_time)
    if expire_time < _now:
        raise ValueError('Expiration time is in the past')
    return (expire_time - _now).total_seconds()
