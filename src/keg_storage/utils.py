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
    """Re-encrypt the object at ``path`` from one of ``old_key`` to ``new_key``.

    The stored format (keg_elements AES-CBC) has no MAC, so a successful decrypt only means the
    PKCS7 padding happened to be valid. A wrong key passes that check roughly 1 time in 256 and
    yields garbage. To avoid overwriting the object with re-encrypted garbage, every candidate key
    is tried and the object is only rewritten when exactly one old key decrypts it:

    - only ``new_key`` decrypts it: already re-encrypted (e.g. a restarted run), nothing is written.
    - more than one key decrypts it: ambiguous, ``DecryptionException`` is raised, nothing is
      written.
    - no key decrypts it: ``DecryptionException`` is raised, nothing is written.

    Residual risk: if the correct key is not supplied at all and a wrong key happens to produce
    valid padding, the object is still rewritten with garbage. Only authenticated encryption can
    rule that out.
    """
    if ke_crypto is None:
        raise MissingDependencyException('Keg Elements is required for crypto operations')

    old_keys = ensure_list(old_key)

    if not verify_key_length(new_key, expected=DEFAULT_KEY_SIZE):
        raise EncryptionKeyException(
            f'New key is not the correct size. Got {len(new_key)}, expecting {DEFAULT_KEY_SIZE}',
        )

    # The new key is included in case the operation was restarted and this file was already
    # re-encrypted. Duplicates are dropped so the same key can't be counted as two matches.
    keys = []
    for key in [*filter(verify_key_length, old_keys), new_key]:
        if key not in keys:
            keys.append(key)

    if len(keys) < 2:
        raise EncryptionKeyException('No Keys Found')

    log.info(f'Found {len(keys) - 1} old keys')

    with (
        tempfile.NamedTemporaryFile() as old_key_local,
        tempfile.NamedTemporaryFile() as new_key_local,
    ):
        log.info(f'Fetching {path}')
        storage.get(path, old_key_local.name)

        old_key_bytes = None
        matching_keys = []
        for idx, key in enumerate(keys):
            log.info(f'Trying to decrypt {path} with key {idx}.')
            try:
                plaintext = ke_crypto.decrypt_bytesio(key, old_key_local.name)
            except ValueError:
                # Bad padding or a ciphertext length that isn't a multiple of the block size.
                log.info(f'Key {idx} failed for {path}')
                continue

            log.info(f'Key {idx} decrypted {path}.')
            matching_keys.append(key)
            if old_key_bytes is None:
                old_key_bytes = plaintext

        if not matching_keys:
            raise DecryptionException(f'Unable to Decrypt File {path}')

        if len(matching_keys) > 1:
            raise DecryptionException(
                f'Multiple keys decrypt {path}; refusing to guess which is correct. '
                'The file has not been modified.',
            )

        if matching_keys[0] == new_key:
            log.info(f'{path} is already encrypted with the new key, skipping.')
            return

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
