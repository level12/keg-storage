import tempfile

import keg_elements.crypto as ke_crypto


def reencrypt(storage, path, old_key, new_key):
    with tempfile.NamedTemporaryFile() as old_key_local, \
            tempfile.NamedTemporaryFile() as new_key_local:
        storage.get(path, old_key_local.name)

        old_key_bytes = ke_crypto.decrypt_bytesio(old_key, old_key_local.name)
        new_key_bytes = ke_crypto.encrypt_fileobj(new_key, old_key_bytes)

        for chunk in new_key_bytes:
            new_key_local.write(chunk)
        new_key_local.flush()

        new_path = path if path.endswith('.enc2') else path.replace('.enc', '.enc2')
        storage.put(new_key_local.name, new_path)
