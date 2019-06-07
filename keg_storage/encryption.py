"""Keg Storage Encryption Methods

Keg-Storage can automatically encrypt and decrypt data as it is saved or as it is fetched. To
enable encryption you must configuration your application with the required configuration items.
"""
import aws_encryption_sdk as aes
import attr

from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
from aws_encryption_sdk.identifiers import EncryptionKeyType, WrappingAlgorithm
from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
from aws_encryption_sdk.key_providers.base import MasterKeyProviderConfig


class EncryptionProvider:
    pass


class AWSEncryptionProvider(EncryptionProvider):
    def encrypt(self, in_bytes):
        return aes.stream(mode='e', source=in_bytes, key_provider=self.provider)

    def decrypt(self, in_bytes):
        return aes.stream(mode='d', source=in_bytes, key_provider=self.provider)


class KMSEncryptionProvider(AWSEncryptionProvider):
    def __init__(self, keys):
        self._keys = keys
        self.provider = aes.KMSMasterKeyProvider(key_ids=keys)


@attr.s(hash=True)
class StaticMasterKeyProviderConfig(MasterKeyProviderConfig):
    keys = attr.ib(
        hash=True,
        validator=attr.validators.instance_of((dict,)),
        converter=lambda keys: {
            kid: WrappingKey(wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                             wrapping_key=material,
                             wrapping_key_type=EncryptionKeyType.SYMMETRIC,)
            for kid, material in keys.items()}
    )


class StaticMasterKeyProvider(RawMasterKeyProvider):
    provider_id = 'static-master'

    _config_class = StaticMasterKeyProviderConfig

    def _get_raw_key(self, key_id):
        return self.config.keys[key_id]


class SymmetricKeyProvider(AWSEncryptionProvider):

    def __init__(self, encryption_keys):
        self.provider = StaticMasterKeyProvider(keys=encryption_keys)
        self.provider.add_master_keys_from_list(encryption_keys.keys())
