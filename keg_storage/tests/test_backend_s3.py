import keg_storage.backends as backends


class TestS3Storage:
    def test_init_sets_up_correctly(self):
        s3 = backends.S3Storage('bucket', 'key', 'secret', name='test')
        assert s3.name == 'test'
