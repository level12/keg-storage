import keg_storage.s3


class TestS3Storage:
    def test_init_sets_up_correctly(self):
        s3 = keg_storage.s3.S3Storage('bucket', 'key', 'secret', name='test')
        assert s3.name == 'test'
