import unittest

from EasyLambda.EasyIterator import EasyIterator
from EasyLambda.EasyIteratorDestination import EasyIteratorDestination
from EasyLambda.EasyIteratorDestinationFactory import EasyIteratorDestinationFactory
from EasyLambda.EasyIteratorSource import EasyIteratorSource
from EasyLambda.EasyIteratorSourceFactory import EasyIteratorSourceFactory
from EasyLambda.EasyLog import EasyLog


# noinspection PyMethodMayBeStatic
class EasyIteratorTest(unittest.TestCase):
    # SFTP server credentials used for testing
    sftp_username = 'sftp'
    sftp_password = 'sftp'
    sftp_port = 22
    sftp_base_path = '/home/sftp/'
    sftp_address = 'localhost'
    sftp_fingerprint = 'AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBFNOEIDqVWibjhKwJt4F0SR/DyOqw64+yMPRRJu/si2V+cYSxLkJzyYZ5yGI8sSJKfmEBW7WYNqr2DJsmgWkRBk='
    sftp_fingerprint_type = 'ecdsa-sha2-nistp256'

    # S3 bucket credentials
    s3_bucket_name = 'test_bucket.eonx.com'
    s3_base_path = 'test_path'

    # Test file contents
    test_gpg_file = 'This is a test'

    def test_create_s3_source(self):
        """
        Assert that we can create an S3 file source
        """
        EasyLog.test('Testing creation of S3 file source...')

        s3_source = EasyIteratorSourceFactory.create_s3_source(
            recursive=True,
            bucket_name=EasyIteratorTest.s3_bucket_name,
            base_path=EasyIteratorTest.s3_base_path,
            success_destinations=None,
            failure_destinations=None,
            delete_on_success=False,
            delete_on_failure=False
        )

        EasyLog.test('Asserting expected EasyIteratorSource return type...')
        self.assertTrue(isinstance(s3_source, EasyIteratorSource))

    def test_create_s3_destination(self):
        """
        Assert that we can create an S3 file destination
        """
        EasyLog.test('Testing creation of S3 file destination...')

        s3_destination = EasyIteratorDestinationFactory.create_s3_destination(
            bucket_name=EasyIteratorTest.s3_bucket_name,
            base_path=EasyIteratorTest.s3_base_path,
            create_logfile_on_completion=False,
            create_timestamped_folder=False,
            allow_overwrite=False
        )

        EasyLog.test('Asserting expected EasyIteratorDestination return type...')
        self.assertTrue(isinstance(s3_destination, EasyIteratorDestination))

    def test_create_sftp_source(self):
        """
        Assert that we can create an SFTP file source
        """
        EasyLog.test('Testing creation of SFTP server file source...')

        sftp_source = EasyIteratorSourceFactory.create_sftp_source(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            fingerprint=EasyIteratorTest.sftp_fingerprint,
            fingerprint_type=EasyIteratorTest.sftp_fingerprint_type,
            delete_on_success=False,
            delete_on_failure=False,
            recursive=True,
            port=EasyIteratorTest.sftp_port,
            base_path=EasyIteratorTest.sftp_base_path,
            success_destinations=None,
            failure_destinations=None
        )

        EasyLog.test('Asserting expected EasyIteratorSource return type...')
        self.assertTrue(isinstance(sftp_source, EasyIteratorSource))

    def test_create_sftp_destination(self):
        """
        Assert that we can create an S3 file destination
        """
        EasyLog.test('Testing creation of SFTP destination...')

        sftp_destination = EasyIteratorDestinationFactory.create_sftp_destination(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            fingerprint=EasyIteratorTest.sftp_fingerprint,
            fingerprint_type=EasyIteratorTest.sftp_fingerprint_type,
            port=EasyIteratorTest.sftp_port,
            base_path=EasyIteratorTest.sftp_base_path,
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
        )

        EasyLog.test('Asserting expected EasyIteratorDestination return type...')
        self.assertTrue(isinstance(sftp_destination, EasyIteratorDestination))

    def test_create_iterator(self):
        """
        Assert that we can create an iterator and add sources
        """
        EasyLog.test('Testing creation of EasyIterator...')

        EasyLog.test('Creating EasyIteratorSource...')
        sftp_source = EasyIteratorSourceFactory.create_sftp_source(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            fingerprint=EasyIteratorTest.sftp_fingerprint,
            fingerprint_type=EasyIteratorTest.sftp_fingerprint_type,
            delete_on_success=False,
            delete_on_failure=False,
            recursive=True,
            port=EasyIteratorTest.sftp_port,
            base_path=EasyIteratorTest.sftp_base_path,
            success_destinations=None,
            failure_destinations=None
        )

        EasyLog.test('Creating success EasyIteratorDestination...')
        sftp_success_destination = EasyIteratorDestinationFactory.create_sftp_destination(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            fingerprint=EasyIteratorTest.sftp_fingerprint,
            fingerprint_type=EasyIteratorTest.sftp_fingerprint_type,
            port=EasyIteratorTest.sftp_port,
            base_path=EasyIteratorTest.sftp_base_path,
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
        )

        EasyLog.test('Creating failure EasyIteratorDestination...')
        sftp_failure_destination = EasyIteratorDestinationFactory.create_sftp_destination(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            fingerprint=EasyIteratorTest.sftp_fingerprint,
            fingerprint_type=EasyIteratorTest.sftp_fingerprint_type,
            port=EasyIteratorTest.sftp_port,
            base_path=EasyIteratorTest.sftp_base_path,
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
        )

        EasyLog.test('Creating EasyIterator...')
        easy_iterator = EasyIterator()

        EasyLog.test('Asserting expected EasyIterator return type...')
        self.assertTrue(isinstance(easy_iterator, EasyIterator))

        EasyLog.test('Asserting no iteration success destinations...')
        iterator_sources = easy_iterator.get_sources()
        self.assertEqual(len(iterator_sources), 0)
        easy_iterator.add_source(source=sftp_source)
        EasyLog.test('Asserting expected number of iteration sources...')
        iterator_sources = easy_iterator.get_sources()
        self.assertEqual(len(iterator_sources), 1)
        EasyLog.test('Asserting expected EasyIteratorSource type...')
        self.assertTrue(isinstance(iterator_sources[0], EasyIteratorSource))

        EasyLog.test('Asserting no iteration success destinations...')
        iterator_success_destinations = easy_iterator.get_success_destinations()
        self.assertEqual(len(iterator_success_destinations), 0)
        EasyLog.test('Adding success destination...')
        easy_iterator.add_success_destination(destination=sftp_success_destination)
        EasyLog.test('Asserting expected number of iteration success destinations...')
        iterator_success_destinations = easy_iterator.get_success_destinations()
        self.assertEqual(len(iterator_success_destinations), 1)
        EasyLog.test('Asserting expected EasyIteratorDestination type...')
        self.assertTrue(isinstance(iterator_success_destinations[0], EasyIteratorDestination))

        EasyLog.test('Asserting no iteration failure destinations...')
        iterator_failure_destinations = easy_iterator.get_failure_destinations()
        self.assertEqual(len(iterator_failure_destinations), 0)
        EasyLog.test('Adding failure destination...')
        easy_iterator.add_failure_destination(destination=sftp_failure_destination)
        EasyLog.test('Asserting expected number of iteration failure destinations...')
        iterator_failure_destinations = easy_iterator.get_failure_destinations()
        self.assertEqual(len(iterator_failure_destinations), 1)
        EasyLog.test('Asserting expected EasyIteratorDestination type...')
        self.assertTrue(isinstance(iterator_failure_destinations[0], EasyIteratorDestination))

        EasyLog.test('Create test files in source filesystem...')

        # Store all test files in an array that we can use in a shared array with the callback, so we can validate all files were iterated as expected
        self.__expected_callback_filenames__ = []
        self.__found_callback_filenames__ = []
        for count in range(1, 5):
            self.__expected_callback_filenames__.append('test_file_{count}.txt')

        EasyLog.test('Starting iteration of files...')
        easy_iterator.iterate_sources(
            callback=self.easy_iterator_callback,
            maximum_files=None
        )

    def easy_iterator_callback(self, staked_filename) -> bool:
        """
        Assert iterator callback is working as expected

        :type staked_filename: str
        :param staked_filename: The current staked filename

        :return: bool
        """
        EasyLog.test('Iterator callback function triggered...')

        # Make sure the file was in the test files we created
        if staked_filename in self.__expected_callback_filenames__:
            EasyLog.test('Found expected file: {staked_filename}'.format(staked_filename=staked_filename))
            self.__found_callback_filenames__.append(staked_filename)
            return True
        else:
            # Ignore any other files
            EasyLog.test('Ignoring unknown file: {staked_filename}'.format(staked_filename=staked_filename))
            return False


if __name__ == '__main__':
    unittest.main()
