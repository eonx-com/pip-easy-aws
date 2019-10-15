import unittest
import uuid

from TestHelpers.LazyHelpers import LazyHelpers
from EasyFilesystem.EasyIterator import Iterator
from EasyFilesystem.Iterator import Destination
from EasyFilesystem.EasyIteratorDestinationFactory import IteratorDestinationFactory
from EasyFilesystem.Iterator import Source
from EasyFilesystem.EasyIteratorSourceFactory import IteratorSourceFactory
from EasyLog.Log import Log

# noinspection PyMethodMayBeStatic
from EasyLambda.EasySftp import EasySftp


# noinspection DuplicatedCode
class EasyIteratorTest(unittest.TestCase):
    # SFTP server credentials used for testing
    sftp_username = 'sftp'
    sftp_password = 'sftp'
    sftp_port = 22
    sftp_base_path = '/home/sftp/'
    sftp_address = 'localhost'

    def test_create_s3_source(self):
        """
        Assert that we can create an S3 file source
        """
        Log.test('Testing creation of S3 file source...')

        s3_bucket_name = 'easy-iterator-test.{uuid}.eonx.com'.format(uuid=uuid.uuid4())
        s3_source = IteratorSourceFactory.create_s3_source(
            recursive=True,
            bucket_name=s3_bucket_name,
            base_path='source',
            success_destinations=None,
            failure_destinations=None,
            delete_on_success=False,
            delete_on_failure=False
        )

        Log.test('Asserting expected EasyIteratorSource return type...')
        self.assertTrue(isinstance(s3_source, Source))

    def test_create_s3_destination(self):
        """
        Assert that we can create an S3 file destination
        """
        Log.test('Testing creation of S3 file destination...')

        s3_bucket_name = 'easy-iterator-test.{uuid}.eonx.com'.format(uuid=uuid.uuid4())
        s3_destination = IteratorDestinationFactory.create_s3_destination(
            bucket_name=s3_bucket_name,
            base_path='destination',
            create_logfile_on_completion=False,
            create_timestamped_folder=False,
            allow_overwrite=False
        )

        Log.test('Asserting expected EasyIteratorDestination return type...')
        self.assertTrue(isinstance(s3_destination, Destination))

    def test_create_sftp_source(self):
        """
        Assert that we can create an SFTP file source
        """
        Log.test('Testing creation of SFTP server file source...')

        sftp_source = IteratorSourceFactory.create_sftp_source(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            validate_fingerprint=False,
            delete_on_success=False,
            delete_on_failure=False,
            recursive=True,
            port=EasyIteratorTest.sftp_port,
            base_path='{base_path}/source'.format(base_path=EasyIteratorTest.sftp_base_path),
            success_destinations=None,
            failure_destinations=None
        )

        Log.test('Asserting expected EasyIteratorSource return type...')
        self.assertTrue(isinstance(sftp_source, Source))

    def test_create_sftp_destination(self):
        """
        Assert that we can create an S3 file destination
        """
        Log.test('Testing creation of SFTP destination...')

        sftp_destination = IteratorDestinationFactory.create_sftp_destination(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            validate_fingerprint=False,
            port=EasyIteratorTest.sftp_port,
            base_path='{base_path}/destination'.format(base_path=EasyIteratorTest.sftp_base_path),
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
        )

        Log.test('Asserting expected EasyIteratorDestination return type...')
        self.assertTrue(isinstance(sftp_destination, Destination))

    def test_iterator(self):
        """
        Assert that we can create an iterator and add sources
        """
        Log.test('Testing creation of EasyIterator...')

        # Generate unique test paths
        test_file_count = 5
        test_local_path = LazyHelpers.create_unique_local_temp_path()
        test_uuid = uuid.uuid4()

        base_path_local = EasySftp.sanitize_filename('/{test_uuid}'.format(test_uuid=test_uuid))
        base_path_remote = '/{base_path}/{local_base_path}'.format(base_path=EasyIteratorTest.sftp_base_path, local_base_path=base_path_local)

        test_source_path = EasySftp.sanitize_filename('/{remote_base_path}/source'.format(remote_base_path=base_path_remote))
        test_destination_success_path = EasySftp.sanitize_filename('/{remote_base_path}//destination-success'.format(remote_base_path=base_path_remote))
        test_destination_failure_path = EasySftp.sanitize_filename('/{remote_base_path}//destination-failure'.format(remote_base_path=base_path_remote))

        Log.test('Local test path: {test_local_path}'.format(test_local_path=test_local_path))
        Log.test('Remote source: {test_source_path}'.format(test_source_path=test_source_path))
        Log.test('Remote success destination: {test_destination_success_path}'.format(test_destination_success_path=test_destination_success_path))
        Log.test('Remote failure destination: {test_destination_failure_path}'.format(test_destination_failure_path=test_destination_failure_path))

        Log.test('Creating source...')
        sftp_source = IteratorSourceFactory.create_sftp_source(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            validate_fingerprint=False,
            delete_on_success=False,
            delete_on_failure=False,
            recursive=True,
            port=EasyIteratorTest.sftp_port,
            base_path=test_source_path,
            success_destinations=None,
            failure_destinations=None
        )

        Log.test('Creating success destination...')
        sftp_success_destination = IteratorDestinationFactory.create_sftp_destination(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            validate_fingerprint=False,
            port=EasyIteratorTest.sftp_port,
            base_path=test_destination_success_path,
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
        )

        Log.test('Creating failure destination...')
        sftp_failure_destination = IteratorDestinationFactory.create_sftp_destination(
            address=EasyIteratorTest.sftp_address,
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            validate_fingerprint=False,
            port=EasyIteratorTest.sftp_port,
            base_path=test_destination_failure_path,
            create_timestamped_folder=False,
            create_logfile_on_completion=False,
            allow_overwrite=False
        )

        Log.test('Creating iterator...')
        easy_iterator = Iterator()

        Log.test('Asserting no iteration success destinations...')
        iterator_sources = easy_iterator.get_sources()
        self.assertEqual(len(iterator_sources), 0)

        Log.test('Adding and asserting addition of iterator source...')
        easy_iterator.add_source(source=sftp_source)
        iterator_sources = easy_iterator.get_sources()
        self.assertEqual(len(iterator_sources), 1)
        self.assertTrue(isinstance(iterator_sources[0], Source))

        Log.test('Adding and asserting addition of iterator success destination...')
        iterator_success_destinations = easy_iterator.get_destinations_success()
        self.assertEqual(len(iterator_success_destinations), 0)
        easy_iterator.add_destination_success(destination=sftp_success_destination)
        iterator_success_destinations = easy_iterator.get_destinations_success()
        self.assertEqual(len(iterator_success_destinations), 1)
        self.assertTrue(isinstance(iterator_success_destinations[0], Destination))

        Log.test('Adding and asserting addition of iterator failure destination...')
        iterator_failure_destinations = easy_iterator.get_destinations_failure()
        self.assertEqual(len(iterator_failure_destinations), 0)
        easy_iterator.add_destination_failure(destination=sftp_failure_destination)
        iterator_failure_destinations = easy_iterator.get_destinations_failure()
        self.assertEqual(len(iterator_failure_destinations), 1)
        self.assertTrue(isinstance(iterator_failure_destinations[0], Destination))

        # Create test files on the source SFTP server
        Log.test('Uploading test files to source filesystem...')

        sftp_client = EasySftp()
        sftp_client.disable_fingerprint_validation()
        sftp_client.connect_password(
            username=EasyIteratorTest.sftp_username,
            password=EasyIteratorTest.sftp_password,
            address=EasyIteratorTest.sftp_address,
            port=EasyIteratorTest.sftp_port
        )

        Log.test('Creating local test file...')
        local_test_filename = LazyHelpers.create_local_test_file()

        Log.test('Uploading test files to SFTP server...')
        remote_filenames = {}
        for i in range(0, test_file_count):
            remote_filenames[i] = EasySftp.sanitize_filename('/{test_source_path}/{i}/{local_filename}'.format(
                i=i,
                test_source_path=test_source_path,
                local_filename=local_test_filename
            ))
            Log.test('Uploading: {remote_filename}...'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_upload(
                local_filename=local_test_filename,
                remote_filename=remote_filenames[i]
            )

        # Now we can start the iterator and assert that we find all of the files we uploaded
        self.callback_test_iterator_files = []

        Log.test('Starting iteration of files...')
        easy_iterator.iterate_sources(
            callback=self.callback_test_iterator,
            maximum_files=None
        )

        Log.test('Searching iterator results for uploaded files...')
        found_file_count = 0
        for i in range(0, test_file_count):
            for staked_file in self.callback_test_iterator_files:
                expected_filename = remote_filenames[i][len(test_source_path):]
                if staked_file.get_remote_filename_original() == expected_filename:
                    Log.test('Found: {remote_filename}'.format(remote_filename=remote_filenames[i]))
                    found_file_count += 1
                    break

        Log.test('Asserting all expected files iterated...')
        self.assertEqual(found_file_count, test_file_count)

    def callback_test_iterator(self, staked_file) -> bool:
        """
        Callback for testing iterator

        :type staked_file: EasyIteratorStakedFile
        :param staked_file: Staked file details

        :return: bool
        """
        Log.test('Iterator callback triggered...')
        Log.test('Current Filename: {remote_filename_current}'.format(remote_filename_current=staked_file.get_remote_staked_filename()))
        Log.test('Original Filename: {remote_filename_original}'.format(remote_filename_original=staked_file.get_remote_filename_original()))
        Log.test('Local Filename: {local_filename}'.format(local_filename=staked_file.get_staked_filename()))

        if self.callback_test_iterator_files is None:
            self.callback_test_iterator_files = []

        self.callback_test_iterator_files.append(staked_file)

        return True


if __name__ == '__main__':
    unittest.main()
