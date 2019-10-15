import filecmp
import unittest

# noinspection DuplicatedCode
import uuid

from EasyFilesystem.BaseFilesystem import BaseFilesystem
from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyFilesystem.Sftp.Filesystem import Filesystem as FilesystemSftp
from EasyFilesystem.S3.Filesystem import Filesystem as FilesystemS3
from EasyLog.Log import Log


# noinspection DuplicatedCode,PyMethodMayBeStatic
class EasyFilesystemTest(unittest.TestCase):
    TEST_SFTP_ADDRESS = 'localhost'
    TEST_SFTP_PORT = 22
    TEST_SFTP_BASE_PATH = '/home/sftp/'
    TEST_SFTP_USERNAME = 'sftp'
    TEST_SFTP_PASSWORD = 'sftp'

    TEST_BUCKET_NAME = 'easy-lambda.test.eonx.com'
    TEST_BUCKET_BASE_PATH = 'easy-filesystem'

    callback_files = []

    def test_filesystems(self):
        """
        Test all filesystems
        """
        test_filesystems = []

        Log.test('Creating S3 Filesystem...')
        test_filesystems.append(FilesystemS3(
            bucket_name=EasyFilesystemTest.TEST_BUCKET_NAME,
            base_path=EasyFilesystemTest.TEST_BUCKET_BASE_PATH
        ))

        Log.test('Creating SFTP Filesystem...')
        test_filesystems.append(FilesystemSftp(
            address=EasyFilesystemTest.TEST_SFTP_ADDRESS,
            port=EasyFilesystemTest.TEST_SFTP_PORT,
            username=EasyFilesystemTest.TEST_SFTP_USERNAME,
            password=EasyFilesystemTest.TEST_SFTP_PASSWORD,
            base_path=EasyFilesystemTest.TEST_SFTP_BASE_PATH,
            validate_fingerprint=False
        ))

        for test_filesystem in test_filesystems:
            Log.test('Testing Filesystem Type: {type}'.format(type=type(test_filesystem)))
            self.execute_filesystem_test(test_filesystem)
            del test_filesystem

    def execute_filesystem_test(self, filesystem) -> None:
        """
        Test SFTP filesystem path creation
        """
        test_uuid = uuid.uuid4()

        Log.test('Asserting Inheritance...')
        self.assertTrue(isinstance(filesystem, BaseFilesystem))

        # Create a temporary path
        Log.test('Creating Temporary Path...')
        filesystem.create_path(path='/tmp/', allow_overwrite=True)
        remote_temp_path = filesystem.create_temp_path()
        remote_temp_filename = '{remote_temp_path}/{uuid}.txt'.format(remote_temp_path=remote_temp_path, uuid=test_uuid)

        # Make sure the temporary path exists
        Log.test('Ensuring Temporary Path Exists...')
        Log.test(remote_temp_path)
        self.assertTrue(filesystem.path_exists(remote_temp_path))

        # Create a test file locally
        local_temp_path = LocalDiskClient.create_temp_path()
        local_temp_filename = '{local_temp_path}{uuid}.txt'.format(local_temp_path=local_temp_path, uuid=test_uuid)

        Log.test('Creating Local Test File...')
        file = open(local_temp_filename, 'wt')
        contents = ''
        for i in range(0, 100):
            contents += str(uuid.uuid4())
        file.write(contents)
        file.close()

        # Upload the file to the bucket
        Log.test('Uploading Test File...')
        self.assertFalse(filesystem.file_exists(filename=remote_temp_filename))
        filesystem.file_upload(
            remote_filename=remote_temp_filename,
            local_filename=local_temp_filename,
            allow_overwrite=False
        )
        self.assertTrue(filesystem.file_exists(filename=remote_temp_filename))

        # Download the file locally
        Log.test('Downloading Test File...')
        download_temp_filename = '{local_temp_filename}.download'.format(local_temp_filename=local_temp_filename)
        self.assertFalse(LocalDiskClient.file_exists(download_temp_filename))
        filesystem.file_download(local_filename=download_temp_filename, remote_filename=remote_temp_filename)
        self.assertTrue(LocalDiskClient.file_exists(download_temp_filename))

        # Compare the file the original
        Log.test('Comparing Downloaded File Contents...')
        self.assertTrue(filecmp.cmp(f1=download_temp_filename, f2=local_temp_filename))

        # Delete temporary file
        Log.test('Deleting Temporary File...')
        self.assertTrue(filesystem.file_exists(remote_temp_filename))
        filesystem.file_delete(remote_temp_filename)
        self.assertFalse(filesystem.file_exists(remote_temp_filename))

        # Delete temporary path
        Log.test('Deleting Temporary Path...')
        self.assertTrue(filesystem.path_exists(remote_temp_path))
        filesystem.path_delete(remote_temp_path)
        self.assertFalse(filesystem.path_exists(remote_temp_path))

        # Upload test file multiple times
        Log.test('Uploading Recursive Download Test Files...')
        remote_recursive_path = '{remote_temp_path}/recursive/'.format(remote_temp_path=remote_temp_path)
        for i in range(1, 10):
            recursive_temp_filename = '{remote_recursive_path}recursive-{i}.txt'.format(remote_recursive_path=remote_recursive_path, i=i)
            self.assertFalse(filesystem.file_exists(filename=recursive_temp_filename))

            Log.test('Uploading Test File: {recursive_temp_filename}...'.format(recursive_temp_filename=recursive_temp_filename))
            filesystem.file_upload(
                remote_filename=recursive_temp_filename,
                local_filename=local_temp_filename,
                allow_overwrite=False
            )
            self.assertTrue(filesystem.file_exists(filename=recursive_temp_filename))

        # Perform recursive download
        Log.test('Starting Recursive Download Test...')
        EasyFilesystemTest.callback_files = []
        filesystem.file_download_recursive(
            remote_path=remote_recursive_path,
            local_path=local_temp_path,
            callback=self.recursive_download_callback,
            allow_overwrite=False
        )

        Log.test('Comparing Recursively Downloaded Files...')
        for filename_current in EasyFilesystemTest.callback_files:
            Log.test('Comparing: {filename_current}'.format(filename_current=filename_current['local_filename']))
            filecmp.cmp(f1=filename_current['local_filename'], f2=local_temp_filename)

    def recursive_download_callback(self, local_filename, remote_filename) -> bool:
        """
        Test recursive download callback

        :type local_filename: str
        :param local_filename:

        :type remote_filename: str
        :param remote_filename:

        :return: bool
        """
        Log.test('Downloaded: {remote_filename}'.format(remote_filename=remote_filename))

        EasyFilesystemTest.callback_files.append({
            'local_filename': local_filename,
            'remote_filename': remote_filename
        })
        # Continue iterating
        return True
