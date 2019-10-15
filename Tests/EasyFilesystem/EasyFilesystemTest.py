import filecmp
import unittest

# noinspection DuplicatedCode
import uuid

from EasyFilesystem.BaseFilesystem import BaseFilesystem
from EasyLocalDisk.Client import Client as LocalDiskClient
from EasyFilesystem.Sftp.Filesystem import Filesystem
from EasyLog.Log import Log


# noinspection DuplicatedCode
class EasyFilesystemSftpTest(unittest.TestCase):
    TEST_SFTP_ADDRESS = 'localhost'
    TEST_SFTP_PORT = 22
    TEST_SFTP_BASE_PATH = '/home/sftp/'
    TEST_SFTP_USERNAME = 'sftp'
    TEST_SFTP_PASSWORD = 'sftp'

    TEST_BUCKET_NAME = 'easy-lambda.test.eonx.com'
    TEST_BUCKET_BASE_PATH = 'easy-filesystem'

    def test_filesystems(self):
        """
        Test all filesystems
        """
        Log.test('Creating S3 Filesystem...')
        filesystem_s3 = Filesystem(
            bucket_name=EasyFilesystemS3Test.TEST_BUCKET_NAME,
            base_path=EasyFilesystemS3Test.TEST_BUCKET_BASE_PATH
        )

        Log.test('Creating SFTP Filesystem...')
        filesystem_sftp = Filesystem(
            address=EasyFilesystemSftpTest.TEST_SFTP_ADDRESS,
            port=EasyFilesystemSftpTest.TEST_SFTP_PORT,
            username=EasyFilesystemSftpTest.TEST_SFTP_USERNAME,
            password=EasyFilesystemSftpTest.TEST_SFTP_PASSWORD,
            base_path=EasyFilesystemSftpTest.TEST_SFTP_BASE_PATH,
            validate_fingerprint=False
        )


    def execute_filesystem_test(self, filesystem) -> None:
        """
        Test SFTP filesystem path creation
        """
        Log.test('Asserting Inheritance...')
        self.assertTrue(isinstance(filesystem_sftp, BaseFilesystem))
        self.assertTrue(isinstance(filesystem_sftp, Filesystem))

        # Create a temporary path
        Log.test('Creating Temporary Path...')
        filesystem_sftp.create_path(path='/tmp/', allow_overwrite=True)
        remote_temp_path = filesystem_sftp.create_temp_path()
        remote_temp_filename = '{remote_temp_path}/{uuid}.txt'.format(remote_temp_path=remote_temp_path, uuid=EasyFilesystemSftpTest.TEST_UUID)

        # Make sure the temporary path exists
        Log.test('Ensuring Temporary Path Exists...')
        Log.test(remote_temp_path)
        self.assertTrue(filesystem_sftp.path_exists(remote_temp_path))

        # Create a test file locally
        local_temp_path = LocalDiskClient.create_temp_path()
        local_temp_filename = '{local_temp_path}{uuid}.txt'.format(local_temp_path=local_temp_path, uuid=EasyFilesystemSftpTest.TEST_UUID)

        Log.test('Creating Local Test File...')
        file = open(local_temp_filename, 'wt')
        contents = ''
        for i in range(0, 100):
            contents += str(uuid.uuid4())
        file.write(contents)
        file.close()

        # Upload the file to the bucket
        Log.test('Uploading Test File...')
        self.assertFalse(filesystem_sftp.file_exists(filename=remote_temp_filename))
        filesystem_sftp.file_upload(
            remote_filename=remote_temp_filename,
            local_filename=local_temp_filename,
            allow_overwrite=False
        )
        self.assertTrue(filesystem_sftp.file_exists(filename=remote_temp_filename))

        # Download the file locally
        Log.test('Downloading Test File...')
        download_temp_filename = '{local_temp_filename}.download'.format(local_temp_filename=local_temp_filename)
        self.assertFalse(LocalDiskClient.file_exists(download_temp_filename))
        filesystem_sftp.file_download(local_filename=download_temp_filename, remote_filename=remote_temp_filename)
        self.assertTrue(LocalDiskClient.file_exists(download_temp_filename))

        # Compare the file the original
        Log.test('Comparing Downloaded File Contents...')
        self.assertTrue(filecmp.cmp(f1=download_temp_filename, f2=local_temp_filename))

        # Delete temporary file
        Log.test('Deleting Temporary File...')
        self.assertTrue(filesystem_sftp.file_exists(remote_temp_filename))
        filesystem_sftp.file_delete(remote_temp_filename)
        self.assertFalse(filesystem_sftp.file_exists(remote_temp_filename))

        # Delete temporary path
        Log.test('Deleting Temporary Path...')
        self.assertTrue(filesystem_sftp.path_exists(remote_temp_path))
        filesystem_sftp.path_delete(remote_temp_path)
        print(remote_temp_path)
        self.assertFalse(filesystem_sftp.path_exists(remote_temp_path))
