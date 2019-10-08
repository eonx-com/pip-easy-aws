import os
import unittest

from EasyFilesystem.File import File
from EasyFilesystem.Filesystem.Factory import Factory
from EasyFilesystem.Filesystem.Sftp import Sftp
from EasyLog.Log import Log
from Tests.Helpers import Helpers


# noinspection DuplicatedCode
class SftpFilesystemTest(unittest.TestCase):
    sftp_username = 'sftp'
    sftp_password = 'sftp'
    sftp_address = 'localhost'
    sftp_port = 22
    sftp_base_path = '/home/sftp/'

    test_file_count = 5

    @staticmethod
    def __create_sftp_filesystem__() -> Sftp:
        """
        Return an SFTP filesystem for use in these tests

        :return: Sftp
        """
        Log.test('Creating SFTP filesystem...')
        return Factory.create_sftp_filesystem(
            username=SftpFilesystemTest.sftp_username,
            password=SftpFilesystemTest.sftp_password,
            address=SftpFilesystemTest.sftp_address,
            port=SftpFilesystemTest.sftp_port,
            base_path=SftpFilesystemTest.sftp_base_path,
            validate_fingerprint=False
        )
    
    def test_create_filesystem(self) -> None:
        """
        Test successful connection to SFTP server and subsequent disconnection

        :return: None
        """
        Log.test('Testing SFTP filesystem creation...')

        Log.test('Asserting correct filesystem type...')
        filesystem = SftpFilesystemTest.__create_sftp_filesystem__()
        self.assertTrue(isinstance(filesystem, Sftp))

    def test_file_list(self) -> None:
        """
        Test listing of files is successful

        :return: None
        """
        Log.test('Testing listing of files...')

        filesystem = SftpFilesystemTest.__create_sftp_filesystem__()
        files = filesystem.file_list(recursive=True)

        Log.test('Asserting return type is a list...')
        self.assertTrue(isinstance(files, list))

        Log.test('Asserting all list items are file objects...')
        for file in files:
            self.assertTrue(file, File)

        Log.test('Creating local test file...')
        local_test_filename = Helpers.create_local_test_file()

        Log.test('Uploading test files...')

        uploaded_files = []
        for i in range(0, SftpFilesystemTest.test_file_count):
            filename = '{base_path}/test_sftp_file_list_success/{i}/{local_filename}'.format(
                i=i,
                base_path=SftpFilesystemTest.sftp_base_path,
                local_filename=local_test_filename
            )
            Log.test('Uploading: {filename}...'.format(filename=filename))

            uploaded_files.append(
                filesystem.file_upload(
                    filename=filename,
                    local_filename=local_test_filename,
                    allow_overwrite=False
                )
            )

        # Assert we can find the files on SFTP server by listing each specific directory
        Log.test('Testing listing of each specific path...')

        uploaded_file: File
        for uploaded_file in uploaded_files:
            path = os.path.dirname(uploaded_file.get_filename())

            uploaded_files_found = filesystem.file_list(path=path, recursive=False)

            found_filename = False
            uploaded_file_found: File
            for uploaded_file_found in uploaded_files_found:
                print(uploaded_file_found.get_filename())
                print(uploaded_file.get_filename())
                if filename == uploaded_file.get_filename():
                    Log.test('Found: {filename}'.format(filename=filename))
                    found_filename = True
                    break

            self.assertTrue(found_filename)

        exit(0)

        # Assert we can find all the files by performing a recursive search of the base directory
        Log.test('Testing recursive directory listing...')
        Log.test('Listing files in remote path: {remote_path}...'.format(remote_path=SftpFilesystemTest.sftp_base_path))
        files = sftp_client.file_list(remote_path=SftpFilesystemTest.sftp_base_path, recursive=True)

        Log.test('Searching for test files...')
        found_files = 0
        for i in range(0, 5):
            for filename in files:
                if filename == remote_filenames[i]:
                    Log.test('Found: {remote_filename}'.format(remote_filename=remote_filenames[i]))
                    found_files += 1
                    if found_files == test_file_count:
                        Log.test('Found all test files successfully')
                    break

        Log.test('Asserting all test files found...')
        self.assertEqual(found_files, test_file_count)

        # Assert we can find all the files by performing a recursive search of the base directory
        Log.test('Testing non-recursive directory listing...')
        Log.test('Listing files in remote path: {remote_path}...'.format(remote_path=SftpFilesystemTest.sftp_base_path))
        files = sftp_client.file_list(remote_path=SftpFilesystemTest.sftp_base_path, recursive=False)

        Log.test('Searching for test files...')
        found_files = 0
        for i in range(0, 5):
            for filename in files:
                if filename == remote_filenames[i]:
                    Log.test('Found: {remote_filename}'.format(remote_filename=remote_filenames[i]))
                    found_files += 1
                    break

        Log.test('Asserting no test files found...')
        self.assertEqual(found_files, 0)