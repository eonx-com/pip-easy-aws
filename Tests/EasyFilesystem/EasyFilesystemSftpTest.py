import os
import unittest

from EasyFilesystem.File import File
from EasyFilesystem.Filesystem.Factory import Factory
from EasyFilesystem.Filesystem.Sftp import Sftp
from EasyLog.Log import Log
from Tests.Helpers import Helpers


# noinspection DuplicatedCode
class EasyFilesystemSftpTest(unittest.TestCase):
    sftp_username = 'sftp'
    sftp_password = 'sftp'
    sftp_address = 'localhost'
    sftp_port = 22
    sftp_base_path = '/home/sftp/'

    def test_filesystem(self) -> None:
        """
        Test functionality of the SFTP filesystem object

        :return: None
        """
        Log.test('Creating SFTP filesystem...')
        filesystem = Factory.create_sftp_filesystem(
            username=EasyFilesystemSftpTest.sftp_username,
            password=EasyFilesystemSftpTest.sftp_password,
            address=EasyFilesystemSftpTest.sftp_address,
            port=EasyFilesystemSftpTest.sftp_port,
            base_path=EasyFilesystemSftpTest.sftp_base_path,
            validate_fingerprint=False
        )

        Log.test('Asserting correct filesystem type...')
        self.assertTrue(isinstance(filesystem, Sftp))

        Log.test('Creating local test file...')
        local_test_filename = Helpers.create_local_test_file()

        Log.test('Uploading test files...')
        test_file_count = 5
        uploaded_files = []

        for i in range(0, test_file_count):
            filename = '{base_path}/test_sftp_file_list_success/{i}/{local_filename}'.format(
                i=i,
                base_path=EasyFilesystemSftpTest.sftp_base_path,
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

        Log.test('Testing listing of files executed ...')
        files = filesystem.file_list(recursive=True)

        Log.test('Asserting return type is a list...')
        self.assertTrue(isinstance(files, list), 'Failed to assert "EasyFilesystem.file_list()" returned a list object')

        Log.test('Asserting all list items are file objects...')
        for file in files:
            self.assertTrue(isinstance(file, File), 'Failed to assert "EasyFilesystem.file_list()" object was a valid "EasyFilesystem.File" type')

        # Assert we can find the files on SFTP server by listing each specific directory
        Log.test('Testing listing of each specific path...')

        uploaded_file_current: File
        for uploaded_file_current in uploaded_files:
            path = os.path.dirname(uploaded_file_current.get_filename())
            uploaded_files_found = filesystem.file_list(path=path, recursive=False)
            found_filename = False
            uploaded_file_found: File
            for uploaded_file_found in uploaded_files_found:
                if uploaded_file_found.get_filename() == uploaded_file_current.get_filename():
                    Log.test('Found: {filename}'.format(filename=uploaded_file_found.get_filename()))
                    found_filename = True
                    break

            self.assertTrue(found_filename, 'Failed to find all uploaded files via non-recursive "EasyFilesystem.file_list()"')

        # Assert we can find all the files again by performing a recursive search of the base directory
        Log.test('Testing recursive directory listing...')

        uploaded_files_found = filesystem.file_list(recursive=True)

        uploaded_file_current: File
        for uploaded_file_current in uploaded_files:
            found_filename = False
            uploaded_file_found: File
            for uploaded_file_found in uploaded_files_found:
                if uploaded_file_found.get_filename() == uploaded_file_current.get_filename():
                    Log.test('Found: {filename}'.format(filename=uploaded_file_found.get_filename()))
                    found_filename = True
                    break

            self.assertTrue(found_filename, 'Failed to find all uploaded files via recursive "EasyFilesystem.file_list()"')

        # Test non-recursive directory listing from the same base folder does not find these files
        uploaded_files_found = filesystem.file_list(recursive=False)

        uploaded_file_current: File
        for uploaded_file_current in uploaded_files:
            found_filename = False
            uploaded_file_found: File
            for uploaded_file_found in uploaded_files_found:
                if uploaded_file_found.get_filename() == uploaded_file_current.get_filename():
                    Log.test('Found: {filename}'.format(filename=uploaded_file_found.get_filename()))
                    found_filename = True
                    break

            Log.test('Asserting test file not found...')
            self.assertFalse(found_filename, 'Failed to assert that non-recursive "EasyFilesystem.list_files()" could not find files in sub-folders')
