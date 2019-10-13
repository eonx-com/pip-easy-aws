import os
import unittest

from EasyLog.Log import Log
from EasySftp.Client import Client
from EasySftp.ClientError import ClientError
from Tests.Helpers import Helpers


# noinspection PyBroadException,DuplicatedCode
class EasySftpTest(unittest.TestCase):
    # SFTP server credentials used for testing
    sftp_username = 'sftp'
    sftp_password = 'sftp'
    sftp_port = 22
    sftp_base_path = '/home/sftp/'
    sftp_address = 'localhost'

    callback_results = []

    def test_host_fingerprint(self) -> None:
        """
        Attempt to connect to the SFTP server with host key checking enable- but no valid host key supplied. Connection should fail.

        :return: None
        """
        Log.test('Testing SFTP server host key validation...')

        # Get SFTP client (there is no need to explicitly enable host key checking as it should be on by default)
        sftp_client = Client()

        # Connect with username/password and assert we receive an base_exception error
        exception = None

        try:
            Log.test('Connecting to SFTP server with unknown host key/fingerprint...')
            sftp_client.connect_password(
                username=EasySftpTest.sftp_username,
                password=EasySftpTest.sftp_password,
                address=EasySftpTest.sftp_address,
                port=EasySftpTest.sftp_port
            )
        except Exception as connect_exception:
            Log.test('Received base_exception error: {connect_exception}'.format(connect_exception=connect_exception))
            exception = str(connect_exception)

        self.assertTrue(exception == 'No hostkey for host localhost found.')

        # Disable host key checking and ensure it works
        Log.test('Disabling host key checks...')
        sftp_client.disable_fingerprint_validation()

        Log.test('Reattempting connection...')
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        Log.test('Checking connection...')
        self.assertTrue(sftp_client.is_connected())

    def test_file_list(self) -> None:
        """
        Test listing of files is successful

        :return: None
        """
        test_file_count = 5

        Log.test('Testing list files...')

        Log.test('Connecting to SFTP server...')
        sftp_client = Client()
        sftp_client.disable_fingerprint_validation()
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        Log.test('Listing files...')
        files = sftp_client.file_list(
            remote_path=EasySftpTest.sftp_base_path,
            recursive=True
        )

        Log.test('Asserting return type...')
        self.assertTrue(isinstance(files, list))

        Log.test('Creating local test file...')
        local_test_filename = Helpers.create_local_test_file()

        Log.test('Uploading test files to SFTP server...')
        remote_filenames = {}
        for i in range(0, test_file_count):
            remote_filenames[i] = Client.sanitize_path('{base_path}/test_sftp_file_list_success/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))
            Log.test('Uploading: {remote_filename}...'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_upload(
                local_filename=local_test_filename,
                remote_filename=remote_filenames[i]
            )

        # Assert we can find the files on SFTP server by listing each specific directory
        Log.test('Testing listing of specific path...')
        for i in range(0, test_file_count):
            remote_path = os.path.dirname(remote_filenames[i])
            Log.test('Listing files in remote path: {remote_path}...'.format(remote_path=remote_path))

            files = sftp_client.file_list(remote_path=remote_path, recursive=False)
            found_filename = False
            for filename in files:
                if filename == remote_filenames[i]:
                    Log.test('Found: {filename}'.format(filename=filename))
                    found_filename = True
                    break

            self.assertTrue(found_filename)

        # Assert we can find all the files by performing a recursive search of the base directory
        Log.test('Testing recursive directory listing...')
        Log.test('Listing files in remote path: {remote_path}...'.format(remote_path=EasySftpTest.sftp_base_path))
        files = sftp_client.file_list(remote_path=EasySftpTest.sftp_base_path, recursive=True)

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
        Log.test('Listing files in remote path: {remote_path}...'.format(remote_path=EasySftpTest.sftp_base_path))
        files = sftp_client.file_list(remote_path=EasySftpTest.sftp_base_path, recursive=False)

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

    def test_file_download_overwrite(self) -> None:
        test_file_count = 5

        Log.test('Testing SFTP download...')

        Log.test('Connecting to SFTP server...')
        sftp_client = Client()
        sftp_client.disable_fingerprint_validation()
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        Log.test('Creating local test file...')
        local_test_filename = Helpers.create_local_test_file()

        remote_filenames = {}
        for i in range(0, test_file_count):
            remote_filenames[i] = Client.sanitize_path('{base_path}/test_sftp_download_overwrite/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))
            Log.test('Uploading test file: {remote_filename}...'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_upload(
                local_filename=local_test_filename,
                remote_filename=remote_filenames[i]
            )

        for i in range(0, test_file_count):
            local_download_filename = Helpers.create_unique_local_filename()
            remote_filename = Client.sanitize_path('{base_path}/test_sftp_download_overwrite/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))

            Log.test('Downloading test file: {remote_filename}...'.format(remote_filename=remote_filename))
            sftp_client.file_download(
                remote_filename=remote_filename,
                local_filename=local_download_filename
            )

            # Try to download existing file without overwrite enabled
            Log.test('Downloading same test file again with allow overwrite disabled...')
            try:
                sftp_client.file_download(
                    remote_filename=remote_filename,
                    local_filename=local_download_filename,
                    allow_overwrite=False
                )
            except Exception as download_exception:
                exception = str(download_exception)

                Log.test('Asserting expected base_exception received...')
                self.assertEqual(ClientError.ERROR_FILE_DOWNLOAD_DESTINATION_EXISTS, exception)

            # Perform the same download, with allow overwrite enabled
            Log.test('Downloading same test file again with allow overwrite enabled...')
            sftp_client.file_download(
                remote_filename=remote_filename,
                local_filename=local_download_filename,
                allow_overwrite=True
            )

    def test_file_upload_download(self) -> None:
        """
        Test listing of files is successful

        :return: None
        """
        test_file_count = 5

        Log.test('Testing SFTP download...')

        Log.test('Connecting to SFTP server...')
        sftp_client = Client()
        sftp_client.disable_fingerprint_validation()
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        Log.test('Creating local test file...')
        local_test_filename = Helpers.create_local_test_file()

        Log.test('Reading test file contents...')
        local_test_file = open(local_test_filename, "rt")
        original_content = local_test_file.read()
        local_test_file.close()

        Log.test('Uploading test files to SFTP server...')

        remote_filenames = {}
        for i in range(0, test_file_count):
            remote_filenames[i] = Client.sanitize_path('{base_path}/test_sftp_file_list_success/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))
            Log.test('Uploading test file: {remote_filename}...'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_upload(
                local_filename=local_test_filename,
                remote_filename=remote_filenames[i]
            )

        # Test downloading of test files individually
        for i in range(0, test_file_count):
            local_filename = Helpers.create_unique_local_filename()
            Log.test('Downloading test file: {remote_filename}'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_download(
                local_filename=local_filename,
                remote_filename=remote_filenames[i]
            )

            Log.test('Asserting content matches original file...')
            test_file = open(local_filename, 'rt')
            test_file_contents = test_file.read()
            test_file.close()
            self.assertEqual(original_content, test_file_contents)

        # Perform recursive download and make sure all test files get downloaded
        Log.test('Testing recursive download...')
        self.found_files = []

        # Download all files from the SFTP server to a local test path, a callback function will store the details of downloaded files
        local_path = Helpers.create_unique_local_temp_path()
        sftp_client.file_download_recursive(
            remote_path=EasySftpTest.sftp_base_path,
            local_path=local_path,
            callback=self.callback_file_download
        )

        # Go through all the files that were captures by the callback function and make sure we found everything we uploaded
        count_found = 0
        for downloaded_file in self.found_files:
            for i in range(0, test_file_count):
                if downloaded_file['remote_filename'] == remote_filenames[i]:
                    Log.test('Found: {remote_filename}'.format(remote_filename=downloaded_file['remote_filename']))
                    count_found += 1

        Log.test('Asserting all uploaded test files found...')
        self.assertEqual(count_found, test_file_count)

    def test_file_download_limit(self) -> None:
        """
        Assert download file limit works

        :return: None
        """
        test_file_count = 5
        test_file_download_limit = 2

        Log.test('Testing SFTP download...')

        Log.test('Connecting to SFTP server...')
        sftp_client = Client()
        sftp_client.set_file_download_limit(test_file_download_limit)
        sftp_client.disable_fingerprint_validation()
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        Log.test('Creating local test file...')
        local_test_filename = Helpers.create_local_test_file()

        Log.test('Uploading test files to SFTP server...')
        remote_filenames = {}
        for i in range(0, test_file_count):
            remote_filenames[i] = Client.sanitize_path('{base_path}/test_file_download_limit/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))
            Log.test('Uploading test file: {remote_filename}...'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_upload(
                local_filename=local_test_filename,
                remote_filename=remote_filenames[i]
            )

        # Download all files from the SFTP server to a local test path, a callback function will store the details of downloaded files
        Log.test('Downloading test files...')
        local_path = Helpers.create_unique_local_temp_path()
        sftp_client.file_download_recursive(
            remote_path=EasySftpTest.sftp_base_path,
            local_path=local_path,
            callback=self.callback_file_download
        )

        # Go through all the files that were captures by the callback function and make sure we found everything we uploaded
        count_found = 0

        for downloaded_file in self.callback_results:
            count_found += 1

        Log.test('Asserting maximum file limit reached...')
        self.assertEqual(count_found, test_file_download_limit)

    def callback_file_download(self, local_filename, remote_filename) -> None:
        """
        Store the names of all files downloaded

        :type local_filename: str
        :param local_filename: Name of file on local filesystem

        :type remote_filename: str
        :param remote_filename: Remote filename

        :return: None
        """
        if self.callback_results is None:
            self.callback_results = []

        self.callback_results.append({
            'local_filename': local_filename,
            'remote_filename': remote_filename
        })


if __name__ == '__main__':
    unittest.main()
