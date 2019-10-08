import os
import unittest

from EasyLambda.EasyHelpers import EasyHelpers
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasySftp import EasySftp


# noinspection PyBroadException,DuplicatedCode
class EasySftpTest(unittest.TestCase):
    # SFTP server credentials used for testing
    sftp_username = 'sftp'
    sftp_password = 'sftp'
    sftp_port = 22
    sftp_base_path = '/home/sftp/'
    sftp_address = 'localhost'

    def test_connect_disconnect(self) -> None:
        """
        Test successful connection to SFTP server and subsequent disconnection

        :return: None
        """
        EasyLog.test('Testing SFTP server connection/disconnection...')

        # Get SFTP client
        EasyLog.test('Connecting to SFTP server...')
        sftp_client = EasySftp()

        # Disable host key checking
        sftp_client.disable_host_key_checking()

        # Connect with username/password
        connection_result = sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        # Connect, and assert it reports a successful connection
        EasyLog.test('Asserting connection opened successfully...')
        self.assertTrue(connection_result)
        self.assertTrue(sftp_client.is_connected())

        # Disconnect, and assert it reports no connection
        EasyLog.test('Disconnecting from SFTP server...')
        sftp_client.disconnect()
        self.assertFalse(sftp_client.is_connected())

    def test_host_fingerprint(self) -> None:
        """
        Attempt to connect to the SFTP server with host key checking enable- but no valid host key supplied. Connection should fail.

        :return: None
        """
        EasyLog.test('Testing SFTP server host key validation...')

        # Get SFTP client (there is no need to explicitly enable host key checking as it should be on by default)
        sftp_client = EasySftp()

        # Connect with username/password and assert we receive an exception error
        exception = None

        try:
            EasyLog.test('Connecting to SFTP server with unknown host key/fingerprint...')
            sftp_client.connect_password(
                username=EasySftpTest.sftp_username,
                password=EasySftpTest.sftp_password,
                address=EasySftpTest.sftp_address,
                port=EasySftpTest.sftp_port
            )
        except Exception as connect_exception:
            EasyLog.test('Received exception error: {connect_exception}'.format(connect_exception=connect_exception))
            exception = connect_exception

        # Assert we received the expect exception error
        EasyLog.test('Asserting expected fingerprint exception received...')
        self.assertTrue(str(exception) == EasySftp.ERROR_INVALID_FINGERPRINT)

        # Disable host key checking and ensure it works
        EasyLog.test('Disabling host key checks...')
        sftp_client.disable_host_key_checking()

        EasyLog.test('Reattempting connection...')
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        EasyLog.test('Checking connection...')
        self.assertTrue(sftp_client.is_connected())

    def test_file_list(self) -> None:
        """
        Test listing of files is successful

        :return: None
        """
        test_file_count = 5

        EasyLog.test('Testing list files...')

        EasyLog.test('Connecting to SFTP server...')
        sftp_client = EasySftp()
        sftp_client.disable_host_key_checking()
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        EasyLog.test('Listing files...')
        files = sftp_client.file_list(
            remote_path=EasySftpTest.sftp_base_path,
            recursive=True
        )

        EasyLog.test('Asserting return type...')
        self.assertTrue(isinstance(files, list))

        EasyLog.test('Creating local test file...')
        local_test_filename = EasyHelpers.create_local_test_file()

        EasyLog.test('Uploading test files to SFTP server...')
        remote_filenames = {}
        for i in range(0, test_file_count):
            remote_filenames[i] = EasySftp.sanitize_path('{base_path}/test_sftp_file_list_success/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))
            EasyLog.test('Uploading: {remote_filename}...'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_upload(
                local_filename=local_test_filename,
                remote_filename=remote_filenames[i]
            )

        # Assert we can find the files on SFTP server by listing each specific directory
        EasyLog.test('Testing listing of specific path...')
        for i in range(0, test_file_count):
            remote_path = os.path.dirname(remote_filenames[i])
            EasyLog.test('Listing files in remote path: {remote_path}...'.format(remote_path=remote_path))

            files = sftp_client.file_list(remote_path=remote_path, recursive=False)
            found_filename = False
            for filename in files:
                if filename == remote_filenames[i]:
                    EasyLog.test('Found: {filename}'.format(filename=filename))
                    found_filename = True
                    break

            self.assertTrue(found_filename)

        # Assert we can find all the files by performing a recursive search of the base directory
        EasyLog.test('Testing recursive directory listing...')
        EasyLog.test('Listing files in remote path: {remote_path}...'.format(remote_path=EasySftpTest.sftp_base_path))
        files = sftp_client.file_list(remote_path=EasySftpTest.sftp_base_path, recursive=True)

        EasyLog.test('Searching for test files...')
        found_files = 0
        for i in range(0, 5):
            for filename in files:
                if filename == remote_filenames[i]:
                    EasyLog.test('Found: {remote_filename}'.format(remote_filename=remote_filenames[i]))
                    found_files += 1
                    if found_files == test_file_count:
                        EasyLog.test('Found all test files successfully')
                    break

        EasyLog.test('Asserting all test files found...')
        self.assertEqual(found_files, test_file_count)

        # Assert we can find all the files by performing a recursive search of the base directory
        EasyLog.test('Testing non-recursive directory listing...')
        EasyLog.test('Listing files in remote path: {remote_path}...'.format(remote_path=EasySftpTest.sftp_base_path))
        files = sftp_client.file_list(remote_path=EasySftpTest.sftp_base_path, recursive=False)

        EasyLog.test('Searching for test files...')
        found_files = 0
        for i in range(0, 5):
            for filename in files:
                if filename == remote_filenames[i]:
                    EasyLog.test('Found: {remote_filename}'.format(remote_filename=remote_filenames[i]))
                    found_files += 1
                    break

        EasyLog.test('Asserting no test files found...')
        self.assertEqual(found_files, 0)

    def test_file_download_overwrite(self) -> None:
        test_file_count = 5

        EasyLog.test('Testing SFTP download...')

        EasyLog.test('Connecting to SFTP server...')
        sftp_client = EasySftp()
        sftp_client.disable_host_key_checking()
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        EasyLog.test('Creating local test file...')
        local_test_filename = EasyHelpers.create_local_test_file()

        remote_filenames = {}
        for i in range(0, test_file_count):
            remote_filenames[i] = EasySftp.sanitize_path('{base_path}/test_sftp_download_overwrite/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))
            EasyLog.test('Uploading test file: {remote_filename}...'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_upload(
                local_filename=local_test_filename,
                remote_filename=remote_filenames[i]
            )

        for i in range(0, test_file_count):
            local_download_filename = EasyHelpers.create_unique_local_filename()
            remote_filename = EasySftp.sanitize_path('{base_path}/test_sftp_download_overwrite/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))

            EasyLog.test('Downloading test file: {remote_filename}...'.format(remote_filename=remote_filename))
            sftp_client.file_download(
                remote_filename=remote_filename,
                local_filename=local_download_filename
            )

            # Try to download existing file without overwrite enabled
            EasyLog.test('Downloading same test file again with allow overwrite disabled...')
            try:
                sftp_client.file_download(
                    remote_filename=remote_filename,
                    local_filename=local_download_filename,
                    allow_overwrite=False
                )
            except Exception as download_exception:
                exception = str(download_exception)

                EasyLog.test('Asserting expected exception received...')
                self.assertEqual(EasySftp.ERROR_DOWNLOAD_FILE_EXISTS, exception)

            # Perform the same download, with allow overwrite enabled
            EasyLog.test('Downloading same test file again with allow overwrite enabled...')
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

        EasyLog.test('Testing SFTP download...')

        EasyLog.test('Connecting to SFTP server...')
        sftp_client = EasySftp()
        sftp_client.disable_host_key_checking()
        sftp_client.connect_password(
            username=EasySftpTest.sftp_username,
            password=EasySftpTest.sftp_password,
            address=EasySftpTest.sftp_address,
            port=EasySftpTest.sftp_port
        )

        EasyLog.test('Creating local test file...')
        local_test_filename = EasyHelpers.create_local_test_file()

        EasyLog.test('Reading test file contents...')
        local_test_file = open(local_test_filename, "rt")
        original_content = local_test_file.read()
        local_test_file.close()

        EasyLog.test('Uploading test files to SFTP server...')

        remote_filenames = {}
        for i in range(0, test_file_count):
            remote_filenames[i] = EasySftp.sanitize_path('{base_path}/test_sftp_file_list_success/{i}/{local_filename}'.format(
                i=i,
                base_path=EasySftpTest.sftp_base_path,
                local_filename=local_test_filename
            ))
            EasyLog.test('Uploading test file: {remote_filename}...'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_upload(
                local_filename=local_test_filename,
                remote_filename=remote_filenames[i]
            )

        # Test downloading of test files individually
        for i in range(0, test_file_count):
            local_filename = EasyHelpers.create_unique_local_filename()
            EasyLog.test('Downloading test file: {remote_filename}'.format(remote_filename=remote_filenames[i]))
            sftp_client.file_download(
                local_filename=local_filename,
                remote_filename=remote_filenames[i]
            )

            EasyLog.test('Asserting content matches original file...')
            test_file = open(local_filename, 'rt')
            test_file_contents = test_file.read()
            test_file.close()
            self.assertEqual(original_content, test_file_contents)

        # Perform recursive download and make sure all test files get downloaded
        EasyLog.test('Testing recursive download...')
        self.found_callback_test_sftp_file_download_success = []

        # Download all files from the SFTP server to a local test path, a callback function will store the details of downloaded files
        local_path = EasyHelpers.create_unique_local_temp_path()
        sftp_client.file_download_recursive(
            remote_path=EasySftpTest.sftp_base_path,
            local_path=local_path,
            callback=self.callback_file_download
        )

        # Go through all the files that were captures by the callback function and make sure we found everything we uploaded
        count_found = 0
        for downloaded_file in self.found_callback_test_sftp_file_download_success:
            for i in range(0, test_file_count):
                if downloaded_file['remote_filename'] == remote_filenames[i]:
                    EasyLog.test('Found: {remote_filename}'.format(remote_filename=downloaded_file['remote_filename']))
                    count_found += 1

        EasyLog.test('Asserting all uploaded test files found...')
        self.assertEqual(count_found, test_file_count)

    def callback_file_download(self, local_filename, remote_filename) -> None:
        """
        Store the names of all files downloaded

        :type local_filename: str
        :param local_filename: Name of file on local filesystem

        :type remote_filename: str
        :param remote_filename: Remote filename

        :return: None
        """
        if self.found_callback_test_sftp_file_download_success is None:
            self.found_callback_test_sftp_file_download_success = []

        self.found_callback_test_sftp_file_download_success.append({
            'local_filename': local_filename,
            'remote_filename': remote_filename
        })


if __name__ == '__main__':
    unittest.main()
