import os
import unittest
import uuid
from time import sleep

from EasyLambda.EasyHelpers import EasyHelpers
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasySftp import EasySftp


class EasySftpTest(unittest.TestCase):
    # SFTP server credentials used for testing
    username = 'LOYC01AU'
    address = 'test-sftp.nabmarkets.com'
    port = 9022
    rsa_private_key = '-----BEGIN RSA PRIVATE KEY-----\n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\n-----END RSA PRIVATE KEY-----'
    fingerprint = 'AAAAB3NzaC1yc2EAAAABJQAAAQEA9s44jQ93ipX9ZN2hMPwC8GY8Yr+kirI3mW22RfG7b0mglk3JHnlTIecxT/wIWqAyBdWM3xMeicRAjheL6+/JUKEmPdwbg2EbbdBGCWtVkmyNHUJboq0ULta2Lb4cXxqYgo/t1Hjz90gpB4xtTxUicgfIBEHeOfiB9Ptt/jZnSzstbcn0wTP1WE2xtwBmta9yjX4Ul/dA2JVE27H9xVyNnBuWZIPxJGGU1ROCPqf7q888WO+lKo0sGG0qO3gsWAbr+MeDQtkYL3P3ylvMDxzgqK/Ud6hEUn4U+W6lYZEQVXn29t26d14JjvOPCyhxQ8jKciLag75uTOMoVlNqVvRmRw=='
    fingerprint_type = 'ssh-rsa'
    test_gpg_file = '-----BEGIN PGP MESSAGE-----\n\n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\n=6KVe\n-----END PGP MESSAGE-----'

    def test_sftp_connect_disconnect_success(self):
        """
        Test successful connection to SFTP server and subsequent disconnection
        """
        EasyLog.test('Testing SFTP server connection/disconnection...')

        EasyLog.test('Connecting to SFTP server...')
        sftp_client = EasySftp()
        connection_result = sftp_client.connect_rsa_private_key(
            username=EasySftpTest.username,
            address=EasySftpTest.address,
            port=EasySftpTest.port,
            rsa_private_key=EasySftpTest.rsa_private_key,
            fingerprint=EasySftpTest.fingerprint,
            fingerprint_type=EasySftpTest.fingerprint_type
        )

        EasyLog.test('Asserting connection opened successfully...')
        self.assertTrue(connection_result)
        self.assertTrue(sftp_client.is_connected())

        EasyLog.test('Disconnecting from SFTP server...')
        sftp_client.disconnect()

        EasyLog.test('Asserting connection closed successfully...')
        self.assertFalse(sftp_client.is_connected())

    def test_sftp_file_list_success(self):
        """
        Test listing of files is successful
        """
        EasyLog.test('Testing list files...')

        EasyLog.test('Connecting to SFTP server...')
        sftp_client = EasySftp()
        sftp_client.connect_rsa_private_key(
            username=EasySftpTest.username,
            address=EasySftpTest.address,
            port=EasySftpTest.port,
            rsa_private_key=EasySftpTest.rsa_private_key,
            fingerprint=EasySftpTest.fingerprint,
            fingerprint_type=EasySftpTest.fingerprint_type
        )

        EasyLog.test('Listing files...')
        files = sftp_client.file_list(
            remote_path='/',
            recursive=True
        )

        EasyLog.test('Asserting return type...')
        self.assertTrue(isinstance(files, list))

    def test_sftp_file_download_recursive_success(self):
        """
        Test listing of files is successful
        """
        EasyLog.test('Testing recursive download...')

        # Connect to SFTP test server
        EasyLog.test('Connecting to SFTP server...')
        sftp_client = EasySftp()
        sftp_client.connect_rsa_private_key(
            username=EasySftpTest.username,
            address=EasySftpTest.address,
            port=EasySftpTest.port,
            rsa_private_key=EasySftpTest.rsa_private_key,
            fingerprint=EasySftpTest.fingerprint,
            fingerprint_type=EasySftpTest.fingerprint_type
        )

        # Create local download folder
        EasyLog.test('Creating local temporary folder...')
        local_path = EasyHelpers.create_unique_local_temp_path()

        # Upload a number of test files
        test_filenames = []
        for count in range(1, 5):
            test_filename = 'test-{uuid}.gpg'.format(uuid=uuid.uuid4())
            test_filenames.append(test_filename)

            remote_filename = '/DTDD/{test_filename}'.format(test_filename=test_filename)
            EasyLog.test('Uploading test file from string: {remote_filename}'.format(remote_filename=remote_filename))
            sftp_client.file_upload_from_string(
                contents=EasySftpTest.test_gpg_file,
                remote_filename=remote_filename
            )

        # Sleep 15 seconds to allowed SFTP server to do its thing
        EasyLog.test('Waiting for remote SFTP server to process files...')
        sleep(15)

        # Download the files we uploaded
        EasyLog.test('Downloading all available files from SFTP server...')
        sftp_client.file_download_recursive(
            local_path=local_path,
            remote_path='/'
        )

        # Make sure all uploaded files were found in the local filesystem
        for test_filename in test_filenames:
            local_filename = EasySftp.sanitize_path('{local_path}/DTDDA/{test_filename}.REJECTED.ACK.pgp'.format(local_path=local_path, test_filename=test_filename))
            EasyLog.test('Checking downloaded file exists: {local_filename}'.format(local_filename=local_filename))
            self.assertTrue(os.path.exists(local_filename))

        EasyLog.test('All files successfully uploaded')


if __name__ == '__main__':
    unittest.main()
