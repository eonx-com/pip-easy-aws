import fcntl
import os
import unittest

from EasyLocalDisk.Client import Client
from EasyLog.Log import Log
from Tests.Helpers import Helpers


# noinspection DuplicatedCode
class EasyLocalDiskTest(unittest.TestCase):
    def test_create_path(self) -> None:
        """
        Test creation of a path on the local filesystem

        :return: None
        """
        Log.test('Testing path creation...')
        test_path = Helpers.create_unique_local_temp_path(make_path=False)

        Log.test('Test path: {test_path}'.format(test_path=test_path))
        Log.test('Asserting path does not exist...')

        path_exists = os.path.exists(test_path)
        self.assertFalse(path_exists)

        Log.test('Creating path...')
        Client.create_path(test_path)

        Log.test('Asserting path exists...')
        self.assertTrue(os.path.exists(test_path))

        Log.test('Asserting subsequent call successful...')
        Client.create_path(test_path)
        self.assertTrue(os.path.exists(test_path))

    def test_file_exists(self) -> None:
        """
        Test file existence check performs as expected

        :return: None
        """
        test_filename = Helpers.create_unique_local_filename()
        test_content = Helpers.create_random_content()

        Log.test('Asserting file does not exist...')
        self.assertFalse(Client.file_exists(test_filename))

        Log.test('Creating file...')
        Client.file_create_from_string(filename=test_filename, content=test_content)

        Log.test('Asserting file does exist...')
        self.assertTrue(Client.file_exists(test_filename))

    def test_file_readable(self) -> None:
        """
        Test file readability

        :return: None
        """
        test_filename = Helpers.create_unique_local_filename()
        test_content = Helpers.create_random_content()

        Log.test('Asserting file does not exist...')
        self.assertFalse(Client.file_exists(test_filename))

        Log.test('Creating file...')
        Client.file_create_from_string(filename=test_filename, content=test_content)

        Log.test('Asserting file does exist...')
        self.assertTrue(Client.file_exists(test_filename))

        Log.test('Asserting file is readable...')
        self.assertTrue(Client.is_file_readable(test_filename))

        Log.test('Deleting test file...')
        Client.file_delete(test_filename)

    def test_file_delete(self) -> None:
        """
        Test file delete

        :return: None
        """
        test_filename = Helpers.create_unique_local_filename()
        test_content = Helpers.create_random_content()

        Log.test('Asserting file does not exist...')
        self.assertFalse(Client.file_exists(test_filename))

        Log.test('Creating file...')
        Client.file_create_from_string(filename=test_filename, content=test_content)

        Log.test('Asserting file does exist...')
        self.assertTrue(Client.file_exists(test_filename))

        Log.test('Deleting test file...')
        Client.file_delete(test_filename)

        Log.test('Asserting file does not exist...')
        self.assertFalse(Client.file_exists(test_filename))

    def test_file_create_from_string(self) -> None:
        """
        Test creation of a file on the local filesystem

        :return: None
        """
        test_contents = Helpers.create_random_content()
        test_filename = Helpers.create_unique_local_filename()

        Log.test('Creating test file...')
        Client.file_create_from_string(filename=test_filename, content=test_contents)

        Log.test('Loading test file contents...')
        file = open(test_filename, "rt")
        read_contents = file.read()
        file.close()

        Log.test('Asserting contents same...')
        self.assertEqual(test_contents, read_contents)


if __name__ == '__main__':
    unittest.main()
