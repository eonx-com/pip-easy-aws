import unittest

from EasyLog.Log import Log

from EasySecretsManager.Client import Client


class EasySecretsManagerTest(unittest.TestCase):
    test_secret_name = 'test/easy-lambda'
    test_secret_value = {
        'one': 1,
        'two': 2
    }

    def test_create_delete_secret(self) -> None:
        """
        Test creation of a new secret

        :return: None
        """
        Log.test('Testing creation of secret...')

        Log.test('Asserting secret does not already exist...')
        self.assertFalse(Client.secret_exists(EasySecretsManagerTest.test_secret_name))

        Log.test('Creating secret...')
        Client.create_secret(
            name=EasySecretsManagerTest.test_secret_name,
            value=EasySecretsManagerTest.test_secret_value
        )

        Log.test('Asserting secret exist...')
        self.assertTrue(Client.secret_exists(EasySecretsManagerTest.test_secret_name))

        Log.test('Deleting secret...')
        Client.delete_secret(
            name=EasySecretsManagerTest.test_secret_name,
            force_delete=True
        )

        Log.test('Asserting secret does not exist...')
        self.assertFalse(Client.secret_exists(EasySecretsManagerTest.test_secret_name))

    def test_get_secret(self) -> None:
        """
        Test retrieval of a secret
        :return:
        """
        Log.test('Testing secret retrieval...')
        secrets = Client.list_secrets()

        Log.test('Asserting expected secret exists...')
        self.assertTrue(EasySecretsManagerTest.test_secret_name in secrets)
