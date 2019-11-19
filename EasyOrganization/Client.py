import boto3

from EasyLog.Log import Log


class Client:
    # Cache for Organizations client
    __client__ = None

    @staticmethod
    def get_organizations_client():
        """
        Setup Organizations client
        """
        # If we haven't gotten a client yet- create one now and cache it for future calls
        if Client.__client__ is None:
            Log.trace('Instantiating AWS Organizations Client...')
            Client.__client__ = boto3.session.Session().client('organizations')

        # Return the cached client
        return Client.__client__

    @staticmethod
    def list_accounts() -> dict:
        """
        List all accounts

        :return: Dictionary of accounts found
        """
        client_organizations = boto3.client('organizations')

        accounts = {}

        while True:
            response = client_organizations.list_accounts()

            accounts[response['Id']] = {
                'id': response['Id'],
                'arn': response['Arn'],
                'email': response['Email'],
                'name': response['Name'],
                'status': response['Status']
            }

            if 'NextToken' not in response:
                break

            # Get next page of accounts
            response = client_organizations.list_accounts(NextToken=response['NextToken'])

        return accounts
