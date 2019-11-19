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
        accounts = {}
        response = Client.get_organizations_client().list_accounts()

        while True:
            if 'Accounts' not in response:
                raise Exception('Response from AWS did not contain expected accounts key')

            for account in response['Accounts']:
                accounts[account['Id']] = {
                    'id': account['Id'],
                    'arn': account['Arn'],
                    'email': account['Email'],
                    'name': account['Name'],
                    'status': account['Status']
                }

            if 'NextToken' not in response or str(response['NextToken']).strip() == '':
                break

            # Get next page of accounts
            response = Client.get_organizations_client().list_accounts(NextToken=response['NextToken'])

        return accounts
