import json

from google.oauth2.service_account import Credentials


def build_gc_credentials(asm_secrets: dict) -> Credentials:
    """

    Args:
        asm_secrets (dict): Response of `boto3.SecretsManager.Client.get_secret_value()`
            The contents are as follows.
            {
                'ARN': 'string',
                'Name': 'string',
                'VersionId': 'string',
                'SecretBinary': b'bytes',
                'SecretString': 'string',
                'VersionStages': [
                    'string',
                ],
                'CreatedDate': datetime(2015, 1, 1)
            }
    """
    credential_dic = json.loads(asm_secrets["SecretString"])
    return Credentials.from_service_account_info(credential_dic)
    return Credentials.from_service_account_info(credential_dic)
