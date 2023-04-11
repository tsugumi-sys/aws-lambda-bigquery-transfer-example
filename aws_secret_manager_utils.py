import boto3


def download_secrets_from_ASM(secret_name: str, region: str) -> dict:
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region,
    )

    asm_secrets = client.get_secret_value(SecretId=secret_name)
    if asm_secrets is None:
        raise ValueError(
            f"Download AMS secrets failed (`secret_name`={secret_name}, "
            f"`secret_region`={region})."
        )
    return asm_secrets
