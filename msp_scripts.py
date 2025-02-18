import os

from api_utils import MSGraphApi


msg = MSGraphApi(
    tenant_id=os.getenv('AZURE_TENANT_ID'),
    client_id=os.getenv('AZURE_CLIENT_ID'),
    client_secret=os.getenv('AZURE_CLIENT_SECRET')
)
msg.request_access_token()
print(msg.get_group_members('4bbe3379-1250-4522-92e6-017f77517470'))