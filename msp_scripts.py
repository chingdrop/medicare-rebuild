import os
import pandas as pd
from pathlib import Path

from api_utils import MSGraphApi


msg = MSGraphApi(
    tenant_id=os.getenv('AZURE_TENANT_ID'),
    client_id=os.getenv('AZURE_CLIENT_ID'),
    client_secret=os.getenv('AZURE_CLIENT_SECRET')
)
msg.request_access_token()
data = msg.get_group_members('4bbe3379-1250-4522-92e6-017f77517470')
df = pd.DataFrame(data['value'])
df.to_csv(Path.cwd() / 'data' / 'test_users.csv', index=False)