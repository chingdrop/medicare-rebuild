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
data = msg.get_group_members('00000000-0000-0000-0000-000000000000')
df = pd.DataFrame(data['value'])
df.to_csv(Path.cwd() / 'data' / 'test_users.csv', index=False)