from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2 import credentials
import google.auth
from google.api_core import client_info

def creds(options: dict):
    scopes = options.get('authScopes', 'https://www.googleapis.com/auth/cloud-platform').split(',')
    auth_type = options.get('authType', 'APPLICATION_DEFAULT')
    if auth_type == 'APPLICATION_DEFAULT':
        creds, _ = google.auth.default(scopes)
    elif auth_type == 'SERVICE_ACCOUNT_JSON_KEYFILE':
        creds = service_account.Credentials.from_service_account_file(options['jsonKeyfile'], scopes=scopes)
    elif auth_type == 'USER_CREDENTIALS':
            creds = credentials.Credentials(
            token=options['accessToken'],
            refresh_token=options['refreshToken'],
            client_id=options['clientId'],
            client_secret=options['clientSecret'],
            scopes=scopes,
        )
    else:
        raise ValueError(f"Invalid authType: {auth_type}")
    
    impersonated_service_account = options.get('impersonatedServiceAccount', None)
    if impersonated_service_account:
        creds = google.auth.impersonated_credentials.Credentials(
            source_credentials=creds,
            target_principal=impersonated_service_account,
            target_scopes=scopes,
        )        
    return creds



service_account.Credentials.from_service_account_info

class Session:
    def __init__(self, params: dict):

        self.client = bigquery.Client(
                        project = params.get('projectId', None),
                        credentials = creds(params),
                        location = params.get('location', None),
                        client_info = client_info.ClientInfo(user_agent="starlake"),
                        client_options =params)

    def sql(self, stmt: str):
        query_job = self.client.query(stmt)  # API request
        iterator = query_job.result()  # Waits for query to finish
        if (stmt.lower().startswith("select")) or (stmt.lower().startswith("with")):
            page = next(iterator.pages)
            rows = list(page)
        else:
            rows = []
        return rows

    def close(self):
        self.client.close()

    def commit(self):
        # available only on multistatements transactions
        return []

    def rollback(self):
        # available only on multistatements transactions
        return []

client = bigquery.Client()

session = Session({})
rows = session.sql("select * from audit.audit where domain = 'bqtest'")

for row in rows:
    print(row)


session.sql("insert into hsh1.order_line values(2, 2, 2, 2.0)")

