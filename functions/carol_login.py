from pycarol import Carol, ApiKeyAuth, PwdAuth, Tasks
import os


def get_login(domain, org, carol_app):
    email = os.environ['CAROLUSER']
    password = os.environ['CAROLPWD']
    login = Carol(domain, carol_app, auth=PwdAuth(email, password), organization=org, )
    api_key = login.issue_api_key()

    login = Carol(domain, carol_app, auth=ApiKeyAuth(api_key['X-Auth-Key']),
                  connector_id=api_key['X-Auth-ConnectorId'], organization=org, )
    return login