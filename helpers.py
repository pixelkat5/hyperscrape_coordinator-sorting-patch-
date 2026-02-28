from auth_token import AuthToken
import state
from flask import request

from workers import Worker

def get_request_ip() -> str|None:
    return request.headers.get("x-forwarded-for", request.remote_addr)

def get_auth_token() -> str|None:
    auth_header = request.headers.get("authorization", None)
    if (auth_header == None):
        return None
    return auth_header.split(" ")[-1]

def get_worker() -> Worker|None:
    """!
    @brief Returns a worker for this request or None if not applicable
    """
    token = get_auth_token()
    if (token == None):
        return None
    try:
        token: AuthToken = AuthToken.from_token(token)
    except:
        return None
    if (token == None or not token.id in state.workers):
        return None
    worker = state.workers[token.id]
    if (get_request_ip() != None and worker.ip != get_request_ip()):
        return None
    if (worker.auth_nonce != token.nonce):
        return None
    return worker