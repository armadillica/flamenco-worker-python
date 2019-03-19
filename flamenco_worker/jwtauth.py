"""JWT token creation for Worker registration."""

import datetime

import jwt

from . import tz

REGISTRATION_TOKEN_EXPIRY = datetime.timedelta(minutes=15)


def new_registration_token(pre_shared_secret: str) -> str:
    """Return a new JWT signed with the pre-shared secret."""

    now = datetime.datetime.now(tz.tzutc())
    expiry = now + REGISTRATION_TOKEN_EXPIRY

    claims = {
        'exp': expiry.timestamp(),
        'iat': now.timestamp(),
    }
    token = jwt.encode(claims, pre_shared_secret.encode(), algorithm='HS256')
    return token.decode('ascii')
