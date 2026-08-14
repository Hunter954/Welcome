import os
import base64
import hashlib
from cryptography.fernet import Fernet


def _fernet():
    raw = os.getenv("FERNET_KEY", "").strip()
    if raw:
        key = raw.encode()
    else:
        secret = os.getenv("SECRET_KEY", "dev-secret-change-me").encode()
        key = base64.urlsafe_b64encode(hashlib.sha256(secret).digest())
    return Fernet(key)


def encrypt(text: str) -> str:
    return _fernet().encrypt(text.encode()).decode()


def decrypt(token: str) -> str:
    return _fernet().decrypt(token.encode()).decode()
