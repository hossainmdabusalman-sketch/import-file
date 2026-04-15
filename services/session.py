"""
Thread-local HTTP session factory.

Each worker thread gets exactly one ``requests.Session`` (with connection
pooling and auth already configured).  Reusing sessions dramatically reduces
TCP/TLS overhead across thousands of requests.
"""
from __future__ import annotations

import threading

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

from config import HTTP_POOL_CONNECTIONS, HTTP_POOL_MAXSIZE

_tls = threading.local()


def get_session(username: str, password: str) -> requests.Session:
    """
    Return the thread-local session, creating it on first call per thread.

    The session is keyed only by thread identity; credentials are assumed
    constant for a single upload job.
    """
    if not hasattr(_tls, "session"):
        adapter = HTTPAdapter(
            pool_connections=HTTP_POOL_CONNECTIONS,
            pool_maxsize=HTTP_POOL_MAXSIZE,
        )
        session = requests.Session()
        session.auth = HTTPBasicAuth(username, password)
        session.headers.update({"Accept": "application/json"})
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _tls.session = session

    return _tls.session


def clear_session() -> None:
    """Drop the thread-local session (useful in tests or long-lived threads)."""
    if hasattr(_tls, "session"):
        _tls.session.close()
        del _tls.session