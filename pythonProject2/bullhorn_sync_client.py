"""
Bullhorn REST API Client for Location Sync

Minimal client that handles OAuth auth and candidate entity updates.
Used by batch_sync_weaviate.py to push location data from Weaviate to Bullhorn.

Auth flow: authorize -> token -> login -> BhRestToken
"""

import os
import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class InvalidCredentialsError(RuntimeError):
    """Bullhorn rejected the username/password (wrong, expired, or account locked).
    Non-retryable — retrying a bad password risks locking the account."""


class BullhornClient:
    """Minimal Bullhorn REST API client for candidate updates."""

    AUTH_ENDPOINTS = [
        "https://auth-west.bullhornstaffing.com",
        "https://auth.bullhornstaffing.com",
    ]
    REST_BASE = "https://rest.bullhornstaffing.com"

    def __init__(self):
        self.client_id = os.getenv("BULLHORN_CLIENT_ID")
        self.client_secret = os.getenv("BULLHORN_CLIENT_SECRET")
        self.username = os.getenv("BULLHORN_USERNAME")
        self.password = os.getenv("BULLHORN_PASSWORD")

        if not all([self.client_id, self.client_secret, self.username, self.password]):
            raise ValueError("Missing Bullhorn credentials in .env (BULLHORN_CLIENT_ID, BULLHORN_CLIENT_SECRET, BULLHORN_USERNAME, BULLHORN_PASSWORD)")

        self.session = requests.Session()
        self.session.headers["User-Agent"] = "VectorLoader-BullhornSync/1.0"

        # Auto-retry on connection errors, DNS failures, 502/503/504
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,  # 2s, 4s, 8s
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.auth_base = None  # resolved during login
        self.bh_rest_token = None
        self.rest_url = None
        self.refresh_token = None
        self.token_expiry = 0  # epoch seconds

    # ── Auth flow ──────────────────────────────────────────────

    def login(self):
        """Full OAuth login with auth endpoint failover: authorize -> token -> REST login."""
        auth_code = self._get_auth_code()
        access_token, refresh_token = self._get_access_token(auth_code)
        self.refresh_token = refresh_token
        self._rest_login(access_token)
        logger.info("Bullhorn login successful")

    def _ensure_session(self):
        """Re-login if session is expired or about to expire."""
        if self.bh_rest_token and time.time() < self.token_expiry - 60:
            return
        if self.refresh_token:
            try:
                self._refresh_session()
                return
            except Exception as e:
                logger.warning(f"Refresh failed, doing full login: {e}")
        self.login()

    def _get_auth_code(self) -> str:
        """Step 1: Get authorization code via programmatic login. Tries each auth endpoint."""
        last_error = None

        for endpoint in self.AUTH_ENDPOINTS:
            try:
                code = self._try_auth_code(endpoint)
                self.auth_base = endpoint
                logger.info(f"Auth succeeded via {endpoint}")
                return code
            except InvalidCredentialsError:
                # Bad password fails the same way on every endpoint — don't retry,
                # and don't bury it in the generic "all endpoints failed" message.
                raise
            except Exception as e:
                last_error = e
                logger.warning(f"Auth endpoint {endpoint} failed: {e}")

        raise RuntimeError(f"All auth endpoints failed. Last error: {last_error}")

    def _try_auth_code(self, auth_base: str) -> str:
        """Try to get auth code from a single endpoint."""
        url = f"{auth_base}/oauth/authorize"
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "action": "Login",
            "username": self.username,
            "password": self.password,
        }

        # Follow redirect chain manually to find the auth code
        max_redirects = 5
        for i in range(max_redirects):
            resp = self.session.get(url, params=params, allow_redirects=False, timeout=15)
            logger.debug(f"Auth attempt {i+1}: status={resp.status_code}, location={resp.headers.get('Location', '')}")

            if resp.status_code in (301, 302, 307, 308):
                location = resp.headers.get("Location", "")
                parsed = parse_qs(urlparse(location).query)
                if "code" in parsed:
                    return parsed["code"][0]
                # Follow the redirect
                url = location
                params = {}  # Params are in the redirect URL now
            else:
                # Bullhorn returns 200 + the login page (not a redirect) when the
                # username/password is rejected. Detect it and fail fast and clearly
                # instead of retrying — retrying a bad password risks an account lockout.
                if "Invalid credentials" in resp.text:
                    raise InvalidCredentialsError(
                        f"Bullhorn rejected the login for user '{self.username}': "
                        f"Invalid credentials. The BULLHORN_PASSWORD in .env is wrong, "
                        f"expired, or the account is locked. Reset it in Bullhorn and update .env."
                    )
                break

        raise RuntimeError(f"Failed to get auth code after {max_redirects} attempts. Last status {resp.status_code}, body: {resp.text[:500]}")

    def _get_access_token(self, auth_code: str) -> tuple:
        """Step 2: Exchange auth code for access + refresh tokens."""
        url = f"{self.auth_base}/oauth/token"
        params = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        resp = self.session.post(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data["access_token"], data["refresh_token"]

    def _rest_login(self, access_token: str):
        """Step 3: REST login to get BhRestToken and restUrl."""
        url = f"{self.REST_BASE}/rest-services/login"
        params = {
            "version": "2.0",
            "access_token": access_token,
        }
        resp = self.session.post(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        self.bh_rest_token = data["BhRestToken"]
        self.rest_url = data["restUrl"]
        # Session lasts ~10 minutes; refresh at 8 min
        self.token_expiry = time.time() + 480
        logger.info(f"Bullhorn REST session established at {self.rest_url}")

    def _refresh_session(self):
        """Use refresh token to get a new access token, then re-login."""
        url = f"{self.auth_base}/oauth/token"
        params = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        resp = self.session.post(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        self.refresh_token = data["refresh_token"]
        self._rest_login(data["access_token"])
        logger.info("Bullhorn session refreshed")

    # ── Entity updates ─────────────────────────────────────────

    def update_candidate(self, candidate_id: int, fields: dict) -> dict:
        """
        Update a Candidate entity in Bullhorn.

        POST /entity/Candidate/{id} with JSON body of fields to update.
        Returns the API response dict.
        """
        self._ensure_session()

        url = f"{self.rest_url}entity/Candidate/{candidate_id}"
        params = {"BhRestToken": self.bh_rest_token}

        resp = self.session.post(url, params=params, json=fields, timeout=15)
        resp.raise_for_status()
        return resp.json()
