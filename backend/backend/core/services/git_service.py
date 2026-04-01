"""Git service layer — strategy pattern for git provider integration.

Provides an abstract base class and concrete GitHub implementation
for all git file operations (CRUD, commits, repo info).
"""

import logging
import re
import time
from abc import ABC, abstractmethod
from base64 import b64decode, b64encode
from typing import Any, Optional

import httpx

from backend.errors.exceptions import (
    GitConnectionFailedException,
    GitPushFailedException,
    GitRateLimitException,
    GitTokenExpiredException,
    UnsupportedGitProviderException,
)

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
HTTPX_TIMEOUT = 30.0
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0


def _sanitize_token(token: str) -> str:
    if not token or len(token) < 12:
        return "****"
    return f"{token[:4]}...{token[-4:]}"


def _sanitize_url(url: str) -> str:
    return re.sub(r"://[^@]+@", "://***@", url)


class GitServiceBase(ABC):
    """Abstract interface for git provider operations."""

    @abstractmethod
    def test_connection(self) -> dict[str, Any]: ...

    @abstractmethod
    def get_file(
        self, path: str, ref: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str]]: ...

    @abstractmethod
    def put_file(
        self, path: str, content: str, message: str, sha: Optional[str] = None,
    ) -> dict[str, Any]: ...

    @abstractmethod
    def delete_file(
        self, path: str, sha: str, message: str
    ) -> dict[str, Any]: ...

    @abstractmethod
    def list_commits(
        self, path: Optional[str] = None, page: int = 1, per_page: int = 30
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def get_repo_info(self) -> dict[str, Any]: ...


class GitHubService(GitServiceBase):
    """GitHub REST API v3 implementation using httpx."""

    def __init__(self, repo_url: str, token: str, branch: str = "main") -> None:
        self._repo_url = repo_url
        self._token = token
        self._branch = branch
        self._owner, self._repo = self._parse_url(repo_url)
        self._client = httpx.Client(
            base_url=GITHUB_API_BASE,
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Authorization": f"Bearer {token}",
            },
            timeout=HTTPX_TIMEOUT,
        )
        self._sha_cache: dict[str, str] = {}
        logger.info(
            "GitHubService initialized for %s/%s (branch=%s, token=%s)",
            self._owner, self._repo, self._branch, _sanitize_token(self._token),
        )

    def __del__(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    @staticmethod
    def _parse_url(repo_url: str) -> tuple[str, str]:
        url = repo_url.strip().rstrip("/")
        ssh_match = re.match(r"git@github\.com:(.+)/(.+?)(?:\.git)?$", url)
        if ssh_match:
            return ssh_match.group(1), ssh_match.group(2)
        if url.endswith(".git"):
            url = url[:-4]
        parts = url.split("/")
        if len(parts) < 2:
            raise GitConnectionFailedException(
                error_message=f"Invalid GitHub URL: {_sanitize_url(repo_url)}"
            )
        return parts[-2], parts[-1]

    @staticmethod
    def _log_rate_limit(response: httpx.Response) -> None:
        remaining = response.headers.get("x-ratelimit-remaining")
        limit = response.headers.get("x-ratelimit-limit")
        if remaining is not None:
            level = logging.WARNING if int(remaining) < 100 else logging.DEBUG
            logger.log(level, "GitHub API rate limit: %s/%s remaining", remaining, limit)

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        last_exc: Optional[Exception] = None
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._client.request(method, url, **kwargs)
                self._log_rate_limit(response)
                if response.status_code < 500 and response.status_code != 429:
                    return response
                if response.status_code == 429:
                    retry_after = response.headers.get("retry-after")
                    wait = int(retry_after) if retry_after else backoff
                    logger.warning("GitHub 429 (attempt %d/%d), waiting %ds", attempt, MAX_RETRIES, wait)
                    if attempt < MAX_RETRIES:
                        time.sleep(wait)
                        backoff *= 2
                        continue
                    raise GitRateLimitException()
                logger.warning("GitHub %d (attempt %d/%d), retrying in %.1fs",
                               response.status_code, attempt, MAX_RETRIES, backoff)
                if attempt < MAX_RETRIES:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return response
            except httpx.RequestError as exc:
                last_exc = exc
                logger.warning("GitHub request error (attempt %d/%d): %s", attempt, MAX_RETRIES, str(exc))
                if attempt < MAX_RETRIES:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
        raise GitConnectionFailedException(
            error_message=f"Network error after {MAX_RETRIES} attempts: {last_exc}"
        )

    def _handle_auth_error(self, response: httpx.Response) -> None:
        if response.status_code == 401:
            raise GitTokenExpiredException()
        if response.status_code == 403:
            body = response.json() if response.content else {}
            if "rate limit" in body.get("message", "").lower():
                raise GitRateLimitException()
            raise GitConnectionFailedException(
                error_message="Access denied. Ensure the token has repo access."
            )

    def test_connection(self) -> dict[str, Any]:
        response = self._request_with_retry("GET", f"/repos/{self._owner}/{self._repo}")
        self._handle_auth_error(response)
        if response.status_code == 404:
            raise GitConnectionFailedException(
                error_message=f"Repository not found: {self._owner}/{self._repo}. Check the URL and permissions."
            )
        if response.status_code != 200:
            raise GitConnectionFailedException(
                error_message=f"GitHub API error (HTTP {response.status_code}): {response.text[:200]}"
            )
        data = response.json()
        permissions = data.get("permissions", {})
        if not permissions.get("push", False):
            raise GitConnectionFailedException(
                error_message="Write access required. The token has read-only access to this repository."
            )
        return {
            "success": True,
            "repo_info": {
                "full_name": data.get("full_name", ""),
                "default_branch": data.get("default_branch", "main"),
                "private": data.get("private", False),
                "permissions": permissions,
            },
        }

    def get_file(self, path: str, ref: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
        branch = ref or self._branch
        response = self._request_with_retry(
            "GET", f"/repos/{self._owner}/{self._repo}/contents/{path}", params={"ref": branch},
        )
        self._handle_auth_error(response)
        if response.status_code == 404:
            return None, None
        if response.status_code != 200:
            raise GitConnectionFailedException(
                error_message=f"Failed to read file '{path}' (HTTP {response.status_code}): {response.text[:200]}"
            )
        data = response.json()
        sha = data.get("sha", "")
        content = b64decode(data.get("content", "")).decode("utf-8")
        self._sha_cache[path] = sha
        return content, sha

    def put_file(self, path: str, content: str, message: str, sha: Optional[str] = None) -> dict[str, Any]:
        if sha is None:
            sha = self._sha_cache.get(path)
        payload: dict[str, Any] = {
            "message": message,
            "content": b64encode(content.encode("utf-8")).decode("ascii"),
            "branch": self._branch,
        }
        if sha:
            payload["sha"] = sha
        response = self._request_with_retry(
            "PUT", f"/repos/{self._owner}/{self._repo}/contents/{path}", json=payload,
        )
        self._handle_auth_error(response)
        if response.status_code == 409:
            raise GitPushFailedException(
                model_name=path,
                error_message="File was modified externally since last read. Please refresh and try again.",
            )
        if response.status_code == 422:
            body = response.json() if response.content else {}
            raise GitPushFailedException(model_name=path, error_message=f"GitHub rejected the file: {body.get('message', 'Validation failed')}")
        if response.status_code not in (200, 201):
            raise GitPushFailedException(
                model_name=path,
                error_message=f"Failed to write file '{path}' (HTTP {response.status_code}): {response.text[:200]}",
            )
        data = response.json()
        commit_sha = data.get("commit", {}).get("sha", "")
        html_url = data.get("content", {}).get("html_url", "")
        new_sha = data.get("content", {}).get("sha", "")
        if new_sha:
            self._sha_cache[path] = new_sha
        logger.info("Git file written: %s (commit=%s)", path, commit_sha[:8] if commit_sha else "unknown")
        return {"commit_sha": commit_sha, "html_url": html_url}

    def delete_file(self, path: str, sha: str, message: str) -> dict[str, Any]:
        payload = {"message": message, "sha": sha, "branch": self._branch}
        response = self._request_with_retry(
            "DELETE", f"/repos/{self._owner}/{self._repo}/contents/{path}", json=payload,
        )
        self._handle_auth_error(response)
        if response.status_code != 200:
            raise GitPushFailedException(
                model_name=path,
                error_message=f"Failed to delete file '{path}' (HTTP {response.status_code}): {response.text[:200]}",
            )
        data = response.json()
        commit_sha = data.get("commit", {}).get("sha", "")
        self._sha_cache.pop(path, None)
        logger.info("Git file deleted: %s (commit=%s)", path, commit_sha[:8] if commit_sha else "unknown")
        return {"commit_sha": commit_sha}

    def list_commits(self, path: Optional[str] = None, page: int = 1, per_page: int = 30) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"sha": self._branch, "page": page, "per_page": min(per_page, 100)}
        if path:
            params["path"] = path
        response = self._request_with_retry("GET", f"/repos/{self._owner}/{self._repo}/commits", params=params)
        self._handle_auth_error(response)
        if response.status_code != 200:
            logger.warning("Failed to list commits (HTTP %d): %s", response.status_code, response.text[:200])
            return []
        return [
            {
                "sha": c.get("sha", ""),
                "message": c.get("commit", {}).get("message", ""),
                "author": c.get("commit", {}).get("author", {}).get("name", ""),
                "date": c.get("commit", {}).get("author", {}).get("date", ""),
                "html_url": c.get("html_url", ""),
            }
            for c in response.json()
        ]

    def get_repo_info(self) -> dict[str, Any]:
        response = self._request_with_retry("GET", f"/repos/{self._owner}/{self._repo}")
        self._handle_auth_error(response)
        if response.status_code != 200:
            raise GitConnectionFailedException(
                error_message=f"Failed to fetch repo info (HTTP {response.status_code}): {response.text[:200]}"
            )
        data = response.json()
        return {
            "full_name": data.get("full_name", ""),
            "default_branch": data.get("default_branch", "main"),
            "private": data.get("private", False),
            "permissions": data.get("permissions", {}),
            "description": data.get("description", ""),
            "html_url": data.get("html_url", ""),
        }


def get_git_service(config) -> GitServiceBase:
    """Create a git service instance from a GitRepoConfig model."""
    repo_url = config.repo_url or ""
    credentials = config.decrypted_credentials or {}
    token = credentials.get("token", "")
    branch = config.branch_name or "main"
    if "github.com" in repo_url:
        return GitHubService(repo_url, token, branch)
    raise UnsupportedGitProviderException(repo_url=_sanitize_url(repo_url))
