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
    GitBranchAlreadyExistsException,
    GitBranchException,
    GitConnectionFailedException,
    GitPRAlreadyExistsException,
    GitPRException,
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

    @abstractmethod
    def list_branches(self, per_page: int = 100) -> list[dict[str, Any]]: ...

    @abstractmethod
    def get_branch(self, branch_name: str) -> Optional[dict[str, Any]]: ...

    @abstractmethod
    def create_branch(self, branch_name: str, from_branch: Optional[str] = None) -> dict[str, Any]: ...

    @abstractmethod
    def list_directory(
        self, path: str = "", ref: Optional[str] = None
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def create_pull_request(
        self, title: str, body: str, head_branch: str, base_branch: Optional[str] = None,
    ) -> dict[str, Any]: ...

    @abstractmethod
    def get_pull_request(self, pr_number: int) -> dict[str, Any]: ...

    @abstractmethod
    def get_commit_detail(self, sha: str) -> Optional[dict[str, Any]]: ...

    @abstractmethod
    def push_combined_file(
        self, file_path: str, content: str, commit_message: str,
        author_name: Optional[str] = None, author_email: Optional[str] = None,
    ) -> dict[str, Any]: ...


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
        logger.debug(
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

    def get_commit_detail(self, sha: str) -> Optional[dict[str, Any]]:
        response = self._request_with_retry("GET", f"/repos/{self._owner}/{self._repo}/commits/{sha}")
        self._handle_auth_error(response)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            return None
        data = response.json()
        commit = data.get("commit", {})
        author = commit.get("author", {})
        return {
            "sha": data.get("sha", ""),
            "message": commit.get("message", ""),
            "author_name": author.get("name", ""),
            "author_email": author.get("email", ""),
            "date": author.get("date", ""),
            "html_url": data.get("html_url", ""),
            "files_changed": [
                {"filename": f.get("filename", ""), "status": f.get("status", ""), "additions": f.get("additions", 0), "deletions": f.get("deletions", 0)}
                for f in data.get("files", [])
            ],
        }

    def list_directory(self, path: str = "", ref: Optional[str] = None) -> list[dict[str, Any]]:
        branch = ref or self._branch
        params: dict[str, Any] = {"ref": branch}
        url_path = f"/repos/{self._owner}/{self._repo}/contents/{path}" if path else f"/repos/{self._owner}/{self._repo}/contents"
        response = self._request_with_retry("GET", url_path, params=params)
        self._handle_auth_error(response)
        if response.status_code != 200:
            logger.warning("Failed to list directory (HTTP %d): %s", response.status_code, response.text[:200])
            return []
        data = response.json()
        if not isinstance(data, list):
            return []
        return [
            {"name": entry.get("name", ""), "type": entry.get("type", ""), "path": entry.get("path", "")}
            for entry in data
            if entry.get("type") == "dir"
        ]

    def list_branches(self, per_page: int = 100) -> list[dict[str, Any]]:
        response = self._request_with_retry(
            "GET", f"/repos/{self._owner}/{self._repo}/branches",
            params={"per_page": min(per_page, 100)},
        )
        self._handle_auth_error(response)
        if response.status_code != 200:
            logger.warning("Failed to list branches (HTTP %d): %s", response.status_code, response.text[:200])
            return []
        return [
            {"name": b.get("name", ""), "protected": b.get("protected", False)}
            for b in response.json()
        ]

    def get_branch(self, branch_name: str) -> Optional[dict[str, Any]]:
        response = self._request_with_retry(
            "GET", f"/repos/{self._owner}/{self._repo}/branches/{branch_name}",
        )
        self._handle_auth_error(response)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise GitBranchException(
                branch_name=branch_name,
                error_message=f"Failed to get branch (HTTP {response.status_code}): {response.text[:200]}",
            )
        data = response.json()
        return {"name": data.get("name", ""), "sha": data.get("commit", {}).get("sha", "")}

    def create_branch(self, branch_name: str, from_branch: Optional[str] = None) -> dict[str, Any]:
        source = from_branch or self._branch
        source_info = self.get_branch(source)
        if not source_info:
            raise GitBranchException(
                branch_name=source,
                error_message=f"Source branch '{source}' not found.",
            )
        payload = {"ref": f"refs/heads/{branch_name}", "sha": source_info["sha"]}
        response = self._request_with_retry(
            "POST", f"/repos/{self._owner}/{self._repo}/git/refs", json=payload,
        )
        self._handle_auth_error(response)
        if response.status_code == 422:
            body = response.json() if response.content else {}
            if "reference already exists" in body.get("message", "").lower():
                raise GitBranchAlreadyExistsException(branch_name=branch_name)
            raise GitBranchException(
                branch_name=branch_name,
                error_message=f"GitHub rejected branch creation: {body.get('message', 'Validation failed')}",
            )
        if response.status_code not in (200, 201):
            raise GitBranchException(
                branch_name=branch_name,
                error_message=f"Failed to create branch (HTTP {response.status_code}): {response.text[:200]}",
            )
        data = response.json()
        sha = data.get("object", {}).get("sha", source_info["sha"])
        logger.info("Branch created: %s (sha=%s)", branch_name, sha[:8])
        return {"branch_name": branch_name, "sha": sha}

    def create_pull_request(
        self, title: str, body: str, head_branch: str, base_branch: Optional[str] = None,
    ) -> dict[str, Any]:
        base = base_branch or self._branch
        payload = {"title": title, "body": body, "head": head_branch, "base": base}
        response = self._request_with_retry(
            "POST", f"/repos/{self._owner}/{self._repo}/pulls", json=payload,
        )
        self._handle_auth_error(response)
        if response.status_code == 422:
            body_resp = response.json() if response.content else {}
            errors = body_resp.get("errors", [])
            if any("pull request already exists" in (e.get("message", "")).lower() for e in errors):
                raise GitPRAlreadyExistsException(head_branch=head_branch, base_branch=base)
            raise GitPRException(
                error_message=f"GitHub rejected PR creation: {body_resp.get('message', 'Validation failed')}",
            )
        if response.status_code not in (200, 201):
            raise GitPRException(
                error_message=f"Failed to create PR (HTTP {response.status_code}): {response.text[:200]}",
            )
        data = response.json()
        pr_number = data.get("number", 0)
        logger.info("PR created: #%d (%s -> %s)", pr_number, head_branch, base)
        return {
            "pr_number": pr_number,
            "pr_url": data.get("html_url", ""),
            "title": data.get("title", ""),
            "state": data.get("state", ""),
        }

    def get_pull_request(self, pr_number: int) -> dict[str, Any]:
        response = self._request_with_retry(
            "GET", f"/repos/{self._owner}/{self._repo}/pulls/{pr_number}",
        )
        self._handle_auth_error(response)
        if response.status_code == 404:
            raise GitPRException(error_message=f"Pull request #{pr_number} not found.")
        if response.status_code != 200:
            raise GitPRException(
                error_message=f"Failed to get PR (HTTP {response.status_code}): {response.text[:200]}",
            )
        data = response.json()
        return {
            "pr_number": data.get("number", 0),
            "pr_url": data.get("html_url", ""),
            "title": data.get("title", ""),
            "state": data.get("state", ""),
            "mergeable": data.get("mergeable"),
        }

    def push_combined_file(
        self, file_path: str, content: str, commit_message: str,
        author_name: Optional[str] = None, author_email: Optional[str] = None,
    ) -> dict[str, Any]:
        _existing, current_sha = self.get_file(file_path)
        payload: dict[str, Any] = {
            "message": commit_message,
            "content": b64encode(content.encode("utf-8")).decode("ascii"),
            "branch": self._branch,
        }
        if current_sha:
            payload["sha"] = current_sha
        if author_name and author_email:
            payload["author"] = {"name": author_name, "email": author_email}
        response = self._request_with_retry(
            "PUT", f"/repos/{self._owner}/{self._repo}/contents/{file_path}", json=payload,
        )
        self._handle_auth_error(response)
        if response.status_code == 409:
            raise GitPushFailedException(model_name=file_path, error_message="File was modified externally. Please retry.")
        if response.status_code not in (200, 201):
            raise GitPushFailedException(model_name=file_path, error_message=f"Failed to push file (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        commit_sha = data.get("commit", {}).get("sha", "")
        commit_url = data.get("commit", {}).get("html_url", "")
        new_sha = data.get("content", {}).get("sha", "")
        if new_sha:
            self._sha_cache[file_path] = new_sha
        logger.info("Combined file pushed: %s (commit=%s)", file_path, commit_sha[:8] if commit_sha else "?")
        return {"commit_sha": commit_sha, "commit_url": commit_url}

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


# ── GitLab Implementation ──


class GitLabService(GitServiceBase):
    """GitLab REST API v4 implementation using httpx."""

    def __init__(self, repo_url: str, token: str, branch: str = "main") -> None:
        self._repo_url = repo_url
        self._token = token
        self._branch = branch
        self._base_url, self._project_path = self._parse_url(repo_url)
        self._client = httpx.Client(
            base_url=self._base_url,
            headers={
                "Content-Type": "application/json",
                "PRIVATE-TOKEN": token,
            },
            timeout=HTTPX_TIMEOUT,
        )
        self._sha_cache: dict[str, str] = {}
        logger.debug(
            "GitLabService initialized for %s (branch=%s, token=%s)",
            self._project_path, self._branch, _sanitize_token(self._token),
        )

    def __del__(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    @staticmethod
    def _parse_url(repo_url: str) -> tuple[str, str]:
        """Extract API base URL and URL-encoded project path from a GitLab URL."""
        import urllib.parse
        url = repo_url.strip().rstrip("/")
        if url.endswith(".git"):
            url = url[:-4]
        # SSH: git@gitlab.com:org/repo
        ssh_match = re.match(r"git@([^:]+):(.+?)(?:\.git)?$", url)
        if ssh_match:
            host = ssh_match.group(1)
            path = ssh_match.group(2)
            return f"https://{host}/api/v4", urllib.parse.quote(path, safe="")
        # HTTPS: https://gitlab.com/org/repo
        match = re.match(r"https?://([^/]+)/(.+)", url)
        if not match:
            raise GitConnectionFailedException(error_message=f"Invalid GitLab URL: {_sanitize_url(repo_url)}")
        host = match.group(1)
        path = match.group(2)
        return f"https://{host}/api/v4", urllib.parse.quote(path, safe="")

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        last_exc: Optional[Exception] = None
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._client.request(method, url, **kwargs)
                if response.status_code < 500 and response.status_code != 429:
                    return response
                if response.status_code == 429:
                    retry_after = response.headers.get("retry-after")
                    wait = int(retry_after) if retry_after else backoff
                    logger.warning("GitLab 429 (attempt %d/%d), waiting %ds", attempt, MAX_RETRIES, wait)
                    if attempt < MAX_RETRIES:
                        time.sleep(wait)
                        backoff *= 2
                        continue
                    raise GitRateLimitException()
                logger.warning("GitLab %d (attempt %d/%d), retrying in %.1fs", response.status_code, attempt, MAX_RETRIES, backoff)
                if attempt < MAX_RETRIES:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return response
            except httpx.RequestError as exc:
                last_exc = exc
                logger.warning("GitLab request error (attempt %d/%d): %s", attempt, MAX_RETRIES, str(exc))
                if attempt < MAX_RETRIES:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
        raise GitConnectionFailedException(error_message=f"Network error after {MAX_RETRIES} attempts: {last_exc}")

    def _handle_auth_error(self, response: httpx.Response) -> None:
        if response.status_code == 401:
            raise GitTokenExpiredException()
        if response.status_code == 403:
            raise GitConnectionFailedException(error_message="Access denied. Ensure the token has api or read_api scope.")

    def _proj(self) -> str:
        return self._project_path

    def test_connection(self) -> dict[str, Any]:
        response = self._request_with_retry("GET", f"/projects/{self._proj()}")
        self._handle_auth_error(response)
        if response.status_code == 404:
            raise GitConnectionFailedException(error_message=f"Project not found: {self._project_path}. Check the URL and permissions.")
        if response.status_code != 200:
            raise GitConnectionFailedException(error_message=f"GitLab API error (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        permissions = data.get("permissions", {})
        access = max(
            (permissions.get("project_access") or {}).get("access_level", 0),
            (permissions.get("group_access") or {}).get("access_level", 0),
        )
        if access < 30:  # Developer (30) can push; Maintainer (40) can merge
            raise GitConnectionFailedException(error_message="Developer access or higher required to push to this project.")
        return {
            "success": True,
            "repo_info": {
                "full_name": data.get("path_with_namespace", ""),
                "default_branch": data.get("default_branch", "main"),
                "private": data.get("visibility", "") == "private",
                "permissions": {"push": access >= 30, "admin": access >= 40},
            },
        }

    def get_file(self, path: str, ref: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
        import urllib.parse
        branch = ref or self._branch
        encoded_path = urllib.parse.quote(path, safe="")
        response = self._request_with_retry(
            "GET", f"/projects/{self._proj()}/repository/files/{encoded_path}", params={"ref": branch},
        )
        self._handle_auth_error(response)
        if response.status_code == 404:
            return None, None
        if response.status_code != 200:
            raise GitConnectionFailedException(error_message=f"Failed to read file '{path}' (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        content = b64decode(data.get("content", "")).decode("utf-8")
        sha = data.get("last_commit_id", "")
        self._sha_cache[path] = sha
        return content, sha

    def put_file(self, path: str, content: str, message: str, sha: Optional[str] = None) -> dict[str, Any]:
        import urllib.parse
        encoded_path = urllib.parse.quote(path, safe="")
        payload = {
            "branch": self._branch,
            "content": b64encode(content.encode("utf-8")).decode("ascii"),
            "commit_message": message,
            "encoding": "base64",
        }
        if sha is None:
            sha = self._sha_cache.get(path)
        method = "PUT" if sha else "POST"
        response = self._request_with_retry(method, f"/projects/{self._proj()}/repository/files/{encoded_path}", json=payload)
        self._handle_auth_error(response)
        if response.status_code == 400:
            body = response.json() if response.content else {}
            if "already exists" in body.get("message", "").lower():
                response = self._request_with_retry("PUT", f"/projects/{self._proj()}/repository/files/{encoded_path}", json=payload)
            else:
                raise GitPushFailedException(model_name=path, error_message=f"GitLab rejected file: {body.get('message', '')}")
        if response.status_code not in (200, 201):
            raise GitPushFailedException(model_name=path, error_message=f"Failed to write file '{path}' (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        commit_sha = data.get("commit_id", "") if isinstance(data, dict) else ""
        logger.info("GitLab file written: %s (commit=%s)", path, commit_sha[:8] if commit_sha else "unknown")
        return {"commit_sha": commit_sha, "html_url": ""}

    def delete_file(self, path: str, sha: str, message: str) -> dict[str, Any]:
        import urllib.parse
        encoded_path = urllib.parse.quote(path, safe="")
        payload = {"branch": self._branch, "commit_message": message}
        response = self._request_with_retry("DELETE", f"/projects/{self._proj()}/repository/files/{encoded_path}", json=payload)
        self._handle_auth_error(response)
        if response.status_code not in (200, 204):
            raise GitPushFailedException(model_name=path, error_message=f"Failed to delete file '{path}' (HTTP {response.status_code}): {response.text[:200]}")
        self._sha_cache.pop(path, None)
        logger.info("GitLab file deleted: %s", path)
        return {"commit_sha": ""}

    def list_commits(self, path: Optional[str] = None, page: int = 1, per_page: int = 30) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"ref_name": self._branch, "page": page, "per_page": min(per_page, 100)}
        if path:
            params["path"] = path
        response = self._request_with_retry("GET", f"/projects/{self._proj()}/repository/commits", params=params)
        self._handle_auth_error(response)
        if response.status_code != 200:
            logger.warning("Failed to list commits (HTTP %d): %s", response.status_code, response.text[:200])
            return []
        return [
            {"sha": c.get("id", ""), "message": c.get("message", ""), "author": c.get("author_name", ""), "date": c.get("created_at", ""), "html_url": c.get("web_url", "")}
            for c in response.json()
        ]

    def get_commit_detail(self, sha: str) -> Optional[dict[str, Any]]:
        response = self._request_with_retry("GET", f"/projects/{self._proj()}/repository/commits/{sha}")
        self._handle_auth_error(response)
        if response.status_code in (404, 400):
            return None
        if response.status_code != 200:
            return None
        data = response.json()
        # Fetch diff stats separately
        diff_response = self._request_with_retry("GET", f"/projects/{self._proj()}/repository/commits/{sha}/diff")
        files_changed = []
        if diff_response.status_code == 200:
            for d in diff_response.json():
                files_changed.append({"filename": d.get("new_path", ""), "status": "modified" if not d.get("new_file") else "added", "additions": 0, "deletions": 0})
        return {
            "sha": data.get("id", ""),
            "message": data.get("message", ""),
            "author_name": data.get("author_name", ""),
            "author_email": data.get("author_email", ""),
            "date": data.get("created_at", ""),
            "html_url": data.get("web_url", ""),
            "files_changed": files_changed,
        }

    def list_directory(self, path: str = "", ref: Optional[str] = None) -> list[dict[str, Any]]:
        branch = ref or self._branch
        params: dict[str, Any] = {"ref": branch, "per_page": 100}
        if path:
            params["path"] = path
        response = self._request_with_retry("GET", f"/projects/{self._proj()}/repository/tree", params=params)
        self._handle_auth_error(response)
        if response.status_code != 200:
            logger.warning("Failed to list directory (HTTP %d): %s", response.status_code, response.text[:200])
            return []
        return [
            {"name": entry.get("name", ""), "type": entry.get("type", ""), "path": entry.get("path", "")}
            for entry in response.json()
            if entry.get("type") == "tree"
        ]

    def list_branches(self, per_page: int = 100) -> list[dict[str, Any]]:
        response = self._request_with_retry("GET", f"/projects/{self._proj()}/repository/branches", params={"per_page": min(per_page, 100)})
        self._handle_auth_error(response)
        if response.status_code != 200:
            logger.warning("Failed to list branches (HTTP %d): %s", response.status_code, response.text[:200])
            return []
        return [{"name": b.get("name", ""), "protected": b.get("protected", False)} for b in response.json()]

    def get_branch(self, branch_name: str) -> Optional[dict[str, Any]]:
        import urllib.parse
        encoded = urllib.parse.quote(branch_name, safe="")
        response = self._request_with_retry("GET", f"/projects/{self._proj()}/repository/branches/{encoded}")
        self._handle_auth_error(response)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise GitBranchException(branch_name=branch_name, error_message=f"Failed to get branch (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        return {"name": data.get("name", ""), "sha": data.get("commit", {}).get("id", "")}

    def create_branch(self, branch_name: str, from_branch: Optional[str] = None) -> dict[str, Any]:
        source = from_branch or self._branch
        payload = {"branch": branch_name, "ref": source}
        response = self._request_with_retry("POST", f"/projects/{self._proj()}/repository/branches", json=payload)
        self._handle_auth_error(response)
        if response.status_code == 400:
            body = response.json() if response.content else {}
            if "already exists" in body.get("message", "").lower():
                raise GitBranchAlreadyExistsException(branch_name=branch_name)
            raise GitBranchException(branch_name=branch_name, error_message=f"GitLab rejected branch creation: {body.get('message', '')}")
        if response.status_code not in (200, 201):
            raise GitBranchException(branch_name=branch_name, error_message=f"Failed to create branch (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        sha = data.get("commit", {}).get("id", "")
        logger.info("GitLab branch created: %s (sha=%s)", branch_name, sha[:8])
        return {"branch_name": branch_name, "sha": sha}

    def create_pull_request(self, title: str, body: str, head_branch: str, base_branch: Optional[str] = None) -> dict[str, Any]:
        base = base_branch or self._branch
        payload = {"title": title, "description": body, "source_branch": head_branch, "target_branch": base}
        response = self._request_with_retry("POST", f"/projects/{self._proj()}/merge_requests", json=payload)
        self._handle_auth_error(response)
        if response.status_code == 409:
            raise GitPRAlreadyExistsException(head_branch=head_branch, base_branch=base)
        if response.status_code not in (200, 201):
            body_resp = response.json() if response.content else {}
            if "already exists" in str(body_resp.get("message", "")).lower():
                raise GitPRAlreadyExistsException(head_branch=head_branch, base_branch=base)
            raise GitPRException(error_message=f"Failed to create MR (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        mr_iid = data.get("iid", 0)
        logger.info("GitLab MR created: !%d (%s -> %s)", mr_iid, head_branch, base)
        return {"pr_number": mr_iid, "pr_url": data.get("web_url", ""), "title": data.get("title", ""), "state": data.get("state", "")}

    def get_pull_request(self, pr_number: int) -> dict[str, Any]:
        response = self._request_with_retry("GET", f"/projects/{self._proj()}/merge_requests/{pr_number}")
        self._handle_auth_error(response)
        if response.status_code == 404:
            raise GitPRException(error_message=f"Merge request !{pr_number} not found.")
        if response.status_code != 200:
            raise GitPRException(error_message=f"Failed to get MR (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        return {
            "pr_number": data.get("iid", 0), "pr_url": data.get("web_url", ""), "title": data.get("title", ""),
            "state": data.get("state", ""), "mergeable": data.get("merge_status", "") == "can_be_merged",
        }

    def push_combined_file(
        self, file_path: str, content: str, commit_message: str,
        author_name: Optional[str] = None, author_email: Optional[str] = None,
    ) -> dict[str, Any]:
        import urllib.parse
        encoded_path = urllib.parse.quote(file_path, safe="")
        _existing, current_sha = self.get_file(file_path)
        payload: dict[str, Any] = {
            "branch": self._branch,
            "content": b64encode(content.encode("utf-8")).decode("ascii"),
            "commit_message": commit_message,
            "encoding": "base64",
        }
        if author_name:
            payload["author_name"] = author_name
        if author_email:
            payload["author_email"] = author_email
        method = "PUT" if current_sha else "POST"
        response = self._request_with_retry(method, f"/projects/{self._proj()}/repository/files/{encoded_path}", json=payload)
        self._handle_auth_error(response)
        if response.status_code == 400 and method == "POST":
            body = response.json() if response.content else {}
            if "already exists" in body.get("message", "").lower():
                response = self._request_with_retry("PUT", f"/projects/{self._proj()}/repository/files/{encoded_path}", json=payload)
        if response.status_code not in (200, 201):
            raise GitPushFailedException(model_name=file_path, error_message=f"Failed to push file (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        commit_sha = data.get("commit_id", "") if isinstance(data, dict) else ""
        logger.info("GitLab combined file pushed: %s (commit=%s)", file_path, commit_sha[:8] if commit_sha else "?")
        return {"commit_sha": commit_sha, "commit_url": ""}

    def get_repo_info(self) -> dict[str, Any]:
        response = self._request_with_retry("GET", f"/projects/{self._proj()}")
        self._handle_auth_error(response)
        if response.status_code != 200:
            raise GitConnectionFailedException(error_message=f"Failed to fetch project info (HTTP {response.status_code}): {response.text[:200]}")
        data = response.json()
        return {
            "full_name": data.get("path_with_namespace", ""), "default_branch": data.get("default_branch", "main"),
            "private": data.get("visibility", "") == "private", "permissions": data.get("permissions", {}),
            "description": data.get("description", ""), "html_url": data.get("web_url", ""),
        }


# ── Factory ──


def _is_gitlab_host(repo_url: str) -> bool:
    """Check if a host is a self-hosted GitLab instance by pinging its API."""
    match = re.match(r"https?://([^/]+)", repo_url)
    if not match:
        return False
    host = match.group(1)
    try:
        response = httpx.get(f"https://{host}/api/v4/version", timeout=5.0)
        if response.status_code == 200:
            data = response.json()
            return "version" in data
    except Exception:
        pass
    return False


def get_git_service(config) -> GitServiceBase:
    """Create a git service instance from a GitRepoConfig model."""
    repo_url = config.repo_url or ""
    credentials = config.decrypted_credentials or {}
    token = credentials.get("token", "")
    branch = config.branch_name or "main"
    if "github.com" in repo_url:
        return GitHubService(repo_url, token, branch)
    if "gitlab.com" in repo_url or _is_gitlab_host(repo_url):
        return GitLabService(repo_url, token, branch)
    raise UnsupportedGitProviderException(repo_url=_sanitize_url(repo_url))
