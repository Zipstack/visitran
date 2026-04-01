# type: ignore
from __future__ import annotations

from invoke import Context, task


@task
def install(context):
    # type: (Context) -> None
    """Install dependencies and pre-commit hooks."""
    c: Context = context
    c.run("pip install --upgrade pip", pty=True)
    c.run("pip install uv", pty=True)
    c.run("pip install mypy", pty=True)
    c.run("uv sync", pty=True)
    c.run("uv run pre-commit install", pty=True)
    c.run("git lfs install", pty=True)
    c.run("git lfs pull", pty=True)


@task
def checks(context):
    # type: (Context) -> None
    """Run pre-commit hooks."""
    c: Context = context
    c.run("uv run pre-commit install", pty=True)
    c.run("uv run pre-commit run --all-files", pty=True)


@task
def test_visitran(context):
    # type: (Context) -> None
    """Run tests."""
    c: Context = context
    c.run(
        "docker-compose down && docker-compose build && \
            docker-compose up --wait && sleep 5",
        pty=True,
    )
    c.run(
        "uv run pytest -vv --dist loadgroup -n 5 tests cloud",
        pty=True,
    )
    c.run(
        "docker-compose down",
        pty=True,
    )


@task
def test_visitran_with_coverage(context):
    # type: (Context) -> None
    """Run tests with coverage."""
    c: Context = context
    c.run(
        "docker-compose down && docker-compose build && \
            docker-compose up --wait && sleep 5",
        pty=True,
    )
    c.run(
        "uv run pytest -vv --cov=visitran --cov=adapters --cov=cloud \
            --cov-report=xml --cov-config=pyproject.toml --dist loadgroup -n 5 tests cloud",
        pty=True,
    )
    c.run(
        "docker-compose down",
        pty=True,
    )


@task
def update(context):
    # type: (Context) -> None
    """Update packages and pre-commit."""
    c: Context = context
    c.run("uv sync --upgrade", pty=True)
    c.run("uv sync", pty=True)
    c.run("uv run pre-commit autoupdate", pty=True)


@task
def type_check(context):
    # type: (Context) -> None
    """Run type check."""
    c: Context = context
    c.run("uv run mypy --config-file .mypy.ini ./", pty=True)


@task
def testall(context):
    # type: (Context) -> None
    """Run tests in all tox environments."""
    c: Context = context
    c.run("uv run tox", pty=True)


@task
def clean(context):
    # type: (Context) -> None
    """Remove all pyc files."""
    c: Context = context
    c.run('find . | grep -E "(/__pycache__$|\\.pyc$|\\.pyo$)" | xargs rm -rf', pty=True)
