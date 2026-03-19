# Contributing to Visitran

Thank you for your interest in contributing to Visitran! This guide will help you get started.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## How to Contribute

### Reporting Bugs

- Search [existing issues](https://github.com/Zipstack/visitran/issues) first to avoid duplicates
- Use the bug report template and include:
  - Steps to reproduce
  - Expected vs actual behavior
  - Environment details (OS, Python version, database adapter)

### Suggesting Features

- Open a [feature request](https://github.com/Zipstack/visitran/issues/new) describing the use case
- Explain why this would be useful to other Visitran users

### Submitting Code

1. **Fork** the repository and create a branch from `main`
2. **Set up** your development environment (see below)
3. **Make** your changes
4. **Test** your changes
5. **Submit** a pull request

## Development Setup

### Prerequisites

- Python >= 3.10
- Node.js 18+
- [uv](https://docs.astral.sh/uv/) (Python package manager — installed automatically by `inv install`)
- Docker (for running test databases)

### Backend

```bash
pip install invoke
inv install
source .venv/bin/activate

cd backend
python manage.py migrate
python manage.py runserver
```

### Frontend

```bash
cd frontend
npm install
npm start
```

## Code Quality

### Before Submitting a PR

**Backend:**

```bash
inv checks        # Linting
inv type_check    # Type checking
```

**Frontend:**

```bash
npm run lint       # ESLint
npm run lint:fix   # Auto-fix
npm test           # Tests
```

### Running Tests

```bash
# Backend (requires Docker for test databases)
docker compose up --wait
uv run pytest -vv --dist loadgroup -n 5 tests
docker compose down

# Frontend
npm test
npm run test:coverage
```

## Pull Request Guidelines

- Keep PRs focused — one feature or fix per PR
- Write descriptive commit messages
- Update documentation if your change affects user-facing behavior
- Ensure all CI checks pass before requesting review
- Reference related issues in your PR description

## Project Structure

See the [README](README.md#project-structure) for a map of the codebase.

## License

By contributing to Visitran, you agree that your contributions will be licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE).
