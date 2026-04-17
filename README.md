<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/logo-light.png" />
    <source media="(prefers-color-scheme: light)" srcset="docs/images/logo-dark.png" />
    <img src="docs/images/logo-dark.png" alt="Visitran" width="200" />
  </picture>
</p>

<p align="center">
  <strong>Agentic &amp; No-Code Data Transformations</strong><br />
  Vibe coded pipelines: say hello to accuracy and maintainability.
</p>

<p align="center">
  <a href="https://visitran.com">Website</a> &middot;
  <a href="https://docs.visitran.com">Documentation</a> &middot;
  <a href="https://github.com/Zipstack/visitran/issues">Issues</a> &middot;
  <a href="#contributing">Contributing</a>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/github/license/Zipstack/visitran" alt="License" /></a>
  <a href="https://github.com/Zipstack/visitran/releases"><img src="https://img.shields.io/github/v/release/Zipstack/visitran" alt="Latest Release" /></a>
  <a href="https://deepwiki.com/Zipstack/visitran"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki" /></a>
</p>
<p align="center">
  <img src="https://img.shields.io/badge/python-3.10.x-blue.svg" alt="Python" />
  <img src="https://img.shields.io/badge/node-16%2B-green.svg" alt="Node.js" />
  <a href="https://github.com/astral-sh/uv"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json" alt="uv" /></a>
</p>

---

## What is Visitran?

Visitran is the modern, agent-native data transformation platform that lets you create sophisticated, yet highly standardized transformation metrics and highly maintainable transformation projects with AI. It is built on a strong foundation of Python (powered by Ibis) and SQL, while affording you a simple, conversational interface.

### I just vibe code my pipelines. Why do I need Visitran?

First off, congratulations. You are using modern tools to quickly solve pains you have. The trouble starts when everyone in your organization is vibe coding pipelines and seeing different numbers for critical metrics. Pandemonium ensues. As it is, it's tough for even humans to come to an agreement on how core metrics are defined. With Visitran, you get the best of both worlds. You "vibe code" your pipelines with Visitran AI, but you can collaborate with your team members and build transformations that are agreed upon, verifiable and maintainable. Speed: say hi to accuracy. Accuracy: say hi to maintainability.


**Supported Databases**

| Database | Status | Database | Status |
|----------|--------|----------|--------|
| PostgreSQL | :white_check_mark: | DuckDB | :white_check_mark: |
| Snowflake | :white_check_mark: | BigQuery | :white_check_mark: |
| Trino | :white_check_mark: | Databricks | :white_check_mark: |

> If Visitran helps you, please star this repo — it helps others discover the project!

## Key Features

**For Data Engineers**
- No-code backed by Python-based transformation models with Ibis SQL generation
- 6 database adapters with table, view, and incremental materialization
- DAG execution engine with dependency resolution
- Job scheduling with cron/interval triggers, retry, and chaining

**For Analysts & No-Code Users**
- No-code model builder — joins, filters, aggregates, window functions, pivots, unions
- Formula Columns support: Create complex transformations with Excel-like formulas
- AI assistant for natural language to data transformations
- Built-in testing framework for model validation

## Getting Started

Requires [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/).

```bash
git clone https://github.com/Zipstack/visitran.git
cd visitran
cp backend/sample.env backend/.env
cd docker
docker compose up --build -d
```

Open [http://localhost:3000](http://localhost:3000) and **Sign Up** to create your account — that's it!

> **AI Features (optional):** Get an API key from [app.visitran.com](https://app.visitran.com) and add `VISITRAN_AI_KEY=vtk_...` to `backend/.env`, then restart with `docker compose restart backend`.

<details>
<summary><strong>Services started by Docker Compose</strong></summary>

| Service | Port | Description |
|---------|------|-------------|
| Frontend | `3000` | React UI (Nginx) |
| Backend | `8000` | Django REST API (Gunicorn) |
| PostgreSQL | `5432` | Database |
| Redis | `6379` | WebSockets and async task broker |
| Celery Worker | — | Background job processing |
| Celery Beat | — | Scheduled task processing |

To stop: `docker compose down`
To stop and delete all data: `docker compose down -v`

</details>

<details>
<summary><strong>Environment variables</strong></summary>

The `sample.env` ships with working defaults — no edits needed to get started. For production, replace these keys:

| Variable | Default | How to Generate (production) |
|----------|---------|------------------------------|
| `SECRET_KEY` | Provided | Use [djecrety.ir](https://djecrety.ir/) or `python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"` |
| `VISITRAN_ENCRYPTION_KEY` | Provided | `python -c "from base64 import b64encode; from cryptography.fernet import Fernet; print(b64encode(Fernet.generate_key()).decode())"` |
| `VISITRAN_RSA_PRIVATE_KEY` | Provided | Generate a 2048-bit RSA key pair (see `sample.env`) |
| `VISITRAN_RSA_PUBLIC_KEY` | Provided | Derived from the private key above |

</details>

<details>
<summary><strong>Local development (without Docker)</strong></summary>

For development or when you want full control over each component.

**Prerequisites**
- Python 3.10.x (`>=3.10, <3.11.1` — required by `pyproject.toml`)
- Node.js 16+
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Redis (for WebSockets and Celery) — install via `brew install redis` (macOS) or your OS package manager

**Backend**

```bash
git clone https://github.com/Zipstack/visitran.git
cd visitran
cp backend/sample.env backend/.env
```

Edit `backend/.env` for local development:

| Variable | Required | Value for localhost |
|----------|----------|---------------------|
| `DB_HOST` | No | Leave **empty** for SQLite, or `localhost` for local PostgreSQL |
| `REDIS_HOST` | Yes | `localhost` |
| `DB_SAMPLE_HOST` | No | `localhost` (requires PostgreSQL) or leave empty to skip |

> **Important:** Change `DB_HOST=postgres` → `DB_HOST=` (empty) and `REDIS_HOST=redis` → `REDIS_HOST=localhost`.

```bash
cd backend
pip install uv
uv sync
source .venv/bin/activate
python manage.py migrate
python manage.py runserver
```

**Frontend**

```bash
cd frontend
cp sample.env .env
npm install
npm start
```

Open [http://localhost:3000](http://localhost:3000) and **sign up** to create your account.

</details>

## Project Structure

```
backend/
├── backend/                     # Django application
│   ├── core/                    #   Models, views, REST API
│   ├── application/             #   Business logic, config parser, interpreter
│   └── server/settings/         #   Django settings
├── visitran/                    # Core transformation engine (standalone)
│   ├── adapters/                #   Database adapters (postgres, duckdb, snowflake, etc.)
│   ├── templates/               #   VisitranModel base class
│   └── visitran.py              #   DAG execution engine

frontend/src/
├── base/                        # App shell, routing, project listing
├── ide/                         # Main workspace (editor, file explorer, SQL drawer)
│   ├── chat-ai/                 #   AI assistant interface
│   └── editor/                  #   No-code visual model builder
├── store/                       # Zustand state management
├── service/                     # API services (axios, socket)
└── widgets/                     # Reusable UI components

docker/
├── dockerfiles/                 # Backend and frontend Dockerfiles
└── docker-compose.yaml          # Docker Compose configuration
```

## Development

### Backend

```bash
# Run server with hot reload
cd backend && python manage.py runserver

# Run migrations
python manage.py migrate

# Lint & type check
inv checks
inv type_check

# Run tests (requires Docker for test databases)
docker compose up --wait
uv run pytest -vv --dist loadgroup -n 5 tests
docker compose down
```

### Frontend

```bash
cd frontend

npm start              # Dev server
npm run build          # Production build
npm test               # Run tests
npm run test:coverage  # Test with coverage
npm run lint           # ESLint check
npm run lint:fix       # ESLint auto-fix
```

## Cloud & Enterprise

For teams that need managed infrastructure, advanced features, or enterprise-grade security.

- **SSO & RBAC** — SAML/OIDC single sign-on with role-based access control
- **Managed AI** — Built-in AI service with no API key setup required
- **Job Scheduling** — Cron/interval triggers with retry, chaining, and notifications
- **Cloud Storage** — Managed file storage with version control
- **Multi-Tenant** — Organization-level isolation and team management
- **Priority Support** — Dedicated support with SLA guarantees

<a href="https://visitran.com/book-free-demo/">
  <img src="https://img.shields.io/badge/Book%20a%20Free%20Demo-blue?style=for-the-badge" alt="Book a Free Demo" />
</a>

## Visitran AI

The AI assistant generates data transformations from natural language. In OSS mode, AI access requires a **Visitran AI subscription key**:

1. Sign up at [app.visitran.com](https://app.visitran.com)
2. Subscribe to an AI plan and generate an API key
3. Go to **Settings > Visitran AI** in your local instance and enter your key
4. Open the Chat AI drawer in the IDE to start using AI

The cloud gateway validates your key, routes prompts through LLM models (Claude, GPT-4o, Gemini), and streams responses back — no local AI setup or LLM credentials needed.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Backend | Python, Django, Django REST Framework, Celery |
| Frontend | React, Ant Design, Zustand, Monaco Editor |
| SQL Generation | [Ibis](https://ibis-project.org/) |
| Databases | PostgreSQL, DuckDB, Snowflake, BigQuery, Trino, Databricks |
| Infrastructure | Docker, Redis, Gunicorn |

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

1. Fork the repo and create a feature branch from `main`
2. Make your changes
3. Run linting:
   - Frontend: `npm run lint`
   - Backend: `inv checks`
4. Run tests:
   - Frontend: `npm test`
   - Backend: `uv run pytest`
5. Submit a pull request

## Community

Join the Visitran community:

[![Slack](https://img.shields.io/badge/Slack-4CAF50?style=flat)](https://visitran.slack.com)
[![Blog](https://img.shields.io/badge/Blog-FF6B6B?style=flat)](https://visitran.com/blog/)
[![LinkedIn](https://img.shields.io/badge/Follow%20us%20on%20LinkedIn-C8A2E8?style=flat)](https://www.linkedin.com/company/visitran/posts/?feedView=all)
[![X](https://img.shields.io/badge/Follow%20us%20on%20X-FFD700?style=flat)](https://x.com/GetVisitran)

## A Note on Analytics

Visitran integrates Posthog to track minimal usage analytics. Disable by setting `REACT_APP_ENABLE_POSTHOG=false` in the frontend's `.env` file.

## License

Visitran is licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE). You are free to use, modify, and distribute this software under the terms of the AGPL-3.0. If you run a modified version as a network service, you must make the source code available to users of that service.

---

<div align="center">
  <p>Built with care by the <a href="https://visitran.com">Visitran</a> team</p>
  <p>
    <a href="https://visitran.com">Website</a> &middot;
    <a href="https://docs.visitran.com">Documentation</a> &middot;
    <a href="https://visitran.com/book-free-demo/">Book a Demo</a>
  </p>
</div>
