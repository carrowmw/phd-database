FROM python:3.10-slim-bookworm

ARG DATABASE_ENV=development

LABEL maintainer="{{ cookiecutter.project_domain }}"
LABEL vendor="{{ cookiecutter.project_domain }}"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.8.3 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PATH="/opt/poetry/bin:$PATH"

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN curl -sSL https://install.python-poetry.org | python3 - \
    && ln -s /opt/poetry/bin/poetry /usr/local/bin/poetry

# Verify Poetry installation
RUN poetry --version


# Copy the poetry configuration
COPY pyproject.toml poetry.lock ./
COPY phd_package/__init__.py phd_package/
COPY phd_package/database phd_package/database/
COPY phd_package/utils phd_package/utils/
COPY phd_package/config phd_package/config/

# Install project dependencies
RUN if [ "$DATABASE_ENV" = "production" ]; then \
    poetry install --no-dev --no-root; \
    else \
    poetry install --no-root; \
    fi

# Run the entrypoint
CMD ["poetry", "run", "python", "-m", "phd_package.database"]

