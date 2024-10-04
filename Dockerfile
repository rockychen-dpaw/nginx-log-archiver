# syntax=docker/dockerfile:1
# Prepare the base environment.
FROM python:3.12.7-slim AS builder_base_archiver
LABEL org.opencontainers.image.authors=asi@dbca.wa.gov.au
LABEL org.opencontainers.image.source=https://github.com/dbca-wa/nginx-log-archiver

RUN apt-get update -y \
  && apt-get upgrade -y \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --root-user-action=ignore --no-cache-dir --upgrade pip

# Install Python libs using Poetry.
FROM builder_base_archiver AS python_libs_archiver
WORKDIR /app
ARG POETRY_VERSION=1.8.3
RUN pip install --root-user-action=ignore --no-cache-dir poetry==${POETRY_VERSION}
COPY poetry.lock pyproject.toml ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --only main

# Set up a non-root user.
ARG UID=10001
ARG GID=10001
RUN groupadd -g ${GID} appuser \
  && useradd --no-create-home --no-log-init --uid ${UID} --gid ${GID} appuser

# Install the project.
FROM python_libs_archiver
COPY archiver.py ./
USER ${UID}
