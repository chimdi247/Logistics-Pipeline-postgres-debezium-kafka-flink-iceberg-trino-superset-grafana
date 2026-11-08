# Synthetic logistics data generator. It writes to PostgreSQL only; Debezium does the rest.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LOG_FORMAT=json

RUN useradd --create-home --uid 10001 generator
WORKDIR /app

RUN pip install "psycopg[binary]>=3.2,<4.0"

COPY --chown=generator:generator generators/generate_logistics_data.py /app/generators/generate_logistics_data.py
COPY --chown=generator:generator infra/generator/entrypoint.sh /app/entrypoint.sh
RUN chmod 0555 /app/entrypoint.sh

USER generator
ENTRYPOINT ["/app/entrypoint.sh"]
