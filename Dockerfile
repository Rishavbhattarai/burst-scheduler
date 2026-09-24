# One image for both roles: `burst-controller` (default) or `burst-worker`.
FROM python:3.13-slim

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md burst.example.toml ./
COPY burst burst
RUN pip install ".[kubernetes,aws]" && useradd --create-home burst && mkdir /data && chown burst /data

USER burst
ENV BURST_DB_PATH=/data/burst.db BURST_HOST=0.0.0.0
EXPOSE 8000
CMD ["burst-controller"]
