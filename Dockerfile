# One image for both roles: `burst-controller` (default, also serves the dashboard) or `burst-worker`.

# -- dashboard ---------------------------------------------------------------------------------------
FROM node:24-slim AS dashboard
WORKDIR /dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build

# -- controller / worker ----------------------------------------------------------------------------
FROM python:3.13-slim

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml README.md burst.example.toml ./
COPY burst burst
RUN pip install ".[kubernetes,aws]" && useradd --create-home burst && mkdir /data && chown burst /data
COPY --from=dashboard /dashboard/dist dashboard/dist

USER burst
ENV BURST_DB_PATH=/data/burst.db BURST_HOST=0.0.0.0 BURST_DASHBOARD_DIR=/app/dashboard/dist
EXPOSE 8000
CMD ["burst-controller"]
