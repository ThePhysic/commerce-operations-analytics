FROM python:3.12.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    COMMERCE_ROOT=/app \
    COMMERCE_DATA_DIR=/workspace/data
WORKDIR /app

COPY requirements.lock ./requirements.lock
RUN python -m pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml README.md ./
COPY src ./src
COPY warehouse ./warehouse
COPY contracts ./contracts
COPY queries ./queries
COPY fixtures ./fixtures
COPY scripts ./scripts
RUN python -m pip install --no-cache-dir --no-deps . \
    && mkdir -p /workspace/data

ENTRYPOINT ["commerce"]
CMD ["demo"]
