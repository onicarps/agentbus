# Experimental stdio MCP container scaffold. AgentBus does not expose HTTP/SSE.
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    AGENTBUS_WORKSPACE=/data/workspace

WORKDIR /app

# Install system dependencies (sqlite3, git, ca-certificates)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    sqlite3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install AgentBus package with all optional extras (devex, sdk, obs)
COPY pyproject.toml README.md /app/
COPY src/ /app/src/
RUN pip install --no-cache-dir "/app[devex,sdk,obs]"

# Default data volume mount point
RUN mkdir -p /data/workspace && chmod 777 /data/workspace

VOLUME ["/data/workspace"]

# AgentBus serve is an MCP stdio process. This image is not an HTTP service.
ENTRYPOINT ["agentbus"]
CMD ["serve", "--workspace", "/data/workspace"]
