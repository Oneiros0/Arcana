FROM python:3.11-slim

WORKDIR /app

# Install only production dependencies
COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .

ENTRYPOINT ["arcana"]
