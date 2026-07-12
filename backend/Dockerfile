# A Dockerfile is a recipe. Each instruction creates a cached "layer" — Docker
# only rebuilds layers whose inputs changed, which makes rebuilds fast.

# 1. Base image: a minimal Debian with Python 3.13 preinstalled.
#    "-slim" strips out extras we don't need → smaller, faster image.
FROM python:3.13-slim

# 2. Environment tweaks for running Python inside containers:
#    - DONTWRITEBYTECODE: don't litter .pyc files
#    - UNBUFFERED: send logs straight to the terminal (so `docker logs` is live)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. All following commands run inside /app in the image's filesystem.
WORKDIR /app

# 4. Copy ONLY requirements first, then install. This is a deliberate ordering
#    trick: as long as requirements.txt doesn't change, Docker reuses the cached
#    "pip install" layer even when your app code changes → much faster rebuilds.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Now copy the actual application code.
COPY app ./app

# 6. Document that the app listens on port 8000 (informational).
EXPOSE 8000

# 7. Run as a non-root user inside the container (safer default).
RUN useradd --create-home --uid 10001 appuser
USER appuser

# 8. The command that runs when the container starts.
#    0.0.0.0 (not localhost) so it's reachable from outside the container.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
