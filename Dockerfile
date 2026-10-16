FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY fal_gateway ./fal_gateway
COPY dashboard ./dashboard-source
RUN python -m fal_gateway.dashboard --source /app/dashboard-source --output /app/dashboard && \
    rm -rf /app/dashboard-source && pip install --no-cache-dir . && pip check && \
    useradd --uid 10001 --system --home /nonexistent fal
USER 10001:10001
EXPOSE 8000 8001
CMD ["python", "-m", "fal_gateway.server"]
