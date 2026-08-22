# The fake warehouse and the fake customer API, so the stack has something to migrate
# the moment it starts. Nothing in the platform depends on this image.
FROM python:3.12-alpine

WORKDIR /mock

RUN pip install --no-cache-dir pymongo==4.10.1

COPY databricks_mock.py ./

# Seeded on first start and kept in the image's own filesystem. A volume would make the
# row count survive a rebuild, which is the opposite of what a demo wants: delete the
# container, get a clean hundred thousand rows.
EXPOSE 8099

# Bound to every interface here, unlike the local script, because a container's ports are
# reachable only through the ones Compose publishes.
ENV MOCK_BIND=0.0.0.0

# Without this Python buffers stdout whenever it is not a terminal, so `docker logs` shows
# nothing at all until the process exits — which for a server is never. The whole point of
# this container printing what it was asked is that somebody can watch it.
ENV PYTHONUNBUFFERED=1

CMD ["python", "databricks_mock.py", "100000", "1000"]
