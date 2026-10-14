FROM apache/airflow:2.9.2-python3.11

USER root

# Install modern static Docker CLI (v27.1.1) to communicate with host Docker daemon (API >= 1.44)
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.1.1.tgz | \
    tar -xzC /usr/local/bin --strip-components=1 docker/docker && \
    chmod +x /usr/local/bin/docker && \
    groupadd -g 999 docker 2>/dev/null || true && \
    usermod -aG root airflow

USER airflow
