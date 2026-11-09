FROM apache/superset:3.1.3

USER root

ARG PIP_INDEX_URL=https://package-mirror.liara.ir/repository/pypi/simple
ARG PIP_TRUSTED_HOST=package-mirror.liara.ir

ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}

RUN pip install --no-cache-dir clickhouse-connect==0.7.19

USER superset