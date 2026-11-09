FROM python:3.11-slim

ARG PIP_INDEX_URL=https://package-mirror.liara.ir/repository/pypi/simple
ARG PIP_TRUSTED_HOST=package-mirror.liara.ir

ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "-u", "producer.py"]