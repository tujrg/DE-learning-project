FROM apache/airflow:2.10.5

# Copy a requirements file into the image
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --use-deprecated=legacy-resolver -r requirements.txt
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow