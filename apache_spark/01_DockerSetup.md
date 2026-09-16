# Docker Setup Steps

## Step 1 — Install Docker Desktop

On Windows, install **Docker Desktop** and start it.

Verify from PowerShell:

```
docker --version
```

You should get something similar to:

```
Docker version 28.x.x
```

Then verify Docker Compose:

```
docker compose version
```

You should get:

```
Docker Compose version v2.x.x
```

---

## Step 2 — Create a project folder

For example:

```
C:\projects\pyspark-docker
```

Open PowerShell:

```
mkdir C:\projects\pyspark-docker
cd C:\projects\pyspark-docker
```

---

## Step 3 — Put your Python files in this folder

For now, just put your three files here:

```
C:\projects\pyspark-docker
│
├── 01-pyspark-api.py
├── 02-pyspark-udf.py
└── 03-data-pipeline.py
```

Don't change the Python files yet.

---

## Step 4 — Create a Dockerfile

Create:

```
C:\projects\pyspark-docker\Dockerfile
```

Put:

```
FROM apache/spark:3.5.6-python3

USER root

# Python dependencies used by the pipeline
RUN pip install --no-cache-dir \
    boto3 \
    py4j

# Add Hadoop AWS support for Amazon S3
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jars/

# Install AWS CLI v2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
        -o "/tmp/awscliv2.zip" && \
    unzip -q /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    rm -rf /tmp/aws /tmp/awscliv2.zip && \
    apt-get remove -y curl unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV SPARK_HOME=/opt/spark
ENV PATH="/usr/local/bin:${SPARK_HOME}/bin:${PATH}"

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-*.zip:${PYTHONPATH}"

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["bash"]
```

---

## Step 5 — Build the Docker image

From:

```
C:\projects\pyspark-docker
```

run:

```
docker build -t pyspark-local .
```

Check that the image exists:

```
docker images
```

You should see:

```
REPOSITORY       TAG       ...
pyspark-local    latest    ...
```

---

## Step 6 — Start a Docker container with your local folder mounted

This is the **important part for your requirement**.

Run:

```
docker run -it --rm --name pyspark-local -v "${PWD}:/app" -v "$HOME\.aws:/root/.aws:ro" pyspark-local
```

You should now be inside the Docker container:

```
root@xxxxxxxx:/app#
```

---

## Step 7 — Configure AWS credentials inside the container

The image includes the AWS CLI, but it still needs credentials before Spark can read/write S3.

If you mounted `$HOME\.aws` in Step 6, credentials are already picked up automatically — verify them:

```
aws sts get-caller-identity
```

You should see your AWS account ID, user ID, and ARN.

If instead the mount was skipped, or you want to use different credentials inside the container, run:

```
aws configure
```

You'll be prompted for:

```
AWS Access Key ID [None]: <your access key>
AWS Secret Access Key [None]: <your secret key>
Default region name [None]: ap-southeast-2
Default output format [None]: json
```

This writes `/root/.aws/credentials` and `/root/.aws/config` inside the container.

Confirm it worked:

```
aws sts get-caller-identity
```

> Note: Credentials entered this way live only inside the container and are lost when the container is removed (`--rm`). Mounting `$HOME\.aws` (Step 6) is the persistent option; `aws configure` is useful for a quick one-off session or testing different credentials.

---

## Step 8 — Verify your local files are visible

Inside Docker:

```
ls -la
```

You should see:

```
01-pyspark-api.py
02-pyspark-udf.py
03-data-pipeline.py
Dockerfile
```

This proves that your Windows folder is mounted into Docker.

---

## Step 9 — Test the synchronization

**Keep the Docker terminal open.**

On Windows, open:

```
03-data-pipeline.py
```

in VS Code.

Add something simple, for example:

```
# TEST CHANGE
```

Save the file.

Then, inside Docker:

```
grep "TEST CHANGE" /app/03-data-pipeline.py
```

You should see:

```
# TEST CHANGE
```

That proves:

```
Windows file
     ↓
   Save
     ↓
Docker /app file
     ↓
Immediately updated
```

You don't need to copy the file.

---

## Step 10 — Test Spark

Inside the container:

```
spark-submit --version
```

Then:

```
python3 -c "import pyspark; print(pyspark.__version__)"
```

You should see the Spark/PySpark version.

---

## Step 11 — Test your UDF example

Your `02-pyspark-udf.py` doesn't depend on local input files, so it's the best first Spark test.

Inside Docker:

```
spark-submit /app/02-pyspark-udf.py
```

If it executes successfully, you have:

```text
Windows
   │
   │ local Python files
   ▼
Docker
   │
   ├── Python
   ├── Java
   ├── Spark
   └── PySpark
```

working correctly.

---