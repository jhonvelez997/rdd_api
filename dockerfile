FROM openjdk:18.0.2.1-slim-buster as builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip

ENV SPARK_VERSION=3.5.1 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz

FROM builder as apache-spark

WORKDIR /opt/spark

RUN apt-get install --fix-broken

RUN apt-get install --fix-broken

RUN apt-get install -y python-dev

COPY requirements.txt /opt/spark/requirements.txt

RUN pip3 install --upgrade pip

RUN pip3 install -r /opt/spark/requirements.txt

RUN apt-get install -y supervisor

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 7000 8888 5000 8000

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY start-spark.sh /

RUN mkdir /app

ADD app.py /app/app.py
ADD templates /app/templates
ADD static /app/static
ADD ml-100k /app/ml-100k

WORKDIR /app

#CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
#CMD ["gunicorn", "app:app", "-b", "0.0.0.0:8000"]
CMD ["uwsgi", "--ini", "wsgi.ini"]

