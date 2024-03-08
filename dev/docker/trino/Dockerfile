#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
FROM debian:buster as jdk11

ARG RANGER_VERSION=2.4.0

WORKDIR /root

RUN apt-get -q update && \
    apt-get install -y -q wget openjdk-11-jdk

RUN wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.6.3/apache-maven-3.6.3-bin.tar.gz && \
    tar zxvf apache-maven-3.6.3-bin.tar.gz && \
    ln -s /root/apache-maven-3.6.3/bin/mvn /usr/local/bin/mvn

RUN wget https://downloads.apache.org/ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz && \
    tar zxvf apache-ranger-${RANGER_VERSION}.tar.gz && \
    ln -s apache-ranger-${RANGER_VERSION} apache-ranger && \
    cd apache-ranger && \
    mvn clean package -DskipTests -P ranger-trino-plugin,-linux -am -pl distro,plugin-trino,ranger-trino-plugin-shim,agents-installer,credentialbuilder

FROM trinodb/trino:426
LABEL maintainer="support@datastrato.com"

ARG IMAGE_NAME
ARG RANGER_VERSION=2.4.0
ENV RANGER_REPOSITORY_NAME=""
ENV RANGER_SERVER_URL=""

USER root

# Only mysql, hudi, iceberg, mariadb, jmx, memory, tpch, tpcds, postgresql, hive plugin are kept
RUN rm -rf /usr/lib/trino/plugin/accumulo  \
    && rm -rf /usr/lib/trino/plugin/blackhole \
    && rm -rf /usr/lib/trino/plugin/delta-lake \
    && rm -rf /usr/lib/trino/plugin/example-http \
    && rm -rf /usr/lib/trino/plugin/geospatial \
    && rm -rf /usr/lib/trino/plugin/kafka \
    && rm -rf /usr/lib/trino/plugin/local-file \
    && rm -rf /usr/lib/trino/plugin/ml \
    && rm -rf /usr/lib/trino/plugin/mysql-event-listener \
    && rm -rf /usr/lib/trino/plugin/phoenix5 \
    && rm -rf /usr/lib/trino/plugin/prometheus \
    && rm -rf /usr/lib/trino/plugin/redshift \
    && rm -rf /usr/lib/trino/plugin/singlestore \
    && rm -rf /usr/lib/trino/plugin/thrift \
    && rm -rf /usr/lib/trino/plugin/atop \
    && rm -rf /usr/lib/trino/plugin/cassandra \
    && rm -rf /usr/lib/trino/plugin/druid \
    && rm -rf /usr/lib/trino/plugin/exchange-filesystem \
    && rm -rf /usr/lib/trino/plugin/google-sheets \
    && rm -rf /usr/lib/trino/plugin/http-event-listener \
    && rm -rf /usr/lib/trino/plugin/ignite \
    && rm -rf /usr/lib/trino/plugin/kinesis \
    && rm -rf /usr/lib/trino/plugin/mongodb \
    && rm -rf /usr/lib/trino/plugin/oracle \
    && rm -rf /usr/lib/trino/plugin/pinot \
    && rm -rf /usr/lib/trino/plugin/raptor-legacy \
    && rm -rf /usr/lib/trino/plugin/resource-group-managers \
    && rm -rf /usr/lib/trino/plugin/sqlserver \
    && rm -rf /usr/lib/trino/plugin/bigquery \
    && rm -rf /usr/lib/trino/plugin/clickhouse \
    && rm -rf /usr/lib/trino/plugin/elasticsearch \
    && rm -rf /usr/lib/trino/plugin/exchange-hdfs \
    && rm -rf /usr/lib/trino/plugin/kudu \
    && rm -rf /usr/lib/trino/plugin/password-authenticators \
    && rm -rf /usr/lib/trino/plugin/redis \
    && rm -rf /usr/lib/trino/plugin/session-property-managers \
    && rm -rf /usr/lib/trino/plugin/teradata-functions
COPY --chown=trino:trino packages/trino/conf /etc/trino
COPY --chown=trino:trino packages/gravitino-trino-connector/mysql-connector-java-8.0.27.jar /usr/lib/trino/plugin/iceberg/
COPY --chown=trino:trino packages/gravitino-trino-connector/postgresql-42.7.0.jar /usr/lib/trino/plugin/iceberg/

RUN mkdir /tmp/gravitino
COPY --chown=trino:trino packages/gravitino-trino-connector /tmp/gravitino

RUN if [ "$IMAGE_NAME" = "datastrato/trino" ] ; then \
    mv /tmp/gravitino /usr/lib/trino/plugin/; \
    else echo "Copying files for other images"; \
    fi


# The following ENV are needed to update configuration file before starting Trino server, you need to
# pass these ENV variables to the container when starting it like `docker run --env GRAVITINO_HOST_IP=xxx --env GRAVITINO_HOST_PORT=xxxx`
#ENV GRAVITINO_HOST_IP
#ENV GRAVITINO_HOST_PORT
#ENV GRAVITINO_METALAKE_NAME
#ENV HADOOP_USER_NAME

# Copy the Ranger plugin to the
COPY --from=jdk11 /root/apache-ranger/target/ranger-${RANGER_VERSION}-trino-plugin.tar.gz /tmp

ENTRYPOINT ["/bin/bash", "-c", "/etc/trino/update-trino-conf.sh && /usr/lib/trino/bin/run-trino"]
