FROM openjdk:11.0.16
LABEL maintainer="support@datastrato.com"

ARG RANGER_VERSION
ENV RANGER_PASSWORD=rangerR0cks!

WORKDIR /root

COPY init-mysql.sql.template /tmp/
COPY start-ranger-services.sh /tmp/
RUN chmod +x /tmp/start-ranger-services.sh

RUN apt-get -q update && \
    apt-get install -y -q mariadb-server vim curl wget git procps &&\
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/local/openjdk-11
#RUN ARCH=$(uname -m) && \
#    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
#    ln -s /usr/lib/jvm/java-11-openjdk-arm64 ${JAVA_HOME}; \
#    else \
#    ln -s /usr/lib/jvm/java-11-openjdk-amd64 ${JAVA_HOME}; \
#    fi

ADD packages/ranger-distro-${RANGER_VERSION}-admin.tar.gz /opt/
RUN ln -s /opt/ranger-${RANGER_VERSION}-admin /opt/ranger-admin

# Initialize Ranger envirioment
RUN curl -L https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar --output /opt/ranger-admin/ews/webapp/WEB-INF/lib/mysql-connector-java-8.0.28.jar && \
    cp /opt/ranger-admin/ews/webapp/WEB-INF/lib/mysql-connector-java-8.0.28.jar /opt/ranger-admin/jisql/lib/ && \
    curl -L https://repo1.maven.org/maven2/com/googlecode/log4jdbc/log4jdbc/1.2/log4jdbc-1.2.jar --output /opt/ranger-admin/ews/webapp/WEB-INF/lib/log4jdbc-1.2.jar && \
    cp -r /opt/ranger-admin/ews/webapp/WEB-INF/classes/conf.dist/ /opt/ranger-admin/ews/webapp/WEB-INF/classes/conf && \
    mkdir /opt/ranger-admin/ews/logs

EXPOSE 6080

ENTRYPOINT ["/bin/bash", "-c", "/tmp/start-ranger-services.sh"]
