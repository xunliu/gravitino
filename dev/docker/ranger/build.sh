#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
docker run -it -v $(pwd):/root/build-ranger -p 6080:6080 debian:buster bash # openjdk:8-jdk-buster bash

# docker pull openjdk:11.0.15-jdk-bullseye

apt-get -q update && apt-get install -y -q python python3 mariadb-server vim curl wget openjdk-11-jdk git procps


vi /usr/share/vim/vim81/defaults.vim
#if has('mouse')
#  if &term =~ 'xterm'
#    set mouse=a
#  else
#    set mouse=nvi
#  endif
#endif

cd /root
wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.6.3/apache-maven-3.6.3-bin.tar.gz
#https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.8.1/apache-maven-3.8.1-bin.tar.gz
# https://aka.ms/download-jdk/microsoft-jdk-11.0.22-linux-aarch64.tar.gz
tar xzvf apache-maven-3.6.3-bin.tar.gz
#mv apache-maven-3.6.3 /opt


export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
echo "JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64" >> ~/.bashrc
# /usr/lib/jvm/java-11-openjdk-arm64
# /usr/lib/jvm/java-11-openjdk-amd64
export MVN_HOME=/root/apache-maven-3.6.3
echo "MVN_HOME=/root/apache-maven-3.6.3" >> ~/.bashrc
echo "export PATH=${JAVA_HOME}/bin:${MVN_HOME}/bin:$PATH" >> ~/.bashrc
source ~/.bashrc


# https://aka.ms/download-jdk/microsoft-jdk-11.0.22-linux-aarch64.tar.gz

# wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.5.3/apache-maven-3.5.3-bin.tar.gz
#tar xzvf apache-maven-3.5.3-bin.tar.gz
#v apache-maven-3.5.3 /opt
#echo "export PATH=/opt/apache-maven-3.8.8/bin:$PATH" >> ~/.bashrc
#source ~/.bashrc

wget https://downloads.apache.org/ranger/2.4.0/apache-ranger-2.4.0.tar.gz

# mvn clean compile package assembly:assembly install -pl '!hive-agent'
# mvn -DskipTests=true clean compile package install -P ranger-trino-plugin,'!linux' -am
# mvn -DskipTests=true clean compile package -P ranger-trino-plugin

#wget https://downloads.apache.org/ranger/2.3.0/apache-ranger-2.3.0.tar.gz

tar zxvf apache-ranger-2.4.0.tar.gz
ln -s apache-ranger-2.4.0 apache-ranger

/tmp/ranger-build/build-ranger.sh

cd apache-ranger && mvn -DskipTests=true -Pranger-jdk11 clean compile package \
 && cp target/ranger-2.4.0-admin.tar.gz /opt \
 && cd /opt \
 && tar zxvf ranger-2.4.0-admin.tar.gz \
 && ln -s ranger-2.4.0-admin ranger-admin
 #&& chmod +x /opt/ranger-entrypoint.sh

#mkdir /usr/share/java/
#curl -L https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar --output /usr/share/java/mysql-connector-java.jar

curl -L https://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar --output /opt/ranger-admin/ews/webapp/WEB-INF/lib/mysql-connector-java-8.0.28.jar
cp /opt/ranger-admin/ews/webapp/WEB-INF/lib/mysql-connector-java-8.0.28.jar /opt/ranger-2.4.0-admin/jisql/lib/
curl -L https://repo1.maven.org/maven2/com/googlecode/log4jdbc/log4jdbc/1.2/log4jdbc-1.2.jar --output /opt/ranger-admin/ews/webapp/WEB-INF/lib/log4jdbc-1.2.jar

cp -r /opt/ranger-admin/ews/webapp/WEB-INF/classes/conf.dist/ /opt/ranger-admin/ews/webapp/WEB-INF/classes/conf

sed -i 's|audit_store=solr|audit_store=DB|g' /opt/ranger-admin/install.properties
sed -i 's|db_password=|db_password=r@ngerR0cks|g' /opt/ranger-admin/install.properties
sed -i 's|rangerAdmin_password=|rangerAdmin_password=r@ngerR0cks|g' /opt/ranger-admin/install.properties
sed -i 's|rangerTagsync_password=|rangerTagsync_password=r@ngerR0cks|g' /opt/ranger-admin/install.properties
sed -i 's|rangerUsersync_password=|rangerUsersync_password=r@ngerR0cks|g' /opt/ranger-admin/install.properties
sed -i 's|keyadmin_password=|keyadmin_password=r@ngerR0cks|g' /opt/ranger-admin/install.properties

#chmod 777 /opt/ranger-admin/setup.sh
sed -i 's|check_java_version|#check_java_version|g' /opt/ranger-admin/setup.sh
sed -i 's|#check_java_version()|check_java_version()|g' /opt/ranger-admin/setup.sh

sed -i 's|check_db_connector|#check_db_connector|g' /opt/ranger-admin/setup.sh
sed -i 's|#check_db_connector()|check_db_connector()|g' /opt/ranger-admin/setup.sh

sed -i 's|copy_db_connector|#copy_db_connector|g' /opt/ranger-admin/setup.sh
sed -i 's|#copy_db_connector()|copy_db_connector()|g' /opt/ranger-admin/setup.sh

#service mariadb restart
service mysql restart
mysql -uroot -p < /tmp/ranger-build/init-mysql.sql

/opt/ranger-admin/ews/ranger-admin-services.sh start

## Trino host
curl -L http://172.19.0.4:6080/ranger-2.4.0-trino-plugin.tar.gz --output ./ranger-2.4.0-trino-plugin.tar.gz

wget http://172.19.0.4:6080/ranger-2.4.0-trino-plugin.tar.gz


## install  plugin into trino server
# https://blog.csdn.net/qq_36096641/article/details/127518912
sed -i 's|POLICY_MGR_URL=|POLICY_MGR_URL=http://localhost:6080|g' /opt/ranger-2.4.0-trino-plugin/install.properties
sed -i 's|REPOSITORY_NAME=|REPOSITORY_NAME=trinodev|g' /opt/ranger-2.4.0-trino-plugin/install.properties
echo "XAAUDIT.SUMMARY.ENABLE=true" >> /opt/ranger-2.4.0-trino-plugin/install.properties
sed -i 's|COMPONENT_INSTALL_DIR_NAME|COMPONENT_INSTALL_DIR_NAME=/data/trino/|g' /opt/ranger-2.4.0-trino-plugin/install.properties
sed -i 's|echo "$1=$2">>$3|echo "\n$1=$2">>$3|g' /opt/ranger-2.4.0-trino-plugin/enable-trino-plugin.sh
/opt/ranger-2.4.0-trino-plugin/enable-trino-plugin.sh

需要在 jvm.config 最后一行加一个空行，否则执行 /opt/ranger-2.4.0-trino-plugin/enable-trino-plugin.sh 会报错

#vi /var/run/ranger/rangeradmin.pid

# ranger-trino-audit.xml


ranger-security.xml
