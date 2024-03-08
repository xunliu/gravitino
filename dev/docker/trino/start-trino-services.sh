#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

if [[ -n "${RANGER_REPOSITORY_NAME}" && -n "${RANGER_SERVER_URL}" ]]; then
  cd /tmp
  tar zxvf ranger-${RANGER_VERSION}-trino-plugin.tar.gz
  cd ranger-${RANGER_VERSION}-trino-plugin
  sed -i "s/POLICY_MGR_URL=/POLICY_MGR_URL=${RANGER_SERVER_URL}/g" install.properties
  sed -i "s/REPOSITORY_NAME=/REPOSITORY_NAME=${RANGER_REPOSITORY_NAME}/g" install.properties
  echo "XAAUDIT.SUMMARY.ENABLE=true" >> install.properties
  sed -i 's|COMPONENT_INSTALL_DIR_NAME|COMPONENT_INSTALL_DIR_NAME=/data/trino/|g' install.properties
  ln -s /etc/trino /data/trino/etc
  ln -s /usr/lib/trino/plugin /data/trino/plugin
  /tmp/ranger-${RANGER_VERSION}-trino-plugin/enable-trino-plugin.sh
fi

/etc/trino/update-trino-conf.sh
/usr/lib/trino/bin/run-trino
