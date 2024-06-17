/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.Authorization;
import com.datastrato.gravitino.authorization.AuthorizationOperations;
import com.datastrato.gravitino.authorization.AuthorizationProvider;
import com.datastrato.gravitino.authorization.BaseAuthorization;
import com.datastrato.gravitino.authorization.Privilege;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/** Implementation of a Mysql catalog in Gravitino. */
public class RangerAuthorization extends BaseAuthorization<RangerAuthorization> {

  RangerAuthorization() {
    mapPrivileges = ImmutableMap.<Privilege.Name, String>builder()
            .put(Privilege.Name.TABULAR_SELECT, "select")
            .put(Privilege.Name.TABULAR_UPDATE, "update")
            .put(Privilege.Name.TABULAR_CREATE, "create")
            .put(Privilege.Name.TABULAR_DROP, "drop")
            .put(Privilege.Name.TABULAR_ALTER, "alter")
            .put(Privilege.Name.TABULAR_INDEX, "index")
            .put(Privilege.Name.TABULAR_LOCK, "lock")
            .put(Privilege.Name.TABULAR_READ, "read")
            .put(Privilege.Name.TABULAR_WRITE, "write")
            .put(Privilege.Name.TABULAR_REPL_ADMIN, "repladmin")
            .put(Privilege.Name.TABULAR_SERVICE_ADMIN, "serviceadmin")
            .put(Privilege.Name.TABULAR_ALL, "all")
            .build();
  }

  @Override
  public String shortName() {
    return "ranger";
  }

  @Override
  protected AuthorizationOperations newOps(Map<String, String> config) {
    return new RangerAuthorizationOperations();
  }
}
