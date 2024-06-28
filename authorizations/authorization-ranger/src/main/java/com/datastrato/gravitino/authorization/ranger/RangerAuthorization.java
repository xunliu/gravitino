/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.AuthorizationHook;
import com.datastrato.gravitino.authorization.BaseAuthorization;
import java.util.Map;

/** Implementation of a Ranger authorization in Gravitino. */
public class RangerAuthorization extends BaseAuthorization<RangerAuthorization> {

  public RangerAuthorization() {}

  @Override
  public String shortName() {
    return "ranger";
  }

  @Override
  protected AuthorizationHook newHook(String catalogProvider, Map<String, String> config) {
    switch (catalogProvider) {
      case "hive":
        return new RangerHiveAuthorizationHook(catalogProvider, config);
      default:
        throw new IllegalArgumentException(
            "Authorization hook unsupported catalog provider: " + catalogProvider);
    }
  }
}
