/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.NameIdentifier;

public interface AuthorizationPolicy {
  Policy createPolicy(String name, NameIdentifier securableEntity, Function[] rowFilterFuns,
                      Function[] maskingFuns, boolean overwrite) throws UnsupportedOperationException;

  Boolean dropPolicy(String name) throws UnsupportedOperationException;

  Policy updatePolicy(String name, PolicyChange... changes) throws UnsupportedOperationException;
}
