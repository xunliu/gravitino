/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.AuthorizationOperations;
import java.io.IOException;
import java.util.Map;

public class RangerAuthorizationOperations implements AuthorizationOperations {
  public RangerAuthorizationOperations() {}

  @Override
  public void initialize(Map<String, String> config) throws RuntimeException {}

  @Override
  public void close() throws IOException {}
}
