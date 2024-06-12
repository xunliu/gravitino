/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.NameIdentifier;

import java.io.Closeable;
import java.util.Map;

public interface AuthorizationUser {
  NameIdentifier grantRole(String user, String group, Policy policy) throws UnsupportedOperationException;

  NameIdentifier revokePolicyFromUser(String user, String group, NameIdentifier policyIdent) throws UnsupportedOperationException;
}
