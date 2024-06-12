/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.NameIdentifier;

public interface AuthorizationRole {
  Role createRole(String name) throws UnsupportedOperationException;

  boolean dropRole(String name) throws UnsupportedOperationException;

  Role updateRole(String name, RoleChange... changes) throws UnsupportedOperationException;
}
