/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.rel.types.Type;

public interface AuthorizationFunction {
  Function createFunction(String name, Function.Parameter[] parameter, String expression, Type returns) throws UnsupportedOperationException;
}
