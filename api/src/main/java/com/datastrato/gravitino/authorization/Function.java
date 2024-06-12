/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.types.Type;

import java.util.List;
import java.util.Map;

@Evolving
public interface Function {
  static public class Parameter {
    String name;
    Type type;
  }

  Policy.Type type();

  /**
   * The name of the policy.
   *
   * @return The name of the policy.
   */
  String name();

  List<Parameter> parameters();

  String expression();

//  Type returns();
}
