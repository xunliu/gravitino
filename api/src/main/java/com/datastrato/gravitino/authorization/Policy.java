/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.annotation.Evolving;

import java.util.List;

@Evolving
public interface Policy extends MetadataObject {

  enum Type {
    ROW_FILTER,
    MASKING
  }

  /**
   * The name of the policy.
   *
   * @return The name of the policy.
   */
  String name();

  List<Function> rowFilter();

  List<Function> masking();
}
