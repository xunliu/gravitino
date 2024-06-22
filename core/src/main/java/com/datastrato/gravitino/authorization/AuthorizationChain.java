/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.util.function.Function;
import java.util.stream.Stream;

/* Manages the authorization instances and operations. */
public class AuthorizationChain<T> {
  public Function<T, T> buildChain(Function<T, T>... functions) {
    return Stream.of(functions)
        .reduce((collected, next) -> collected.andThen(next))
        .orElseThrow(() -> new IllegalArgumentException("Empty Chain found"));
  }
}
