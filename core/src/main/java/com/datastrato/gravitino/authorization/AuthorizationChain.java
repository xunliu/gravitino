/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.IsolatedClassLoader;
import com.datastrato.gravitino.utils.ThrowableFunction;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/* Manages the authorization instances and operations. */
public class AuthorizationChain<T> {
  public Function<T, T> buildChain(Function<T, T>... functions) {
    return Stream.of(functions)
            .reduce((collected, next) -> collected.andThen(next))
            .orElseThrow(() -> new IllegalArgumentException("Empty Chain found"));
  }
}
