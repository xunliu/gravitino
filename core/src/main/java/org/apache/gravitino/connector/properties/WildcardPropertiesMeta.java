/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.connector.properties;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WildcardPropertiesMeta {
  public static final String WILDCARD = "*";

  public static final String WILDCARD_CONFIG_VALUES_SPLITTER = ",";

  public static void validate(
          PropertiesMetadata propertiesMetadata, Map<String, String> properties)
          throws IllegalArgumentException {
    // Get all wildcard properties from PropertiesMetadata
    List<String> wildcardProperties =
            propertiesMetadata.propertyEntries().keySet().stream()
                    .filter(propertiesMetadata::isWildcardProperty)
                    .collect(Collectors.toList());
    if (wildcardProperties.size() > 0) {
      // Find the wildcard config key from the properties
      List<String> wildcardConfigKeys =
              wildcardProperties.stream()
                      .filter(key -> !key.contains(WILDCARD))
                      .collect(Collectors.toList());
      Preconditions.checkArgument(
              wildcardConfigKeys.size() == 1,
              "Only one wildcard config key is allowed, found: %s",
              wildcardConfigKeys);
      String wildcardConfigKey = wildcardConfigKeys.get(0);
      // Get the wildcard values from the properties
      List<String> wildcardValues =
              Arrays.stream(
                              properties
                                      .get(wildcardConfigKey)
                                      .split(WILDCARD_CONFIG_VALUES_SPLITTER))
                      .map(String::trim)
                      .collect(Collectors.toList());
      wildcardValues.stream()
              .filter(v -> v.contains("."))
              .forEach(
                      v -> {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Wildcard property values cannot be set with `.` character in the `%s = %s`.",
                                        wildcardConfigKey, properties.get(wildcardConfigKey)));
                      });
      Preconditions.checkArgument(
              wildcardValues.size() == wildcardValues.stream().distinct().count(),
              "Duplicate values in wildcard config values: %s",
              wildcardValues);

      // Get all wildcard properties with wildcard values
      List<Pattern> patterns =
              wildcardProperties.stream()
                      .filter(k -> k.contains(WILDCARD))
                      .collect(Collectors.toList())
                      .stream()
                      .map(
                              wildcard ->
                                      wildcard
                                              .replace(".", "\\.")
                                              .replace(WILDCARD, "([^.]+)"))
                      .map(Pattern::compile)
                      .collect(Collectors.toList());

      List<String> wildcardPrefix =
              wildcardProperties.stream()
                      .filter(s -> s.contains(WILDCARD))
                      .map(
                              s ->
                                      s.substring(
                                              0, s.indexOf(WILDCARD)))
                      .distinct()
                      .collect(Collectors.toList());

      for (String key :
              properties.keySet().stream()
                      .filter(
                              k ->
                                      !k.equals(wildcardConfigKey)
                                              && wildcardPrefix.stream().anyMatch(k::startsWith))
                      .collect(Collectors.toList())) {
        boolean matches =
                patterns.stream()
                        .anyMatch(
                                pattern -> {
                                  Matcher matcher = pattern.matcher(key);
                                  if (matcher.find()) {
                                    String group = matcher.group(1);
                                    return wildcardValues.contains(group);
                                  } else {
                                    return false;
                                  }
                                });
        Preconditions.checkArgument(
                matches,
                "Wildcard properties `%s` not a valid wildcard config with values: %s",
                key,
                wildcardValues);
      }
    }
  }
}
