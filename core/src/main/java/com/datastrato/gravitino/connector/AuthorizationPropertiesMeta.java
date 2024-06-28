/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.connector;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class AuthorizationPropertiesMeta {
  /** Ranger admin web URIs */
  public static final String RANGER_ADMIN_URL = "ranger.admin.url";
  /** Ranger authentication type kerberos or simple */
  public static final String RANGER_AUTH_TYPE = "ranger.auth.type";
  /**
   * Ranger admin web login username(auth_type=simple), or kerberos principal(auth_type=kerberos)
   */
  public static final String RANGER_USERNAME = "ranger.username";
  /**
   * Ranger admin web login user password(auth_type=simple), or path of the keytab
   * file(auth_type=kerberos)
   */
  public static final String RANGER_PASSWORD = "ranger.password";
  /** Ranger service name */
  public static final String RANGER_SERVICE_NAME = "ranger.service.name";

  public static final Map<String, PropertyEntry<?>> RANGER_AUTHORIZATION_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              RANGER_ADMIN_URL,
              PropertyEntry.stringRequiredPropertyEntry(
                  RANGER_ADMIN_URL, "The Ranger admin web URIs", true, false))
          .put(
              RANGER_AUTH_TYPE,
              PropertyEntry.stringRequiredPropertyEntry(
                  RANGER_AUTH_TYPE,
                  "The Ranger admin web auth type (kerberos/simple)",
                  true,
                  false))
          .put(
              RANGER_USERNAME,
              PropertyEntry.stringRequiredPropertyEntry(
                  RANGER_USERNAME, "The Ranger admin web login username", true, false))
          .put(
              RANGER_PASSWORD,
              PropertyEntry.stringRequiredPropertyEntry(
                  RANGER_PASSWORD, "The Ranger admin web login password", true, false))
          .build();
}
