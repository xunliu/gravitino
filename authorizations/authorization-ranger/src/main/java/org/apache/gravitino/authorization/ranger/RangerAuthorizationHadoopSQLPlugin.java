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
package org.apache.gravitino.authorization.ranger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerPrivileges.RangerHadoopSQLPrivilege;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;
import org.apache.gravitino.connector.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.connector.authorization.AuthorizationPrivilege;
import org.apache.gravitino.connector.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerAuthorizationHadoopSQLPlugin extends RangerAuthorizationPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(RangerAuthorizationHadoopSQLPlugin.class);
  private static volatile RangerAuthorizationHadoopSQLPlugin instance = null;

  private RangerAuthorizationHadoopSQLPlugin(String catalogProvider, Map<String, String> config) {
    super(catalogProvider, config);
  }

  public static synchronized RangerAuthorizationHadoopSQLPlugin getInstance(
      String catalogProvider, Map<String, String> config) {
    if (instance == null) {
      synchronized (RangerAuthorizationHadoopSQLPlugin.class) {
        if (instance == null) {
          instance = new RangerAuthorizationHadoopSQLPlugin(catalogProvider, config);
        }
      }
    }
    return instance;
  }

  @Override
  /** Set the default mapping Gravitino privilege name to the Ranger rule */
  public Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.CREATE_CATALOG,
        ImmutableSet.of(RangerHadoopSQLPrivilege.CREATE),
        Privilege.Name.USE_CATALOG,
        ImmutableSet.of(RangerHadoopSQLPrivilege.SELECT),
        Privilege.Name.CREATE_SCHEMA,
        ImmutableSet.of(RangerHadoopSQLPrivilege.CREATE),
        Privilege.Name.USE_SCHEMA,
        ImmutableSet.of(RangerHadoopSQLPrivilege.SELECT),
        Privilege.Name.CREATE_TABLE,
        ImmutableSet.of(RangerHadoopSQLPrivilege.CREATE),
        Privilege.Name.MODIFY_TABLE,
        ImmutableSet.of(
            RangerHadoopSQLPrivilege.UPDATE,
            RangerHadoopSQLPrivilege.ALTER,
            RangerHadoopSQLPrivilege.WRITE),
        Privilege.Name.SELECT_TABLE,
        ImmutableSet.of(RangerHadoopSQLPrivilege.READ, RangerHadoopSQLPrivilege.SELECT));
  }

  @Override
  /** Set the default owner rule. */
  public Set<AuthorizationPrivilege> ownerMappingRule() {
    return ImmutableSet.of(RangerHadoopSQLPrivilege.ALL);
  }

  @Override
  /** Set Ranger policy resource rule. */
  public List<String> policyResourceDefinesRule() {
    return ImmutableList.of(
        PolicyResource.DATABASE.getName(),
        PolicyResource.TABLE.getName(),
        PolicyResource.COLUMN.getName());
  }

  @Override
  /** Allow privilege operation defines rule. */
  public Set<Privilege.Name> allowPrivilegesRule() {
    return ImmutableSet.of(
        Privilege.Name.CREATE_CATALOG,
        Privilege.Name.USE_CATALOG,
        Privilege.Name.CREATE_SCHEMA,
        Privilege.Name.USE_SCHEMA,
        Privilege.Name.CREATE_TABLE,
        Privilege.Name.MODIFY_TABLE,
        Privilege.Name.SELECT_TABLE);
  }

  /**
   * Allow Gravitino MetadataObject type defines rule.
   *
   * @return To allow Gravitino MetadataObject type defines rule.
   */
  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return ImmutableSet.of(
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
        MetadataObject.Type.COLUMN);
  }

  /** Translate the Gravitino securable object to the Ranger owner securable object. */
  @Override
  public List<AuthorizationSecurableObject> translateOwner(MetadataObject gravitinoMetadataObject) {
    List<AuthorizationSecurableObject> AuthorizationSecurableObjects = new ArrayList<>();

    switch (gravitinoMetadataObject.type()) {
      case METALAKE:
      case CATALOG:
        // Add `*` for the SCHEMA permission
        AuthorizationSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(RangerHelper.RESOURCE_ALL),
                RangerHiveMetadataObject.Type.SCHEMA,
                ownerMappingRule()));
        // Add `*.*` for the TABLE permission
        AuthorizationSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL),
                RangerHiveMetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `*.*.*` for the COLUMN permission
        AuthorizationSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL),
                RangerHiveMetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      case SCHEMA:
        // Add `{schema}` for the SCHEMA permission
        AuthorizationSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(gravitinoMetadataObject.name() /*Schema name*/),
                RangerHiveMetadataObject.Type.SCHEMA,
                ownerMappingRule()));
        // Add `{schema}.*` for the TABLE permission
        AuthorizationSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(
                    gravitinoMetadataObject.name() /*Schema name*/, RangerHelper.RESOURCE_ALL),
                RangerHiveMetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `{schema}.*.*` for the COLUMN permission
        AuthorizationSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(
                    gravitinoMetadataObject.name() /*Schema name*/,
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL),
                RangerHiveMetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      case TABLE:
        // Add `{schema}.{table}` for the TABLE permission
        AuthorizationSecurableObjects.add(
            generateAuthorizationSecurableObject(
                translateMetadataObject(gravitinoMetadataObject).names(),
                RangerHiveMetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `{schema}.{table}.*` for the COLUMN permission
        AuthorizationSecurableObjects.add(
            generateAuthorizationSecurableObject(
                Stream.concat(
                        translateMetadataObject(gravitinoMetadataObject).names().stream(),
                        Stream.of(RangerHelper.RESOURCE_ALL))
                    .collect(Collectors.toList()),
                RangerHiveMetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      default:
        throw new AuthorizationPluginException(
            "The owner privilege is not supported for the securable object: %s",
            gravitinoMetadataObject.type());
    }

    return AuthorizationSecurableObjects;
  }

  /** Translate the Gravitino securable object to the Ranger securable object. */
  @Override
  public List<AuthorizationSecurableObject> translatePrivilege(SecurableObject securableObject) {
    List<AuthorizationSecurableObject> AuthorizationSecurableObjects = new ArrayList<>();

    securableObject.privileges().stream()
        .filter(Objects::nonNull)
        .forEach(
            gravitinoPrivilege -> {
              Set<AuthorizationPrivilege> rangerPrivileges = new HashSet<>();
              // Ignore unsupported privileges
              if (!privilegesMappingRule().containsKey(gravitinoPrivilege.name())) {
                return;
              }
              privilegesMappingRule().get(gravitinoPrivilege.name()).stream()
                  .forEach(
                      rangerPrivilege ->
                          rangerPrivileges.add(
                              new RangerPrivileges.RangerHadoopSQLPrivilegeImpl(
                                  rangerPrivilege, gravitinoPrivilege.condition())));

              switch (gravitinoPrivilege.name()) {
                case CREATE_CATALOG:
                  // Ignore the Gravitino privilege `CREATE_CATALOG` in the
                  // RangerAuthorizationHivePlugin
                  break;
                case USE_CATALOG:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`*`)
                      AuthorizationSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerHiveMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                case CREATE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add Ranger privilege(`CREATE`) to SCHEMA(`*`)
                      AuthorizationSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerHiveMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                case USE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`*`)
                      AuthorizationSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerHiveMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    case SCHEMA:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`{schema}`)
                      AuthorizationSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(securableObject.name() /*Schema name*/),
                              RangerHiveMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                case CREATE_TABLE:
                case MODIFY_TABLE:
                case SELECT_TABLE:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add `*.*` for the TABLE permission
                      AuthorizationSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(
                                  RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL),
                              RangerHiveMetadataObject.Type.TABLE,
                              rangerPrivileges));
                      // Add `*.*.*` for the COLUMN permission
                      AuthorizationSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL),
                              RangerHiveMetadataObject.Type.COLUMN,
                              rangerPrivileges));
                      break;
                    case SCHEMA:
                      // Add `{schema}.*` for the TABLE permission
                      AuthorizationSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(
                                  securableObject.name() /*Schema name*/,
                                  RangerHelper.RESOURCE_ALL),
                              RangerHiveMetadataObject.Type.TABLE,
                              rangerPrivileges));
                      // Add `{schema}.*.*` for the COLUMN permission
                      AuthorizationSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(
                                  securableObject.name() /*Schema name*/,
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL),
                              RangerHiveMetadataObject.Type.COLUMN,
                              rangerPrivileges));
                      break;
                    case TABLE:
                      if (gravitinoPrivilege.name() == Privilege.Name.CREATE_TABLE) {
                        throw new AuthorizationPluginException(
                            "The privilege %s is not supported for the securable object: %s",
                            gravitinoPrivilege.name(), securableObject.type());
                      } else {
                        // Add `{schema}.{table}` for the TABLE permission
                        AuthorizationSecurableObjects.add(
                            generateAuthorizationSecurableObject(
                                translateMetadataObject(securableObject).names(),
                                RangerHiveMetadataObject.Type.TABLE,
                                rangerPrivileges));
                        // Add `{schema}.{table}.*` for the COLUMN permission
                        AuthorizationSecurableObjects.add(
                            generateAuthorizationSecurableObject(
                                Stream.concat(
                                        translateMetadataObject(securableObject).names().stream(),
                                        Stream.of(RangerHelper.RESOURCE_ALL))
                                    .collect(Collectors.toList()),
                                RangerHiveMetadataObject.Type.COLUMN,
                                rangerPrivileges));
                      }
                      break;
                    default:
                      LOG.warn(
                          "RangerAuthorizationHivePlugin -> privilege {} is not supported for the securable object: {}",
                          gravitinoPrivilege.name(),
                          securableObject.type());
                  }
                  break;
                default:
                  LOG.warn(
                      "RangerAuthorizationHivePlugin -> privilege {} is not supported for the securable object: {}",
                      gravitinoPrivilege.name(),
                      securableObject.type());
              }
            });

    return AuthorizationSecurableObjects;
  }

  /**
   * Because the Ranger metadata object is different from the Gravitino metadata object, we need to
   * convert the Gravitino metadata object to the Ranger metadata object.
   */
  @Override
  public AuthorizationMetadataObject translateMetadataObject(MetadataObject metadataObject) {
    Preconditions.checkArgument(
        allowMetadataObjectTypesRule().contains(metadataObject.type()),
        String.format(
            "The metadata object type %s is not supported in the RangerAuthorizationHivePlugin",
            metadataObject.type()));
    Preconditions.checkArgument(
        !(metadataObject instanceof RangerPrivileges),
        "The metadata object must be not a RangerPrivileges object.");
    List<String> nsMetadataObject =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    Preconditions.checkArgument(
        nsMetadataObject.size() > 0, "The metadata object must have at least one name.");

    AuthorizationMetadataObject.Type type;
    if (metadataObject.type() == MetadataObject.Type.METALAKE
        || metadataObject.type() == MetadataObject.Type.CATALOG) {
      nsMetadataObject.clear();
      nsMetadataObject.add(RangerHelper.RESOURCE_ALL);
      type = RangerHiveMetadataObject.Type.SCHEMA;
    } else {
      nsMetadataObject.remove(0); // Remove the catalog name
      type = RangerHiveMetadataObject.Type.fromMetadataType(metadataObject.type());
    }

    RangerHiveMetadataObject rangerMetadataObject =
        new RangerHiveMetadataObject(
            AuthorizationMetadataObject.getParentFullName(nsMetadataObject),
            AuthorizationMetadataObject.getLastName(nsMetadataObject),
            type);
    rangerMetadataObject.validateAuthorizationMetadataObject();
    return rangerMetadataObject;
  }

  /**
   * IF remove the SCHEMA, need to remove these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` <br>
   * IF remove the TABLE, need to remove these the relevant policies, `{schema}.*`, `{schema}.*.*`
   * <br>
   * IF remove the COLUMN, Only need to remove `{schema}.*.*` <br>
   */
    @Override
    protected void doRemoveMetadataObject(AuthorizationMetadataObject authMetadataObject) {
    switch (authMetadataObject.metadataObjectType()) {
      case SCHEMA:
        doRemoveSchemaMetadataObject(authMetadataObject);
        break;
      case TABLE:
        doRemoveTableMetadataObject(authMetadataObject);
        break;
      case COLUMN:
        removePolicyByMetadataObject(authMetadataObject.names());
        break;
      default:
        throw new IllegalArgumentException(
                "Unsupported metadata object type: " + authMetadataObject.type());
    }
  }

  /**
   * Remove the SCHEMA, Need to remove these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` permissions.
   */
  private void doRemoveSchemaMetadataObject(AuthorizationMetadataObject authMetadataObject) {
    Preconditions.checkArgument(
            authMetadataObject.type() == RangerHiveMetadataObject.Type.SCHEMA,
            "The metadata object type must be SCHEMA");
    Preconditions.checkArgument(
            authMetadataObject.names().size() == 1, "The metadata object names must be 1");
    if (RangerHelper.RESOURCE_ALL.equals(authMetadataObject.name())) {
      // Delete metalake or catalog policies in this Ranger service
      try {
        List<RangerPolicy> policies = rangerClient.getPoliciesInService(rangerServiceName);
        policies.stream()
                .filter(RangerHelper::hasGravitinoManagedPolicyItem)
                .forEach(rangerHelper::removeAllGravitinoManagedPolicyItem);
      } catch (RangerServiceException e) {
        throw new RuntimeException(e);
      }
    } else {
      List<List<String>> loop =
              ImmutableList.of(
                      ImmutableList.of(authMetadataObject.name())
                      /** SCHEMA permission */
                      ,
                      ImmutableList.of(authMetadataObject.name(), RangerHelper.RESOURCE_ALL)
                      /** TABLE permission */
                      ,
                      ImmutableList.of(
                              authMetadataObject.name(), RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL)
                      /** COLUMN permission */
              );
      for (List<String> resNames : loop) {
        removePolicyByMetadataObject(resNames);
      }
    }
  }

  /**
   * Remove the TABLE, Need to remove these the relevant policies, `*.{table}`, `*.{table}.{column}`
   * permissions.
   */
  private void doRemoveTableMetadataObject(
          AuthorizationMetadataObject AuthorizationMetadataObject) {
    List<List<String>> loop =
            ImmutableList.of(
                    AuthorizationMetadataObject.names()
                    /** TABLE permission */
                    ,
                    Stream.concat(
                                    AuthorizationMetadataObject.names().stream(),
                                    Stream.of(RangerHelper.RESOURCE_ALL))
                            .collect(Collectors.toList())
                    /** COLUMN permission */
            );
    for (List<String> resNames : loop) {
      removePolicyByMetadataObject(resNames);
    }
  }

  /**
   * Remove the policy by the metadata object names. <br>
   *
   * @param metadataNames The metadata object names.
   */
  private void removePolicyByMetadataObject(List<String> metadataNames) {
    List<RangerPolicy> policies = rangerHelper.wildcardSearchPolies(metadataNames);
    Map<String, String> preciseFilters = new HashMap<>();
    for (int i = 0; i < metadataNames.size(); i++) {
      preciseFilters.put(rangerHelper.policyResourceDefines.get(i), metadataNames.get(i));
    }
    policies =
            policies.stream()
                    .filter(
                            policy ->
                                    policy.getResources().entrySet().stream()
                                            .allMatch(
                                                    entry ->
                                                            preciseFilters.containsKey(entry.getKey())
                                                                    && entry.getValue().getValues().size() == 1
                                                                    && entry
                                                                    .getValue()
                                                                    .getValues()
                                                                    .contains(preciseFilters.get(entry.getKey()))))
                    .collect(Collectors.toList());
    policies.forEach(rangerHelper::removeAllGravitinoManagedPolicyItem);
  }
}
