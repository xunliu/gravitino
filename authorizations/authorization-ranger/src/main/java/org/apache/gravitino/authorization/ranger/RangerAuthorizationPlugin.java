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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationHookException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;

/** Ranger authorization operations hooks interfaces. */
public abstract class RangerAuthorizationPlugin implements AuthorizationPlugin {
  protected String catalogProvider;
  protected RangerClientExt rangerClient;
  protected String rangerServiceName;
  /** Mapping Gravitino privilege name to the underlying authorization system privileges. */
  protected Map<Privilege.Name, Set<String>> mapPrivileges = null;

  /**
   * Because UserGroupAuthorizationPlugin::onOwnerSet() interface function didn't have param Role,
   * So we need predefine a role name
   */
  public static final String OWNER_ROLE_NAME = "OWNER";

  public static final String MANAGED_BY_GRAVITINO = "MANAGED_BY_GRAVITINO";
  public static final String POLICY_ITEM_OWNER_USER = "{OWNER}";

  public RangerAuthorizationPlugin() {
    initMapPrivileges();
  }

  /**
   * Different underlying permission system may have different privilege names, this function is
   * used to initialize the privilege mapping.
   */
  protected abstract void initMapPrivileges();

  /**
   * Translate the privilege name to the corresponding privilege name in the underlying permission
   */
  public Set<String> translatePrivilege(Privilege.Name name) {
    return mapPrivileges.get(name);
  }

  /** Whether this privilege is underlying permission system supported */
  protected boolean checkPrivilege(Privilege.Name name) {
    return mapPrivileges.containsKey(name);
  }

  @FormatMethod
  protected static void check(boolean condition, @FormatString String message, Object... args) {
    if (!condition) {
      throw new AuthorizationHookException(message, args);
    }
  }

  @VisibleForTesting
  public String formatPolicyName(String roleName, String securableObjectFullName) {
    return roleName + "-" + securableObjectFullName;
  }

  /** Find the managed policy for the securable object. */
  @VisibleForTesting
  public RangerPolicy findManagedPolicy(MetadataObject metadataObject)
      throws AuthorizationHookException {
    List<String> metaObjNamespaces =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    metaObjNamespaces.remove(0); // skip `catalog`
    List<String> filterKeys =
        Lists.newArrayList(
            RangerDefines.SEARCH_FILTER_DATABASE,
            RangerDefines.SEARCH_FILTER_TABLE,
            RangerDefines.SEARCH_FILTER_COLUMN);
    List<String> preciseFilterKeys =
        Lists.newArrayList(
            RangerDefines.RESOURCE_DATABASE,
            RangerDefines.RESOURCE_TABLE,
            RangerDefines.RESOURCE_COLUMN);
    Map<String, String> policyFilter = new HashMap<>();
    Map<String, String> preciseFilterKeysFilter = new HashMap<>();
    policyFilter.put(RangerDefines.SEARCH_FILTER_SERVICE_NAME, this.rangerServiceName);
    policyFilter.put(SearchFilter.POLICY_LABELS_PARTIAL, MANAGED_BY_GRAVITINO);
    for (int i = 0, j = 0; i < metaObjNamespaces.size() && j < filterKeys.size(); i++, j++) {
      policyFilter.put(filterKeys.get(j), metaObjNamespaces.get(i));
      preciseFilterKeysFilter.put(preciseFilterKeys.get(j), metaObjNamespaces.get(i));
    }

    try {
      List<RangerPolicy> policies = rangerClient.findPolicies(policyFilter);

      if (!policies.isEmpty()) {
        // Because Ranger doesn't support the precise filter, Ranger will return the policy meets
        // the wildcard(*,?) conditions, just like `*.*.*` policy will match `db1.table1.column1`
        // So we need to manual precise filter the policies.
        policies =
            policies.stream()
                .filter(
                    policy ->
                        policy.getResources().entrySet().stream()
                            .allMatch(
                                entry ->
                                    preciseFilterKeysFilter.containsKey(entry.getKey())
                                        && entry.getValue().getValues().size() == 1
                                        && entry
                                            .getValue()
                                            .getValues()
                                            .contains(preciseFilterKeysFilter.get(entry.getKey()))))
                .collect(Collectors.toList());
      }

      // Only return the policies that are delegate Gravitino management
      if (policies.size() > 1) {
        throw new AuthorizationHookException(
            "Each metadata object only have one Gravitino management enable policies.");
      }

      RangerPolicy policy = policies.size() == 1 ? policies.get(0) : null;
      // Didn't contain duplicate privilege in the delegate Gravitino management policy
      if (policy != null) {
        policy.getPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getDenyPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getRowFilterPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getDataMaskPolicyItems().forEach(this::checkPolicyItemAccess);
      }

      return policy;
    } catch (RangerServiceException e) {
      throw new AuthorizationHookException(e);
    }
  }

  /** For easy manage, each privilege will create a RangerPolicyItemAccess in the policy. */
  void checkPolicyItemAccess(RangerPolicy.RangerPolicyItem policyItem)
      throws AuthorizationHookException {
    if (policyItem.getAccesses().size() != 1) {
      throw new AuthorizationHookException(
          "The access type only have one in the delegate Gravitino management policy");
    }
    Map<String, Boolean> mapAccesses = new HashMap<>();
    policyItem
        .getAccesses()
        .forEach(
            access -> {
              if (mapAccesses.containsKey(access.getType()) && mapAccesses.get(access.getType())) {
                throw new AuthorizationHookException(
                    "Contain duplicate privilege in the delegate Gravitino management policy ");
              }
              mapAccesses.put(access.getType(), true);
            });
  }
}
