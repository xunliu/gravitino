/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization.ranger;

import com.datastrato.gravitino.authorization.AuthorizationHook;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.exceptions.AuthorizationHookException;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ranger authorization operations hooks interfaces. */
public abstract class RangerAuthorizationHook implements AuthorizationHook {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationHook.class);
  protected String catalogProvider;
  protected RangerClient rangerClient;
  protected String rangerServiceName;
  /** Mapping Gravitino privilege name to the underlying authorization system. */
  protected Map<Privilege.Name, String> mapPrivileges = null;

  public static final String MANAGED_BY_GRAVITINO = "MANAGED_BY_GRAVITINO";
  public static final String POLICY_ITEM_OWNER_USER = "{OWNER}";

  public RangerAuthorizationHook() {
    initPrivileges();
  }

  /**
   * Different underlying permission system may have different privilege names, this function is
   * used to initialize the privilege mapping.
   */
  protected abstract void initPrivileges();

  /**
   * Translate the privilege name to the corresponding privilege name in the underlying permission
   */
  public String translatePrivilege(Privilege.Name name) {
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

  protected RangerPolicy findManagedPolicy(SecurableObject securableObject, boolean samePrivilege)
      throws AuthorizationHookException {
    List<String> objects = SecurableObjects.DOT_SPLITTER.splitToList(securableObject.fullName());
    List<String> filterKeys =
        Lists.newArrayList(
            RangerRef.SEARCH_FILTER_DATABASE,
            RangerRef.SEARCH_FILTER_TABLE,
            RangerRef.SEARCH_FILTER_COLUMN);
    List<String> preciseFilterKeys =
        Lists.newArrayList(
            RangerRef.RESOURCE_DATABASE, RangerRef.RESOURCE_TABLE, RangerRef.RESOURCE_COLUMN);
    Map<String, String> policyFilter = new HashMap<>();
    Map<String, String> preciseFilterKeysFilter = new HashMap<>();
    policyFilter.put(RangerRef.SEARCH_FILTER_SERVICE_NAME, this.rangerServiceName);
    policyFilter.put(SearchFilter.POLICY_LABELS_PARTIAL, MANAGED_BY_GRAVITINO);
    for (int i = 1, j = 0; i < objects.size() && j < filterKeys.size(); i++, j++) {
      // skip `catalog`
      policyFilter.put(filterKeys.get(j), objects.get(i));
      preciseFilterKeysFilter.put(preciseFilterKeys.get(j), objects.get(i));
    }

    try {
      List<RangerPolicy> policies = rangerClient.findPolicies(policyFilter);

      if (!policies.isEmpty()) {
        // Because Ranger doesn't support the precise filter, Ranger will return the policy meets
        // the wildcard(*,?) conditions, just like `*.*.*` policy will match `db1.table1.column1`
        // So we need to manual precise filter the policies
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

      if (samePrivilege && policies.size() == 1) {
        RangerPolicy policy = policies.get(0);
        Set<String> policyPrivileges =
            policy.getPolicyItems().stream()
                .flatMap(policyItem -> policyItem.getAccesses().stream())
                .map(RangerPolicy.RangerPolicyItemAccess::getType)
                .collect(Collectors.toSet());
        Set<String> newPrivileges =
            securableObject.privileges().stream()
                .map(privilege -> translatePrivilege(privilege.name()))
                .collect(Collectors.toSet());
        if (!policyPrivileges.containsAll(newPrivileges)) {
          LOG.info(
              "The privilege of the securable object {} is different from the delegate Gravitino management policy{} ",
              newPrivileges,
              policyPrivileges);
          return null;
        }
      }

      // Didn't contain duplicate privilege in the delegate Gravitino management policy
      policies.forEach(
          policy -> {
            policy.getPolicyItems().forEach(this::checkPolicyItemAccess);
            policy.getDenyPolicyItems().forEach(this::checkPolicyItemAccess);
            policy.getRowFilterPolicyItems().forEach(this::checkPolicyItemAccess);
            policy.getDataMaskPolicyItems().forEach(this::checkPolicyItemAccess);
          });

      return policies.size() == 1 ? policies.get(0) : null;
    } catch (RangerServiceException e) {
      throw new AuthorizationHookException(e);
    }
  }

  // For easy manage, each privilege will create a RangerPolicyItemAccess in the policy.
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
