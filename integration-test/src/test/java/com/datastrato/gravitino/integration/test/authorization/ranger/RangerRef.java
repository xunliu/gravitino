/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.authorization.ranger;

import org.apache.ranger.plugin.util.SearchFilter;

public class RangerRef {
    // In the Ranger 2.4.0
    // security-admin/src/main/java/org/apache/ranger/service/RangerServiceDefService.java:L43
    public static final String IMPLICIT_CONDITION_EXPRESSION_NAME = "_expression";

    // In the Ranger 2.4.0
    // security-admin/src/main/java/org/apache/ranger/common/RangerSearchUtil.java:L159
    public static final String SEARCH_FILTER_SERVICE_NAME = SearchFilter.SERVICE_NAME;
    public static final String RESOURCE_DATABASE = "database";
    public static final String RESOURCE_TABLE = "table";
    public static final String RESOURCE_COLUMN = "column";
    public static final String SEARCH_FILTER_DATABASE =
            SearchFilter.RESOURCE_PREFIX + RESOURCE_DATABASE;
    public static final String SEARCH_FILTER_TABLE = SearchFilter.RESOURCE_PREFIX + RESOURCE_TABLE;
    public static final String SEARCH_FILTER_COLUMN = SearchFilter.RESOURCE_PREFIX + RESOURCE_COLUMN;
}
