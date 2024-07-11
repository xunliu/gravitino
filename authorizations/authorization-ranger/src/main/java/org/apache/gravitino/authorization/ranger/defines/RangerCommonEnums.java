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
package org.apache.gravitino.authorization.ranger.defines;

// apache/ranger/security-admin/src/main/java/org/apache/ranger/defines/RangerCommonEnums.java
public class RangerCommonEnums {
  /** IS_VISIBLE is an element of enum VisibilityStatus. Its value is "IS_VISIBLE". */
  public static final int IS_VISIBLE = 1;
  /** IS_HIDDEN is an element of enum VisibilityStatus. Its value is "IS_HIDDEN". */
  public static final int IS_HIDDEN = 0;

  /** STATUS_DISABLED is an element of enum ActiveStatus. Its value is "STATUS_DISABLED". */
  public static final int STATUS_DISABLED = 0;
  /** STATUS_ENABLED is an element of enum ActiveStatus. Its value is "STATUS_ENABLED". */
  public static final int STATUS_ENABLED = 1;
  /** STATUS_DELETED is an element of enum ActiveStatus. Its value is "STATUS_DELETED". */
  public static final int STATUS_DELETED = 2;

  /** Max value for enum ActiveStatus_MAX */
  public static final int ActiveStatus_MAX = 2;

  /** USER_APP is an element of enum UserSource. Its value is "USER_APP". */
  public static final int USER_APP = 0;

  public static final int USER_EXTERNAL = 1;
  public static final int USER_AD = 2;
  public static final int USER_LDAP = 3;
  public static final int USER_UNIX = 4;
  public static final int USER_REPO = 5;

  public static final int GROUP_INTERNAL = 0;
  public static final int GROUP_EXTERNAL = 1;
  public static final int GROUP_AD = 2;
  public static final int GROUP_LDAP = 3;
  public static final int GROUP_UNIX = 4;
  public static final int GROUP_REPO = 5;
}
