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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

// apache/ranger/security-admin/src/main/java/org/apache/ranger/view/VXPortalUser.java
@JsonAutoDetect(
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    setterVisibility = JsonAutoDetect.Visibility.NONE,
    fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class VXPortalUser extends VXDataObject implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  /** Login Id for the user */
  protected String loginId;
  /** Password */
  protected String password;
  /** Status This attribute is of type enum CommonEnums::ActivationStatus */
  protected int status;
  /** Email address of the user */
  protected String emailAddress;
  /** First name of the user */
  protected String firstName;
  /** Last name of the user */
  protected String lastName;
  /** Public name of the user */
  protected String publicScreenName;
  /** Source of the user This attribute is of type enum CommonEnums::UserSource */
  protected int userSource;
  /** Notes for the user */
  protected String notes;
  /** List of roles for this user */
  protected Collection<String> userRoleList;

  protected Collection<Long> groupIdList;
  protected List<VXUserPermission> userPermList;
  protected List<VXGroupPermission> groupPermissions;

  /** Additional store attributes. */
  protected String otherAttributes;

  /** sync Source Attribute. */
  protected String syncSource;

  /** Configuration properties. */
  protected Map<String, String> configProperties;

  /** Default constructor. This will set all the attributes to default value. */
  public VXPortalUser() {
    status = RangerCommonEnums.STATUS_ENABLED;
    userSource = RangerCommonEnums.USER_EXTERNAL;
  }

  /**
   * This return the bean content in string format
   *
   * @return formatedStr
   */
  public String toString() {
    String str = "VXPortalUser={";
    str += super.toString();
    str += "loginId={" + loginId + "} ";
    str += "status={" + status + "} ";
    str += "emailAddress={" + emailAddress + "} ";
    str += "firstName={" + firstName + "} ";
    str += "lastName={" + lastName + "} ";
    str += "publicScreenName={" + publicScreenName + "} ";
    str += "userSource={" + userSource + "} ";
    str += "notes={" + notes + "} ";
    str += "userRoleList={" + userRoleList + "} ";
    str += "otherAttributes={" + otherAttributes + "} ";
    str += "syncSource={" + syncSource + "} ";
    str += "}";
    return str;
  }
}
