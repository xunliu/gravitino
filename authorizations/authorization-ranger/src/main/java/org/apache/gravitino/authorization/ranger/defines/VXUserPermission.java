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

import javax.xml.bind.annotation.XmlRootElement;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

// apache/ranger/security-admin/src/main/java/org/apache/ranger/view/VXUserPermission.java
@JsonAutoDetect(
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    setterVisibility = JsonAutoDetect.Visibility.NONE,
    fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class VXUserPermission extends VXDataObject implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  protected Long userId;
  protected Long moduleId;
  protected Integer isAllowed;
  protected String userName;
  protected String moduleName;
  protected String loginId;

  public VXUserPermission() {
    // TODO Auto-generated constructor stub
  }

  /** @return the id */
  public Long getId() {
    return id;
  }

  /** @param id the id to set */
  public void setId(Long id) {
    this.id = id;
  }

  @Override
  public String toString() {

    String str = "VXUserPermission={";
    str += super.toString();
    str += "id={" + id + "} ";
    str += "userId={" + userId + "} ";
    str += "moduleId={" + moduleId + "} ";
    str += "isAllowed={" + isAllowed + "} ";
    str += "moduleName={" + moduleName + "} ";
    str += "loginId={" + loginId + "} ";
    str += "}";

    return str;
  }
}
