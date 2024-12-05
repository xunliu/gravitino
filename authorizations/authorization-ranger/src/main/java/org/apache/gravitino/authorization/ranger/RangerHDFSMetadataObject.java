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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.connector.authorization.AuthorizationMetadataObject;

import javax.annotation.Nullable;
import java.util.List;

/** The helper class for {@link AuthorizationMetadataObject}. */
public class RangerHDFSMetadataObject implements AuthorizationMetadataObject {
  /**
   * The type of object in the Ranger system. Every type will map one kind of the entity of the
   * Gravitino type system.
   */
  public enum Type implements AuthorizationMetadataObject.Type {
    PATH(MetadataObject.Type.FILESET);

    private final MetadataObject.Type metadataType;

    Type(MetadataObject.Type type) {
      this.metadataType = type;
    }

    public MetadataObject.Type metadataObjectType() {
      return metadataType;
    }

    public static Type fromMetadataType(MetadataObject.Type metadataType) {
      for (Type type : Type.values()) {
        if (type.metadataObjectType() == metadataType) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          "No matching RangerMetadataObject.Type for " + metadataType);
    }
  }

  /** The implementation of the {@link MetadataObject}. */
  private final String name;
  private final String parent;
  private final AuthorizationMetadataObject.Type type;

  /**
   * Create the metadata object with the given name, parent and type.
   *
   * @param name The location of the metadata object
   * @param type The type of the metadata object
   */
  public RangerHDFSMetadataObject(String parent, String name, AuthorizationMetadataObject.Type type) {
    this.parent = parent;
    this.name = name;
    this.type = type;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public List<String> names() {
    return DOT_SPLITTER.splitToList(fullName());
  }

  @Override
  public String parent() {
    return parent;
  }

  @Override
  public AuthorizationMetadataObject.Type type() {
    return type;
  }

  @Override
  public void validateAuthorizationMetadataObject() throws IllegalArgumentException {
    Preconditions.checkArgument(
            name != null && !name.isEmpty(), "Cannot create a Ranger metadata object with no names");
    Preconditions.checkArgument(
        type != null, "Cannot create a Ranger metadata object with no type");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof RangerHDFSMetadataObject)) {
      return false;
    }

    RangerHDFSMetadataObject that = (RangerHDFSMetadataObject) o;
    return java.util.Objects.equals(name, that.name)
        && type == that.type;
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return "MetadataObject: [name=" + name() + "], [type=" + type + "]";
  }
}
