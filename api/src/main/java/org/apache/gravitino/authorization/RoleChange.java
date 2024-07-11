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
package org.apache.gravitino.authorization;

import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/** The RoleChange interface defines the public API for managing roles in an authorization. */
@Evolving
public interface RoleChange {
  /**
   * Create a RoleChange to add a securable object into a role.
   *
   * @param securableObject The securable object.
   * @return return a RoleChange for the add securable object.
   */
  static RoleChange addSecurableObject(SecurableObject securableObject) {
    return new AddSecurableObject(securableObject);
  }

  /**
   * Create a RoleChange to remove a securable object from a role.
   *
   * @param securableObject The securable object.
   * @return return a RoleChange for the add securable object.
   */
  static RoleChange removeSecurableObject(SecurableObject securableObject) {
    return new RemoveSecurableObject(securableObject);
  }

  /**
   * Update a securable object RoleChange.
   *
   * @param securableObject The securable object.
   * @param newSecurableObject The new securable object.
   * @return return a RoleChange for the update securable object.
   */
  static RoleChange updateSecurableObject(
      SecurableObject securableObject, SecurableObject newSecurableObject) {
    return new UpdateSecurableObject(securableObject, newSecurableObject);
  }

  /** A AddSecurableObject to add securable object to role. */
  final class AddSecurableObject implements RoleChange {
    private final SecurableObject securableObject;

    private AddSecurableObject(SecurableObject securableObject) {
      this.securableObject = securableObject;
    }

    /**
     * Returns the securable object to be added.
     *
     * @return return a securable object.
     */
    public SecurableObject getSecurableObject() {
      return this.securableObject;
    }

    /**
     * Compares this AddSecurableObject instance with another object for equality. The comparison is
     * based on the add securable object to role.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same add securable object; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddSecurableObject that = (AddSecurableObject) o;
      return securableObject.equals(that.securableObject);
    }

    /**
     * Generates a hash code for this AddSecurableObject instance. The hash code is based on the add
     * securable object.
     *
     * @return A hash code value for this add securable object operation.
     */
    @Override
    public int hashCode() {
      return securableObject.hashCode();
    }

    /**
     * Returns a string representation of the AddSecurableObject instance. This string format
     * includes the class name followed by the add securable object operation.
     *
     * @return A string representation of the AddSecurableObject instance.
     */
    @Override
    public String toString() {
      return "ADDSECURABLEOBJECT " + securableObject;
    }
  }

  /** A RemoveSecurableObject to remove securable object from role. */
  final class RemoveSecurableObject implements RoleChange {
    private final SecurableObject securableObject;

    private RemoveSecurableObject(SecurableObject securableObject) {
      this.securableObject = securableObject;
    }

    /**
     * Returns the securable object to be added.
     *
     * @return return a securable object.
     */
    public SecurableObject getSecurableObject() {
      return this.securableObject;
    }

    /**
     * Compares this RemoveSecurableObject instance with another object for equality. The comparison
     * is based on the add securable object to role.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same add securable object; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveSecurableObject that = (RemoveSecurableObject) o;
      return securableObject.equals(that.securableObject);
    }

    /**
     * Generates a hash code for this RemoveSecurableObject instance. The hash code is based on the
     * add securable object.
     *
     * @return A hash code value for this add securable object operation.
     */
    @Override
    public int hashCode() {
      return securableObject.hashCode();
    }

    /**
     * Returns a string representation of the RemoveSecurableObject instance. This string format
     * includes the class name followed by the add securable object operation.
     *
     * @return A string representation of the RemoveSecurableObject instance.
     */
    @Override
    public String toString() {
      return "REMOVESECURABLEOBJECT " + securableObject;
    }
  }

  /**
   * A UpdateSecurableObject to update securable object's privilege from role. <br>
   * The securable object's metadata entity must same as new securable object's metadata entity.
   * <br>
   * The securable object's privilege must be different as new securable object's privilege. <br>
   */
  final class UpdateSecurableObject implements RoleChange {
    private final SecurableObject securableObject;
    private final SecurableObject newSecurableObject;

    private UpdateSecurableObject(
        SecurableObject securableObject, SecurableObject newSecurableObject) {
      if (!securableObject.fullName().equals(newSecurableObject.fullName())) {
        throw new IllegalArgumentException(
            "The securable object's metadata entity must be same as new securable object's metadata entity.");
      }
      if (securableObject.privileges().containsAll(newSecurableObject.privileges())) {
        throw new IllegalArgumentException(
            "The securable object's privilege must be different as new securable object's privilege.");
      }

      this.securableObject = securableObject;
      this.newSecurableObject = newSecurableObject;
    }

    /**
     * Returns the securable object to be updated.
     *
     * @return return a securable object.
     */
    public SecurableObject getSecurableObject() {
      return this.securableObject;
    }

    /**
     * Returns the new securable object.
     *
     * @return return a securable object.
     */
    public SecurableObject getNewSecurableObject() {
      return this.newSecurableObject;
    }

    /**
     * Compares this UpdateSecurableObject instance with another object for equality. The comparison
     * is based on the old securable object and new securable object.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same add securable object; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateSecurableObject that = (UpdateSecurableObject) o;
      return securableObject.equals(that.securableObject)
          && newSecurableObject.equals(that.newSecurableObject);
    }

    /**
     * Generates a hash code for this UpdateSecurableObject instance. The hash code is based on the
     * old securable object and new securable object.
     *
     * @return A hash code value for this update securable object operation.
     */
    @Override
    public int hashCode() {
      int result = Objects.hash(securableObject);
      result = 31 * result + Objects.hashCode(newSecurableObject);
      return result;
    }

    /**
     * Returns a string representation of the UpdateSecurableObject instance. This string format
     * includes the class name followed by the add securable object operation.
     *
     * @return A string representation of the RemoveSecurableObject instance.
     */
    @Override
    public String toString() {
      return "UPDATESECURABLEOBJECT " + securableObject;
    }
  }
}
