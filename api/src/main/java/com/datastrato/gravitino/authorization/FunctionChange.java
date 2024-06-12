/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache Spark's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/RoleChange.java

package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Evolving;

/**
 * The RoleChange interface defines the public API for managing roles in an authorization.
 */
@Evolving
public interface FunctionChange {
  /**
   * Create a RoleChange for renaming a table.
   *
   * @param newName The new table name.
   * @return A RoleChange for the rename.
   */
  static FunctionChange rename(String newName) {
    return new RenameFunction(newName);
  }

  /** A RoleChange to rename a table. */
  final class RenameFunction implements FunctionChange {
    private final String newName;

    private RenameFunction(String newName) {
      this.newName = newName;
    }

    /**
     * Retrieves the new name for the table.
     *
     * @return The new name of the table.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compares this RenameTable instance with another object for equality. The comparison is based
     * on the new name of the table.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same table renaming; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameFunction that = (RenameFunction) o;
      return newName.equals(that.newName);
    }

    /**
     * Generates a hash code for this RenameTable instance. The hash code is based on the new name
     * of the table.
     *
     * @return A hash code value for this table renaming operation.
     */
    @Override
    public int hashCode() {
      return newName.hashCode();
    }

    /**
     * Returns a string representation of the RenameTable instance. This string format includes the
     * class name followed by the property name to be renamed.
     *
     * @return A string summary of the property rename instance.
     */
    @Override
    public String toString() {
      return "RENAMEROLE " + newName;
    }
  }

  /** A RoleChange to update a table's comment. */
  final class UpdateParamater implements FunctionChange {
    private final Function.Parameter parameter;

    private UpdateParamater(Function.Parameter parameter) {
      this.parameter = parameter;
    }

    private Function.Parameter getParamater() {
      return this.parameter;
    }

    /**
     * Compares this UpdateComment instance with another object for equality. The comparison is
     * based on the new comment of the table.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same table comment update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateParamater that = (UpdateParamater) o;
      return parameter.equals(that.parameter);
    }

    /**
     * Generates a hash code for this UpdateComment instance. The hash code is based on the new
     * comment of the table.
     *
     * @return A hash code value for this table comment update operation.
     */
    @Override
    public int hashCode() {
      return parameter.hashCode();
    }

    /**
     * Returns a string representation of the UpdateComment instance. This string format includes
     * the class name followed by the property name to be updated.
     *
     * @return A string representation of the UpdateComment instance.
     */
    @Override
    public String toString() {
      return "UPDATEPARAMATER " + parameter;
    }
  }
}
