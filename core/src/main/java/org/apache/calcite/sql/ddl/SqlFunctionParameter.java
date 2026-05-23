/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a parameter in a SQL function definition.
 *
 * <p>Encapsulates parameter metadata: name, type, mode (IN/OUT/INOUT),
 * and optional default value expression. This is a data holder class used in
 * the context of SQL UDF definitions.
 */
public class SqlFunctionParameter {
  private final @Nullable String paramName;
  private final SqlDataTypeSpec paramType;
  private final ParameterMode mode;
  private final @Nullable SqlNode defaultValue;

  /**
   * Enumeration of parameter modes (similar to PostgreSQL).
   */
  public enum ParameterMode {
    IN,      // Input parameter (default)
    OUT,     // Output parameter
    INOUT,   // Both input and output
    VARIADIC // Variable-length input parameter
  }

  /**
   * Creates a SqlFunctionParameter.
   *
   * @param paramName        Parameter name (can be null for unnamed parameters)
   * @param paramType        Parameter data type (required)
   * @param mode             Parameter mode (IN, OUT, INOUT, VARIADIC)
   * @param defaultValue     Default value expression (can be null)
   */
  public SqlFunctionParameter(
      @Nullable String paramName,
      SqlDataTypeSpec paramType,
      ParameterMode mode,
      @Nullable SqlNode defaultValue) {
    this.paramName = paramName;
    this.paramType = requireNonNull(paramType, "paramType");
    this.mode = requireNonNull(mode, "mode");
    this.defaultValue = defaultValue;
  }

  /**
   * Creates a SqlFunctionParameter with IN mode and no default value.
   */
  public SqlFunctionParameter(
      @Nullable String paramName,
      SqlDataTypeSpec paramType) {
    this(paramName, paramType, ParameterMode.IN, null);
  }

  /**
   * Returns the parameter name (null if unnamed).
   */
  public @Nullable String getParamName() {
    return paramName;
  }

  /**
   * Returns the parameter data type.
   */
  public SqlDataTypeSpec getParamType() {
    return paramType;
  }

  /**
   * Returns the parameter mode (IN, OUT, INOUT, VARIADIC).
   */
  public ParameterMode getMode() {
    return mode;
  }

  /**
   * Returns the default value expression (null if no default).
   */
  public @Nullable SqlNode getDefaultValue() {
    return defaultValue;
  }

  /**
   * Returns whether this parameter has a default value.
   */
  public boolean hasDefaultValue() {
    return defaultValue != null;
  }

  /**
   * Generates the SQL representation of this parameter.
   */
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    // Write parameter mode always (even for IN, for explicit output)
    writer.keyword(mode.name());

    // Write parameter name if present
    if (paramName != null) {
      writer.identifier(paramName, true);
    }

    // Write parameter type
    paramType.unparse(writer, leftPrec, rightPrec);

    // Write default value if present
    if (defaultValue != null) {
      writer.keyword("DEFAULT");
      defaultValue.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SqlFunctionParameter)) {
      return false;
    }
    SqlFunctionParameter that = (SqlFunctionParameter) o;
    return Objects.equals(paramName, that.paramName)
        && paramType.equals(that.paramType)
        && mode == that.mode
        && Objects.equals(defaultValue, that.defaultValue);
  }

  @Override public int hashCode() {
    return Objects.hash(paramName, paramType, mode, defaultValue);
  }

  @Override public String toString() {
    final StringBuilder sb = new StringBuilder();
    if (mode != ParameterMode.IN) {
      sb.append(mode).append(" ");
    }
    if (paramName != null) {
      sb.append(paramName).append(" ");
    }
    sb.append(paramType);
    if (defaultValue != null) {
      sb.append(" DEFAULT ").append(defaultValue);
    }
    return sb.toString();
  }
}
