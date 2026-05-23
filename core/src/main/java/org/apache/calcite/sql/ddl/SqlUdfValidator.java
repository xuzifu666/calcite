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

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.Set;

/**
 * Validator for SQL UDF (User Defined Function) definitions.
 *
 * <p>Performs semantic validation on SQL UDF parameters and function bodies
 * to ensure correctness before execution.
 */
public class SqlUdfValidator {

  private SqlUdfValidator() {
  }

  /**
   * Validates a SQL UDF definition.
   *
   * @param parameterList Parameter list (nullable)
   * @param returnType Return type specification (required for SQL UDF)
   * @param functionBody Function body expression (nullable)
   * @throws IllegalArgumentException if validation fails
   */
  public static void validate(
      @Nullable SqlNodeList parameterList,
      @Nullable SqlDataTypeSpec returnType,
      @Nullable SqlNode functionBody) {
    validateParameters(parameterList);
    validateReturnType(returnType);
    validateFunctionBody(functionBody);
  }

  /**
   * Validates the parameter list.
   *
   * <p>Checks:
   * - Parameter names are unique
   * - Parameter types are valid
   * - Default value positions are valid (all defaults must come after non-default params)
   */
  private static void validateParameters(@Nullable SqlNodeList parameterList) {
    if (parameterList == null || parameterList.isEmpty()) {
      return;
    }

    final Set<String> seenNames = new HashSet<>();
    boolean seenDefaultValue = false;
    int index = 0;

    for (Object obj : parameterList) {
      final SqlFunctionParameter param;
      if (obj instanceof SqlFunctionParameterNode) {
        param = ((SqlFunctionParameterNode) obj).getParameter();
      } else if (obj instanceof SqlFunctionParameter) {
        param = (SqlFunctionParameter) obj;
      } else {
        throw new IllegalArgumentException(
            "Invalid parameter at position " + index + ": expected SqlFunctionParameter");
      }

      // Currently only IN mode is supported
      if (param.getMode() != SqlFunctionParameter.ParameterMode.IN) {
        throw new IllegalArgumentException(
            "Parameter at position " + index + " uses unsupported mode '"
                + param.getMode() + "'. Currently only IN parameters are supported. "
                + "OUT and INOUT parameters will be implemented in a future version.");
      }

      // Check parameter name uniqueness
      if (param.getParamName() != null) {
        final String name = param.getParamName().toUpperCase();
        if (seenNames.contains(name)) {
          throw new IllegalArgumentException(
              "Duplicate parameter name: '" + param.getParamName() + "'");
        }
        seenNames.add(name);
      }

      // Check default value positions
      if (param.hasDefaultValue()) {
        seenDefaultValue = true;
      } else if (seenDefaultValue) {
        throw new IllegalArgumentException(
            "Parameter at position " + index
                + " cannot have no default value after a parameter with default value");
      }

      // Check parameter type
      if (param.getParamType() == null) {
        throw new IllegalArgumentException(
            "Parameter at position " + index + " has no type specified");
      }

      index++;
    }
  }

  /**
   * Validates the return type specification.
   *
   * @param returnType Return type (required for SQL UDF)
   * @throws IllegalArgumentException if validation fails
   */
  private static void validateReturnType(@Nullable SqlDataTypeSpec returnType) {
    if (returnType == null) {
      throw new IllegalArgumentException("SQL UDF must specify a return type with RETURNS clause");
    }

    // Basic validation - check that the type specification is not null
    if (returnType.getTypeNameSpec() == null) {
      throw new IllegalArgumentException("Invalid return type specification");
    }
  }

  /**
   * Validates the function body.
   *
   * @param functionBody Function body expression (nullable, can be null for now)
   * @throws IllegalArgumentException if validation fails
   */
  private static void validateFunctionBody(@Nullable SqlNode functionBody) {
    // Basic validation - in a full implementation, this would:
    // - Check that all referenced columns/parameters are available
    // - Verify type compatibility between function body and return type
    // - Check for illegal operations (e.g., DML in UDF)
    // For now, we just check that a body is provided if needed
    if (functionBody == null) {
      throw new IllegalArgumentException("SQL UDF must have a function body (RETURN statement)");
    }
  }

  /**
   * Validates parameter type compatibility.
   *
   * @param paramType Parameter type
   * @param valueType Value type to assign to parameter
   * @return true if types are compatible
   */
  public static boolean isTypeCompatible(SqlTypeName paramType, SqlTypeName valueType) {
    // Simple compatibility check - numeric types are compatible with each other
    if (paramType == valueType) {
      return true;
    }

    // Allow numeric compatibility
    if (isNumericType(paramType) && isNumericType(valueType)) {
      return true;
    }

    // Allow string compatibility
    if (isStringType(paramType) && isStringType(valueType)) {
      return true;
    }

    return false;
  }

  /**
   * Checks if a type is numeric.
   */
  private static boolean isNumericType(SqlTypeName type) {
    return SqlTypeName.NUMERIC_TYPES.contains(type);
  }

  /**
   * Checks if a type is string-like.
   */
  private static boolean isStringType(SqlTypeName type) {
    return SqlTypeName.STRING_TYPES.contains(type);
  }
}
