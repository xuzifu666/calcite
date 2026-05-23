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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree for {@code CREATE FUNCTION} statement.
 *
 * <p>Supports both Java UDF (with className and USING) and SQL UDF
 * (with parameters, returnType, and functionBody).
 */
public class SqlCreateFunction extends SqlCreate {
  private final SqlIdentifier name;
  private final @Nullable SqlNode className;
  private final @Nullable SqlNodeList usingList;

  // SQL UDF fields (optional)
  private final @Nullable SqlNodeList parameterList;
  private final @Nullable SqlDataTypeSpec returnType;
  private final @Nullable SqlNode functionBody;
  private final FunctionType functionType;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION) {
        @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          return new SqlCreateFunction(pos,
              ((SqlLiteral) requireNonNull(operands[0], "replace")).booleanValue(),
              ((SqlLiteral) requireNonNull(operands[1], "ifNotExists")).booleanValue(),
              (SqlIdentifier) requireNonNull(operands[2], "name"),
              requireNonNull(operands[3], "className"),
              (SqlNodeList) requireNonNull(operands[4], "usingList"));
        }
      };

  /**
   * Creates a SqlCreateFunction for Java UDF.
   * (existing constructor for backward compatibility)
   */
  public SqlCreateFunction(SqlParserPos pos, boolean replace,
      boolean ifNotExists, SqlIdentifier name,
      SqlNode className, SqlNodeList usingList) {
    this(pos, replace, ifNotExists, name, className, usingList,
        null, null, null, FunctionType.JAVA_UDF);
  }

  /**
   * Creates a SqlCreateFunction for SQL UDF or Java UDF.
   *
   * @param pos             Parse position
   * @param replace         True if CREATE OR REPLACE
   * @param ifNotExists     True if IF NOT EXISTS
   * @param name            Function name
   * @param className       Java class name (for Java UDF, null for SQL UDF)
   * @param usingList       USING resources list (for Java UDF, null for SQL UDF)
   * @param parameterList   Parameter list (for SQL UDF, null for Java UDF)
   * @param returnType      Return type specification (for SQL UDF)
   * @param functionBody    Function body expression/statement (for SQL UDF)
   * @param functionType    Function type (JAVA_UDF or SQL_UDF_SCALAR)
   */
  public SqlCreateFunction(
      SqlParserPos pos,
      boolean replace,
      boolean ifNotExists,
      SqlIdentifier name,
      @Nullable SqlNode className,
      @Nullable SqlNodeList usingList,
      @Nullable SqlNodeList parameterList,
      @Nullable SqlDataTypeSpec returnType,
      @Nullable SqlNode functionBody,
      FunctionType functionType) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = requireNonNull(name, "name");
    this.className = className;
    this.usingList = usingList;
    this.parameterList = parameterList;
    this.returnType = returnType;
    this.functionBody = functionBody;
    this.functionType = requireNonNull(functionType, "functionType");

    // Validate constraints
    if (functionType == FunctionType.JAVA_UDF) {
      checkArgument(className != null, "Java UDF requires className");
      checkArgument(usingList != null, "Java UDF requires usingList");
      if (usingList.size() > 0) {
        checkArgument(usingList.size() % 2 == 0, "usingList must have even size");
      }
    } else if (functionType == FunctionType.SQL_UDF_SCALAR
        || functionType == FunctionType.SQL_UDF_TABLE) {
      checkArgument(returnType != null, "SQL UDF requires returnType");
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec,
      int rightPrec) {
    writer.keyword(getReplace() ? "CREATE OR REPLACE" : "CREATE");
    writer.keyword("FUNCTION");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, 0, 0);

    if (functionType == FunctionType.JAVA_UDF) {
      // Java UDF format
      writer.keyword("AS");
      className.unparse(writer, 0, 0);
      if (usingList != null && !usingList.isEmpty()) {
        writer.keyword("USING");
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
        for (Pair<SqlLiteral, SqlLiteral> using : pairs()) {
          writer.sep(",");
          using.left.unparse(writer, 0, 0); // FILE, URL or ARCHIVE
          using.right.unparse(writer, 0, 0); // e.g. 'file:foo/bar.jar'
        }
        writer.endList(frame);
      }
    } else {
      // SQL UDF format
      if (parameterList != null) {
        parameterList.unparse(writer, 0, 0);
      } else {
        writer.print("()");
      }

      if (returnType != null) {
        writer.keyword("RETURNS");
        returnType.unparse(writer, 0, 0);
      }

      if (functionBody != null) {
        writer.keyword("RETURN");
        functionBody.unparse(writer, 0, 0);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private List<Pair<SqlLiteral, SqlLiteral>> pairs() {
    return Util.pairs((List) usingList);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(
        SqlLiteral.createBoolean(getReplace(), SqlParserPos.ZERO),
        SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
        name, className != null ? className : SqlLiteral.createNull(SqlParserPos.ZERO),
        usingList != null ? usingList : SqlNodeList.EMPTY);
  }

  // Getters for the new fields
  public SqlIdentifier getName() {
    return name;
  }

  public @Nullable SqlNode getClassName() {
    return className;
  }

  public @Nullable SqlNodeList getUsingList() {
    return usingList;
  }

  public @Nullable SqlNodeList getParameterList() {
    return parameterList;
  }

  public @Nullable SqlDataTypeSpec getReturnType() {
    return returnType;
  }

  public @Nullable SqlNode getFunctionBody() {
    return functionBody;
  }

  public FunctionType getFunctionType() {
    return functionType;
  }

  public boolean isSqlUdf() {
    return functionType == FunctionType.SQL_UDF_SCALAR
        || functionType == FunctionType.SQL_UDF_TABLE;
  }

  public boolean isJavaUdf() {
    return functionType == FunctionType.JAVA_UDF;
  }
}
