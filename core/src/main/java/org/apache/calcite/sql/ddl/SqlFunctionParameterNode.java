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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper to represent a SQL function parameter as a SqlNode for AST compatibility.
 *
 * <p>SqlFunctionParameter is a plain Java class that cannot be stored directly in SqlNodeList.
 * This wrapper allows it to be used in the AST while maintaining the original parameter information.
 */
public class SqlFunctionParameterNode extends SqlCall {

  private final SqlFunctionParameter parameter;

  /**
   * Creates a wrapper node for a function parameter.
   */
  public SqlFunctionParameterNode(
      SqlFunctionParameter parameter,
      SqlParserPos pos) {
    super(pos);
    this.parameter = parameter;
  }

  /**
   * Returns the wrapped parameter object.
   */
  public SqlFunctionParameter getParameter() {
    return parameter;
  }

  @Override public SqlKind getKind() {
    return SqlKind.OTHER;
  }

  @Override public SqlOperator getOperator() {
    throw new UnsupportedOperationException("SqlFunctionParameterNode has no operator");
  }

  @Override public List<SqlNode> getOperandList() {
    return new ArrayList<>();
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    parameter.unparse(writer, leftPrec, rightPrec);
  }
}
