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
package org.apache.calcite.sql.babel.postgres;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlBasicOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Parse tree node representing a {@code SHOW} clause.
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-show.html">SHOW specification</a>
 */
public class SqlShow extends SqlCall {
  public static final SqlBasicOperator OPERATOR =
      SqlBasicOperator.create("SHOW").withCallFactory(
          (operator, functionQualifier, pos, operands) ->
              new SqlShow(pos, (SqlIdentifier) operands[0]));
  private final SqlIdentifier name;

  protected SqlShow(final SqlParserPos pos, SqlIdentifier name) {
    super(pos);
    this.name = name;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("SHOW");
    writer.identifier(name.getSimple(), false);
  }

  public SqlIdentifier getName() {
    return name;
  }
}
