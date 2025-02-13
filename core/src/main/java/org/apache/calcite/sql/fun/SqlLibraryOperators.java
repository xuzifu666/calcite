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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Optionality;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlLibrary.BIG_QUERY;
import static org.apache.calcite.sql.fun.SqlLibrary.HIVE;
import static org.apache.calcite.sql.fun.SqlLibrary.MYSQL;
import static org.apache.calcite.sql.fun.SqlLibrary.ORACLE;
import static org.apache.calcite.sql.fun.SqlLibrary.POSTGRESQL;
import static org.apache.calcite.sql.fun.SqlLibrary.SPARK;

/**
 * Defines functions and operators that are not part of standard SQL but
 * belong to one or more other dialects of SQL.
 *
 * <p>They are read by {@link SqlLibraryOperatorTableFactory} into instances
 * of {@link SqlOperatorTable} that contain functions and operators for
 * particular libraries.
 */
public abstract class SqlLibraryOperators {
  private SqlLibraryOperators() {
  }

  /** The "CONVERT_TIMEZONE(tz1, tz2, datetime)" function;
   * converts the timezone of {@code datetime} from {@code tz1} to {@code tz2}.
   * This function is only on Redshift, but we list it in PostgreSQL
   * because Redshift does not have its own library. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction CONVERT_TIMEZONE =
      new SqlFunction("CONVERT_TIMEZONE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE,
          null,
          OperandTypes.CHARACTER_CHARACTER_DATETIME,
          SqlFunctionCategory.TIMEDATE);

  /** THE "DATE_ADD(date, interval)" function
   * (BigQuery) adds the interval to the date. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_ADD =
      SqlBasicFunction.create(SqlKind.DATE_ADD, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.DATE_INTERVAL)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** THE "DATE_DIFF(date, date2, timeUnit)" function
   * (BigQuery) returns the number of timeUnit in (date - date2). */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_DIFF =
      new SqlTimestampDiffFunction("DATE_DIFF",
          OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.DATE, SqlTypeFamily.ANY));

  /** The "DATEADD(timeUnit, numeric, datetime)" function
   * (Microsoft SQL Server, Redshift, Snowflake). */
  @LibraryOperator(libraries = {MSSQL, REDSHIFT, SNOWFLAKE})
  public static final SqlFunction DATEADD =
      new SqlTimestampAddFunction("DATEADD");

  /** The "DATE_ADD(date, numDays)" function
   * (Spark) Returns the date that is num_days after start_date. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction DATE_ADD_SPARK =
      SqlBasicFunction.create(SqlKind.DATE_ADD, ReturnTypes.DATE_NULLABLE,
              OperandTypes.DATE_ANY)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "DATE_SUB(date, numDays)" function
   * (Spark) Returns the date that is num_days before start_date.*/
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlFunction DATE_SUB_SPARK =
      SqlBasicFunction.create(SqlKind.DATE_SUB, ReturnTypes.DATE_NULLABLE,
              OperandTypes.DATE_ANY)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "ADD_MONTHS(start_date, num_months)" function
   * (SPARK) Returns the date that is num_months after start_date. */
  @LibraryOperator(libraries = {ORACLE, SPARK})
  public static final SqlFunction ADD_MONTHS =
      SqlBasicFunction.create(SqlKind.ADD_MONTHS, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.DATE_ANY)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "DATEDIFF(timeUnit, datetime, datetime2)" function
   * (Microsoft SQL Server, Redshift, Snowflake).
   *
   * <p>MySQL has "DATEDIFF(date, date2)" and "TIMEDIFF(time, time2)" functions
   * but Calcite does not implement these because they have no "timeUnit"
   * argument. */
  @LibraryOperator(libraries = {MSSQL, REDSHIFT, SNOWFLAKE})
  public static final SqlFunction DATEDIFF =
      new SqlTimestampDiffFunction("DATEDIFF",
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.DATE,
              SqlTypeFamily.DATE));

  /** The "CONVERT(type, expr [,style])" function (Microsoft SQL Server).
   *
   * <p>Syntax:
   * <blockquote>{@code
   * CONVERT( data_type [ ( length ) ], expression [, style ] )
   * }</blockquote>
   *
   * <p>The optional "style" argument specifies how the value is going to be
   * converted; this implementation ignores the {@code style} parameter.
   *
   * <p>{@code CONVERT(type, expr, style)} is equivalent to CAST(expr AS type),
   * and the implementation delegates most of its logic to actual CAST operator.
   *
   * <p>Not to be confused with standard {@link SqlStdOperatorTable#CONVERT},
   * which converts a string from one character set to another. */
  @LibraryOperator(libraries = {MSSQL})
  public static final SqlFunction MSSQL_CONVERT =
      SqlBasicFunction.create(SqlKind.CAST,
              ReturnTypes.andThen(SqlLibraryOperators::transformConvert,
                  SqlCastFunction.returnTypeInference(false)),
              OperandTypes.repeat(SqlOperandCountRanges.between(2, 3),
                  OperandTypes.ANY))
          .withName("CONVERT")
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withOperandTypeInference(InferTypes.FIRST_KNOWN)
          .withOperandHandler(
              OperandHandlers.of(SqlLibraryOperators::transformConvert));

  /** Transforms a call binding of {@code CONVERT} to an equivalent binding for
   * {@code CAST}. */
  private static SqlCallBinding transformConvert(SqlOperatorBinding opBinding) {
    // Guaranteed to be a SqlCallBinding, with 2 or 3 arguments
    final SqlCallBinding binding = (SqlCallBinding) opBinding;
    return new SqlCallBinding(binding.getValidator(), binding.getScope(),
        transformConvert(binding.getValidator(), binding.getCall()));
  }

  /** Transforms a call to {@code CONVERT} to an equivalent call to
   * {@code CAST}. */
  private static SqlCall transformConvert(SqlValidator validator, SqlCall call) {
    return SqlStdOperatorTable.CAST.createCall(call.getParserPosition(),
        call.operand(1), call.operand(0));
  }

  /** The "DATE_PART(timeUnit, datetime)" function
   * (Databricks, Postgres, Redshift, Snowflake). */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction DATE_PART =
      new SqlExtractFunction("DATE_PART", true) {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
        }
      };

  /** The "DATE_SUB(date, interval)" function (BigQuery);
   * subtracts interval from the date, independent of any time zone. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_SUB =
      SqlBasicFunction.create(SqlKind.DATE_SUB, ReturnTypes.ARG0_NULLABLE,
           OperandTypes.DATE_INTERVAL)
          .withFunctionType(SqlFunctionCategory.TIMEDATE);

  /** The "DATEPART(timeUnit, datetime)" function
   * (Microsoft SQL Server). */
  @LibraryOperator(libraries = {MSSQL})
  public static final SqlFunction DATEPART =
      new SqlExtractFunction("DATEPART", false) {
        @Override public void unparse(SqlWriter writer, SqlCall call,
            int leftPrec, int rightPrec) {
          getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
        }
      };

  /** Return type inference for {@code DECODE}. */
  private static final SqlReturnTypeInference DECODE_RETURN_TYPE =
      opBinding -> {
        final List<RelDataType> list = new ArrayList<>();
        for (int i = 1, n = opBinding.getOperandCount(); i < n; i++) {
          if (i < n - 1) {
            ++i;
          }
          list.add(opBinding.getOperandType(i));
        }
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType type = typeFactory.leastRestrictive(list);
        if (type != null && opBinding.getOperandCount() % 2 == 1) {
          type = typeFactory.createTypeWithNullability(type, true);
        }
        return type;
      };

  /** The "DECODE(v, v1, result1, [v2, result2, ...], resultN)" function. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT, SPARK, HIVE})
  public static final SqlFunction DECODE =
      new SqlFunction("DECODE", SqlKind.DECODE, DECODE_RETURN_TYPE, null,
          OperandTypes.VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "IF(condition, thenValue, elseValue)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, HIVE, SPARK})
  public static final SqlFunction IF =
      new SqlFunction("IF", SqlKind.IF, SqlLibraryOperators::inferIfReturnType,
          null,
          OperandTypes.and(
              OperandTypes.family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY,
                  SqlTypeFamily.ANY),
              // Arguments 1 and 2 must have same type
              new SameOperandTypeChecker(3) {
                @Override protected List<Integer>
                getOperandList(int operandCount) {
                  return ImmutableList.of(1, 2);
                }
              }),
          SqlFunctionCategory.SYSTEM) {
        @Override public boolean validRexOperands(int count, Litmus litmus) {
          // IF is translated to RexNode by expanding to CASE.
          return litmus.fail("not a rex operator");
        }
      };

  /** Infers the return type of {@code IF(b, x, y)},
   * namely the least restrictive of the types of x and y.
   * Similar to {@link ReturnTypes#LEAST_RESTRICTIVE}. */
  private static @Nullable RelDataType inferIfReturnType(SqlOperatorBinding opBinding) {
    return opBinding.getTypeFactory()
        .leastRestrictive(opBinding.collectOperandTypes().subList(1, 3));
  }

  /** The "NVL(value, value)" function. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction NVL =
      new SqlFunction("NVL", SqlKind.NVL,
          ReturnTypes.LEAST_RESTRICTIVE
              .andThen(SqlTypeTransforms.TO_NULLABLE_ALL),
          OperandTypes.SAME_SAME);

  /** The "NVL2(value, value, value)" function. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT, SPARK})
  public static final SqlBasicFunction NVL2 =
      SqlBasicFunction.create(SqlKind.NVL2,
          ReturnTypes.NVL2_RESTRICTIVE,
          OperandTypes.SECOND_THIRD_SAME);

  /** The "IFNULL(value, value)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction IFNULL = NVL.withName("IFNULL");

  /** The "LEN(string)" function. */
  @LibraryOperator(libraries = {REDSHIFT, SNOWFLAKE, SPARK})
  public static final SqlFunction LEN =
      SqlStdOperatorTable.CHAR_LENGTH.withName("LEN");

  /** The "LENGTH(string)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, HIVE, POSTGRESQL, SNOWFLAKE, SPARK})
  public static final SqlFunction LENGTH =
      SqlStdOperatorTable.CHAR_LENGTH.withName("LENGTH");

  // Helper function for deriving types for the *PAD functions
  private static RelDataType deriveTypePad(SqlOperatorBinding binding, RelDataType type) {
    SqlTypeName result = SqlTypeUtil.isBinary(type) ? SqlTypeName.VARBINARY : SqlTypeName.VARCHAR;
    return binding.getTypeFactory().createSqlType(result);
  }

  /** The "LPAD(original_value, return_length[, pattern])" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction LPAD =
      SqlBasicFunction.create(
          "LPAD",
          ReturnTypes.ARG0.andThen(SqlLibraryOperators::deriveTypePad),
          OperandTypes.STRING_NUMERIC_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  /** The "RPAD(original_value, return_length[, pattern])" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction RPAD =
      SqlBasicFunction.create(
          "RPAD",
          ReturnTypes.ARG0.andThen(SqlLibraryOperators::deriveTypePad),
          OperandTypes.STRING_NUMERIC_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  /** The "LTRIM(string)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction LTRIM =
      new SqlFunction("LTRIM", SqlKind.LTRIM,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** The "RTRIM(string)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction RTRIM =
      new SqlFunction("RTRIM", SqlKind.RTRIM,
          ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** BigQuery's "SUBSTR(string, position [, substringLength ])" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction SUBSTR_BIG_QUERY =
      new SqlFunction("SUBSTR", SqlKind.SUBSTR_BIG_QUERY,
          ReturnTypes.ARG0_NULLABLE_VARYING, null,
          OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** MySQL's "SUBSTR(string, position [, substringLength ])" function. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction SUBSTR_MYSQL =
      new SqlFunction("SUBSTR", SqlKind.SUBSTR_MYSQL,
          ReturnTypes.ARG0_NULLABLE_VARYING, null,
          OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** Oracle's "SUBSTR(string, position [, substringLength ])" function.
   *
   * <p>It has different semantics to standard SQL's
   * {@link SqlStdOperatorTable#SUBSTRING} function:
   *
   * <ul>
   *   <li>If {@code substringLength} &le; 0, result is the empty string
   *   (Oracle would return null, because it treats the empty string as null,
   *   but Calcite does not have these semantics);
   *   <li>If {@code position} = 0, treat {@code position} as 1;
   *   <li>If {@code position} &lt; 0, treat {@code position} as
   *       "length(string) + position + 1".
   * </ul>
   */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction SUBSTR_ORACLE =
      new SqlFunction("SUBSTR", SqlKind.SUBSTR_ORACLE,
          ReturnTypes.ARG0_NULLABLE_VARYING, null,
          OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** PostgreSQL's "SUBSTR(string, position [, substringLength ])" function. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction SUBSTR_POSTGRESQL =
      new SqlFunction("SUBSTR", SqlKind.SUBSTR_POSTGRESQL,
          ReturnTypes.ARG0_NULLABLE_VARYING, null,
          OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "GREATEST(value, value)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, HIVE})
  public static final SqlFunction GREATEST =
      new SqlFunction("GREATEST", SqlKind.GREATEST,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "LEAST(value, value)" function. */
  @LibraryOperator(libraries = {BIG_QUERY, ORACLE, HIVE})
  public static final SqlFunction LEAST =
      new SqlFunction("LEAST", SqlKind.LEAST,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /**
   * The <code>TRANSLATE(<i>string_expr</i>, <i>search_chars</i>,
   * <i>replacement_chars</i>)</code> function returns <i>string_expr</i> with
   * all occurrences of each character in <i>search_chars</i> replaced by its
   * corresponding character in <i>replacement_chars</i>.
   *
   * <p>It is not defined in the SQL standard, but occurs in Oracle and
   * PostgreSQL.
   */
  @LibraryOperator(libraries = {ORACLE, POSTGRESQL})
  public static final SqlFunction TRANSLATE3 = new SqlTranslate3Function();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_TYPE = new SqlJsonTypeFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_DEPTH = new SqlJsonDepthFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_LENGTH = new SqlJsonLengthFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_KEYS = new SqlJsonKeysFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_PRETTY = new SqlJsonPrettyFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_REMOVE = new SqlJsonRemoveFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_STORAGE_SIZE = new SqlJsonStorageSizeFunction();

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_INSERT = new SqlJsonModifyFunction("JSON_INSERT");

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_REPLACE = new SqlJsonModifyFunction("JSON_REPLACE");

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction JSON_SET = new SqlJsonModifyFunction("JSON_SET");

  /** The "REGEXP_CONTAINS(value, regexp)" function.
   * Returns TRUE if value is a partial match for the regular expression, regexp. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction REGEXP_CONTAINS =
      SqlBasicFunction.create("REGEXP_CONTAINS", ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_EXTRACT(value, regexp[, position[, occurrence]])" function.
   * Returns the substring in value that matches the regexp. Returns NULL if there is no match. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction REGEXP_EXTRACT =
      SqlBasicFunction.create("REGEXP_EXTRACT", ReturnTypes.VARCHAR_FORCE_NULLABLE,
          OperandTypes.STRING_STRING_OPTIONAL_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_EXTRACT_ALL(value, regexp)" function.
   * Returns the substring in value that matches the regexp. Returns NULL if there is no match. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction REGEXP_EXTRACT_ALL =
      SqlBasicFunction.create("REGEXP_EXTRACT_ALL", ReturnTypes.ARG0
              .andThen(SqlTypeTransforms.TO_ARRAY).andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_INSTR(value, regexp [, position[, occurrence, [occurrence_position]]])" function.
   * Returns the lowest 1-based position of a regexp in value. Returns NULL if there is no match. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlBasicFunction REGEXP_INSTR =
      SqlBasicFunction.create("REGEXP_INSTR", ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.STRING_STRING_OPTIONAL_INTEGER_OPTIONAL_INTEGER_OPTIONAL_INTEGER,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. */
  @LibraryOperator(libraries = {REDSHIFT})
  public static final SqlFunction REGEXP_REPLACE_2 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. */
  @LibraryOperator(libraries = {MYSQL, ORACLE, REDSHIFT, HIVE})
  public static final SqlFunction REGEXP_REPLACE_3 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, pos)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. Start searching value from character position
   * pos. */
  @LibraryOperator(libraries = {MYSQL, ORACLE, REDSHIFT})
  public static final SqlFunction REGEXP_REPLACE_4 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING,
              SqlTypeFamily.INTEGER),
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, pos, [ occurrence | matchType ])"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. Start searching value from character position
   * pos. Replace only the occurrence match or all matches if occurrence is 0. matchType
   * is a string of flags to apply to the search. */
  @LibraryOperator(libraries = {MYSQL, REDSHIFT})
  public static final SqlFunction REGEXP_REPLACE_5 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.or(
              OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
                  SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
              OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
                  SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING)),
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, pos, matchType)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. Start searching value from character position
   * pos. Replace only the occurrence match or all matches if occurrence is 0. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction REGEXP_REPLACE_5_ORACLE =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
              SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, pos, occurrence, matchType)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. Start searching value from character position
   * pos. Replace only the occurrence match or all matches if occurrence is 0. matchType
   * is a string of flags to apply to the search. */
  @LibraryOperator(libraries = {MYSQL, ORACLE, REDSHIFT})
  public static final SqlFunction REGEXP_REPLACE_6 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING,
              SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.STRING),
          SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction REGEXP_REPLACE_BIG_QUERY_3 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = REDSHIFT)
  public static final SqlFunction REGEXP_REPLACE_PG_3 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_REPLACE(value, regexp, rep, flags)"
   * function. Replaces all substrings of value that match regexp with
   * {@code rep} and returns modified value. flags are applied to the search. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = REDSHIFT)
  public static final SqlFunction REGEXP_REPLACE_PG_4 =
      SqlBasicFunction.create("REGEXP_REPLACE", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_STRING_STRING_STRING, SqlFunctionCategory.STRING);

  /** The "REGEXP_SUBSTR(value, regexp[, position[, occurrence]])" function.
   * Returns the substring in value that matches the regexp. Returns NULL if there is no match. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction REGEXP_SUBSTR = REGEXP_EXTRACT.withName("REGEXP_SUBSTR");

  /** The "REGEXP(value, regexp)" function, equivalent to {@link #RLIKE}. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction REGEXP =
      SqlBasicFunction.create("REGEXP", ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The "REGEXP_LIKE(value, regexp)" function, equivalent to {@link #RLIKE}. */
  @LibraryOperator(libraries = {SPARK, MYSQL, POSTGRESQL, ORACLE})
  public static final SqlFunction REGEXP_LIKE =
      SqlBasicFunction.create("REGEXP_LIKE", ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.STRING_STRING_OPTIONAL_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction COMPRESS =
      new SqlFunction("COMPRESS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARBINARY)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.STRING, SqlFunctionCategory.STRING);


  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction EXTRACT_VALUE =
      new SqlFunction("EXTRACTVALUE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          null, OperandTypes.STRING_STRING, SqlFunctionCategory.SYSTEM);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction XML_TRANSFORM =
      new SqlFunction("XMLTRANSFORM", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          null, OperandTypes.STRING_STRING, SqlFunctionCategory.SYSTEM);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction EXTRACT_XML =
      new SqlFunction("EXTRACT", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
          null, OperandTypes.STRING_STRING_OPTIONAL_STRING,
          SqlFunctionCategory.SYSTEM);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction EXISTS_NODE =
      new SqlFunction("EXISTSNODE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE
              .andThen(SqlTypeTransforms.FORCE_NULLABLE), null,
          OperandTypes.STRING_STRING_OPTIONAL_STRING, SqlFunctionCategory.SYSTEM);

  /** The "BOOL_AND(condition)" aggregate function, PostgreSQL and Redshift's
   * equivalent to {@link SqlStdOperatorTable#EVERY}. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlAggFunction BOOL_AND =
      new SqlMinMaxAggFunction("BOOL_AND", SqlKind.MIN, OperandTypes.BOOLEAN);

  /** The "BOOL_OR(condition)" aggregate function, PostgreSQL and Redshift's
   * equivalent to {@link SqlStdOperatorTable#SOME}. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlAggFunction BOOL_OR =
      new SqlMinMaxAggFunction("BOOL_OR", SqlKind.MAX, OperandTypes.BOOLEAN);

  /** The "LOGICAL_AND(condition)" aggregate function, BigQuery's
   * equivalent to {@link SqlStdOperatorTable#EVERY}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlAggFunction LOGICAL_AND =
      new SqlMinMaxAggFunction("LOGICAL_AND", SqlKind.MIN, OperandTypes.BOOLEAN);

  /** The "LOGICAL_OR(condition)" aggregate function, BigQuery's
   * equivalent to {@link SqlStdOperatorTable#SOME}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlAggFunction LOGICAL_OR =
      new SqlMinMaxAggFunction("LOGICAL_OR", SqlKind.MAX, OperandTypes.BOOLEAN);

  /** The "COUNTIF(condition) [OVER (...)]" function, in BigQuery,
   * returns the count of TRUE values for expression.
   *
   * <p>{@code COUNTIF(b)} is equivalent to
   * {@code COUNT(*) FILTER (WHERE b)}. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlAggFunction COUNTIF =
      SqlBasicAggFunction
          .create(SqlKind.COUNTIF, ReturnTypes.BIGINT, OperandTypes.BOOLEAN)
          .withDistinct(Optionality.FORBIDDEN);

  /** The "ARRAY_AGG(value [ ORDER BY ...])" aggregate function,
   * in BigQuery and PostgreSQL, gathers values into arrays. */
  @LibraryOperator(libraries = {POSTGRESQL, BIG_QUERY})
  public static final SqlAggFunction ARRAY_AGG =
      SqlBasicAggFunction
          .create(SqlKind.ARRAY_AGG,
              ReturnTypes.andThen(ReturnTypes::stripOrderBy,
                  ReturnTypes.TO_ARRAY), OperandTypes.ANY)
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION)
          .withAllowsNullTreatment(true);

  /** The "ARRAY_CONCAT_AGG(value [ ORDER BY ...])" aggregate function,
   * in BigQuery and PostgreSQL, concatenates array values into arrays. */
  @LibraryOperator(libraries = {POSTGRESQL, BIG_QUERY})
  public static final SqlAggFunction ARRAY_CONCAT_AGG =
      SqlBasicAggFunction
          .create(SqlKind.ARRAY_CONCAT_AGG, ReturnTypes.ARG0,
              OperandTypes.ARRAY)
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION);

  /** The "STRING_AGG(value [, separator ] [ ORDER BY ...])" aggregate function,
   * BigQuery and PostgreSQL's equivalent of
   * {@link SqlStdOperatorTable#LISTAGG}.
   *
   * <p>{@code STRING_AGG(v, sep ORDER BY x, y)} is implemented by
   * rewriting to {@code LISTAGG(v, sep) WITHIN GROUP (ORDER BY x, y)}. */
  @LibraryOperator(libraries = {POSTGRESQL, BIG_QUERY})
  public static final SqlAggFunction STRING_AGG =
      SqlBasicAggFunction
          .create(SqlKind.STRING_AGG, ReturnTypes.ARG0_NULLABLE,
              OperandTypes.or(OperandTypes.STRING, OperandTypes.STRING_STRING))
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION);

  /** The "GROUP_CONCAT([DISTINCT] expr [, ...] [ORDER BY ...] [SEPARATOR sep])"
   * aggregate function, MySQL's equivalent of
   * {@link SqlStdOperatorTable#LISTAGG}.
   *
   * <p>{@code GROUP_CONCAT(v ORDER BY x, y SEPARATOR s)} is implemented by
   * rewriting to {@code LISTAGG(v, s) WITHIN GROUP (ORDER BY x, y)}. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlAggFunction GROUP_CONCAT =
      SqlBasicAggFunction
          .create(SqlKind.GROUP_CONCAT,
              ReturnTypes.andThen(ReturnTypes::stripOrderBy,
                  ReturnTypes.ARG0_NULLABLE),
              OperandTypes.or(OperandTypes.STRING, OperandTypes.STRING_STRING))
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withAllowsNullTreatment(false)
          .withAllowsSeparator(true)
          .withSyntax(SqlSyntax.ORDERED_FUNCTION);

  /** The "DATE(string)" function, equivalent to "CAST(string AS DATE). */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE =
      new SqlFunction("DATE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE, null, OperandTypes.STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "CURRENT_DATETIME([timezone])" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction CURRENT_DATETIME =
      new SqlFunction("CURRENT_DATETIME", SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP.andThen(SqlTypeTransforms.TO_NULLABLE), null,
          OperandTypes.or(OperandTypes.NILADIC, OperandTypes.STRING),
          SqlFunctionCategory.TIMEDATE);

  /** The "DATE_FROM_UNIX_DATE(integer)" function; returns a DATE value
   * a given number of seconds after 1970-01-01. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction DATE_FROM_UNIX_DATE =
      new SqlFunction("DATE_FROM_UNIX_DATE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE, null, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_DATE(date)" function; returns the number of days since
   * 1970-01-01. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction UNIX_DATE =
      new SqlFunction("UNIX_DATE", SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE, null, OperandTypes.DATE,
          SqlFunctionCategory.TIMEDATE);

  /** The "MONTHNAME(datetime)" function; returns the name of the month,
   * in the current locale, of a TIMESTAMP or DATE argument. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction MONTHNAME =
      new SqlFunction("MONTHNAME", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000, null, OperandTypes.DATETIME,
          SqlFunctionCategory.TIMEDATE);

  /** The "DAYNAME(datetime)" function; returns the name of the day of the week,
   * in the current locale, of a TIMESTAMP or DATE argument. */
  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction DAYNAME =
      new SqlFunction("DAYNAME", SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000, null, OperandTypes.DATETIME,
          SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction LEFT =
      new SqlFunction("LEFT", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null,
          OperandTypes.CBSTRING_INTEGER, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction REPEAT =
      new SqlFunction(
          "REPEAT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING,
          null,
          OperandTypes.STRING_INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction RIGHT =
      new SqlFunction("RIGHT", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null,
          OperandTypes.CBSTRING_INTEGER, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL, SPARK, HIVE})
  public static final SqlFunction SPACE =
      new SqlFunction("SPACE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000_NULLABLE,
          null,
          OperandTypes.INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction STRCMP =
      new SqlFunction("STRCMP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, ORACLE, HIVE})
  public static final SqlFunction SOUNDEX =
      new SqlFunction("SOUNDEX",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_4_NULLABLE,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlFunction DIFFERENCE =
      new SqlFunction("DIFFERENCE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  /** The case-insensitive variant of the LIKE operator. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlSpecialOperator ILIKE =
      new SqlLikeOperator("ILIKE", SqlKind.LIKE, false, false);

  /** The case-insensitive variant of the NOT LIKE operator. */
  @LibraryOperator(libraries = {POSTGRESQL})
  public static final SqlSpecialOperator NOT_ILIKE =
      new SqlLikeOperator("NOT ILIKE", SqlKind.LIKE, true, false);

  /** The regex variant of the LIKE operator. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlSpecialOperator RLIKE =
      new SqlLikeOperator("RLIKE", SqlKind.RLIKE, false, true);

  /** The regex variant of the NOT LIKE operator. */
  @LibraryOperator(libraries = {SPARK, HIVE})
  public static final SqlSpecialOperator NOT_RLIKE =
      new SqlLikeOperator("NOT RLIKE", SqlKind.RLIKE, true, true);

  /** The "CONCAT(arg, ...)" function that concatenates strings.
   * For example, "CONCAT('a', 'bc', 'd')" returns "abcd". */
  @LibraryOperator(libraries = {MYSQL, POSTGRESQL})
  public static final SqlFunction CONCAT_FUNCTION =
      new SqlFunction("CONCAT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE,
          InferTypes.RETURN_TYPE,
          OperandTypes.repeat(SqlOperandCountRanges.from(2),
              OperandTypes.STRING),
          SqlFunctionCategory.STRING);

  /** The "CONCAT(arg0, arg1)" function that concatenates strings.
   * For example, "CONCAT('a', 'bc')" returns "abc".
   *
   * <p>It is assigned {@link SqlKind#CONCAT2} to make it not equal to
   * {@link #CONCAT_FUNCTION}. */
  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction CONCAT2 =
      new SqlFunction("CONCAT",
          SqlKind.CONCAT2,
          ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE,
          InferTypes.RETURN_TYPE,
          OperandTypes.STRING_SAME_SAME,
          SqlFunctionCategory.STRING);

  /** The "ARRAY_LENGTH(array)" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_LENGTH =
      new SqlFunction("ARRAY_LENGTH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.ARRAY,
          SqlFunctionCategory.SYSTEM);

  /** The "ARRAY_REVERSE(array)" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_REVERSE =
      new SqlFunction("ARRAY_REVERSE",
          SqlKind.ARRAY_REVERSE,
          ReturnTypes.ARG0_NULLABLE,
          null,
          OperandTypes.ARRAY,
          SqlFunctionCategory.SYSTEM);

  /** The "ARRAY_CONCAT(array [, array]*)" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_CONCAT =
      new SqlFunction("ARRAY_CONCAT",
          SqlKind.ARRAY_CONCAT,
          ReturnTypes.LEAST_RESTRICTIVE,
          null,
          OperandTypes.AT_LEAST_ONE_SAME_VARIADIC,
          SqlFunctionCategory.SYSTEM);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction REVERSE =
      new SqlFunction("REVERSE",
          SqlKind.REVERSE,
          ReturnTypes.ARG0_NULLABLE_VARYING,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction FROM_BASE64 =
      new SqlFunction("FROM_BASE64", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARBINARY)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.STRING, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction TO_BASE64 =
      new SqlFunction("TO_BASE64", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  /** The "ARRAY_INSERT(array, pos, val)" function (Spark). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_INSERT =
      SqlBasicFunction.create(SqlKind.ARRAY_INSERT,
          SqlLibraryOperators::arrayInsertReturnType,
          OperandTypes.ARRAY_INSERT);

  /** The "ARRAY_INTERSECT(array1, array2)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_INTERSECT =
      SqlBasicFunction.create(SqlKind.ARRAY_INTERSECT,
          ReturnTypes.LEAST_RESTRICTIVE,
          OperandTypes.and(
              OperandTypes.NONNULL_NONNULL,
              OperandTypes.SAME_SAME,
              OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)));

  /** The "ARRAY_JOIN(array, delimiter [, nullText ])" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_JOIN =
      SqlBasicFunction.create(SqlKind.ARRAY_JOIN,
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_ARRAY_CHARACTER_OPTIONAL_CHARACTER);

  /** The "ARRAY_LENGTH(array)" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_LENGTH =
      SqlBasicFunction.create(SqlKind.ARRAY_LENGTH,
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.ARRAY);

  /** The "ARRAY_MAX(array)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_MAX =
      SqlBasicFunction.create(SqlKind.ARRAY_MAX,
          ReturnTypes.TO_COLLECTION_ELEMENT_FORCE_NULLABLE,
          OperandTypes.ARRAY_NONNULL);

  /** The "ARRAY_MAX(array)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_MIN =
      SqlBasicFunction.create(SqlKind.ARRAY_MIN,
          ReturnTypes.TO_COLLECTION_ELEMENT_FORCE_NULLABLE,
          OperandTypes.ARRAY_NONNULL);

  /** The "ARRAY_POSITION(array, element)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_POSITION =
      SqlBasicFunction.create(SqlKind.ARRAY_POSITION,
          ReturnTypes.BIGINT_NULLABLE,
          OperandTypes.ARRAY_ELEMENT_NONNULL);

  /** The "ARRAY_PREPEND(array, element)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_PREPEND =
      SqlBasicFunction.create(SqlKind.ARRAY_PREPEND,
          SqlLibraryOperators::arrayAppendPrependReturnType,
          OperandTypes.ARRAY_ELEMENT_NONNULL);

  /** The "ARRAY_REMOVE(array, element)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_REMOVE =
      SqlBasicFunction.create(SqlKind.ARRAY_REMOVE,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.ARRAY_ELEMENT_NONNULL);

  /** The "ARRAY_REPEAT(element, count)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_REPEAT =
      SqlBasicFunction.create(SqlKind.ARRAY_REPEAT,
          ReturnTypes.TO_ARRAY.andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.sequence(
              "ARRAY_REPEAT(ANY, INTEGER)",
              OperandTypes.ANY, OperandTypes.typeName(SqlTypeName.INTEGER)));

  /** The "ARRAY_REVERSE(array)" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_REVERSE =
      SqlBasicFunction.create(SqlKind.ARRAY_REVERSE,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.ARRAY);

  /** The "ARRAY_SIZE(array)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_SIZE =
      SqlBasicFunction.create(SqlKind.ARRAY_SIZE,
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.ARRAY);

  /** The "ARRAY_SLICE(array, start, length)" function. */
  @LibraryOperator(libraries = {HIVE})
  public static final SqlFunction ARRAY_SLICE =
      SqlBasicFunction.create(SqlKind.ARRAY_SLICE,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.sequence(
              "ARRAY_SLICE(ARRAY, INTEGER, INTEGER)",
              OperandTypes.ARRAY, OperandTypes.INTEGER, OperandTypes.INTEGER));

  /** The "ARRAY_UNION(array1, array2)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAY_UNION =
      SqlBasicFunction.create(SqlKind.ARRAY_UNION,
          ReturnTypes.LEAST_RESTRICTIVE,
          OperandTypes.and(
              OperandTypes.SAME_SAME,
              OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)));

  /** The "ARRAY_TO_STRING(array, delimiter [, nullText ])" function. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction ARRAY_TO_STRING =
      SqlBasicFunction.create(SqlKind.ARRAY_TO_STRING,
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING_ARRAY_CHARACTER_OPTIONAL_CHARACTER);

  /** The "ARRAYS_OVERLAP(array1, array2)" function (Spark). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAYS_OVERLAP =
      SqlBasicFunction.create(SqlKind.ARRAYS_OVERLAP,
          ReturnTypes.BOOLEAN_NULLABLE.andThen(SqlTypeTransforms.COLLECTION_ELEMENT_TYPE_NULLABLE),
          OperandTypes.and(
              OperandTypes.SAME_SAME,
              OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY),
              OperandTypes.NONNULL_NONNULL));

  private static RelDataType deriveTypeArraysZip(SqlOperatorBinding opBinding) {
    final List<RelDataType> argComponentTypes = new ArrayList<>();
    for (RelDataType arrayType : opBinding.collectOperandTypes()) {
      final RelDataType componentType = requireNonNull(arrayType.getComponentType());
      argComponentTypes.add(componentType);
    }

    final List<String> indexes = IntStream.range(0, argComponentTypes.size())
        .mapToObj(i -> String.valueOf(i))
        .collect(Collectors.toList());
    final RelDataType structType =
        opBinding.getTypeFactory().createStructType(argComponentTypes, indexes);
    return SqlTypeUtil.createArrayType(
        opBinding.getTypeFactory(),
        requireNonNull(structType, "inferred value type"),
        false);
  }

  /** The "ARRAYS_ZIP(array, ...)" function (Spark). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction ARRAYS_ZIP =
      SqlBasicFunction.create(SqlKind.ARRAYS_ZIP,
          ((SqlReturnTypeInference) SqlLibraryOperators::deriveTypeArraysZip)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          OperandTypes.SAME_VARIADIC);

  /** The "SORT_ARRAY(array)" function (Spark). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction SORT_ARRAY =
      SqlBasicFunction.create(SqlKind.SORT_ARRAY,
          ReturnTypes.ARG0_NULLABLE,
          OperandTypes.ARRAY.or(OperandTypes.ARRAY_BOOLEAN_LITERAL));

  private static RelDataType deriveTypeMapConcat(SqlOperatorBinding opBinding) {
    if (opBinding.getOperandCount() == 0) {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      requireNonNull(type, "type");
      return SqlTypeUtil.createMapType(typeFactory, type, type, true);
    } else {
      final List<RelDataType> operandTypes = opBinding.collectOperandTypes();
      for (RelDataType operandType : operandTypes) {
        if (!SqlTypeUtil.isMap(operandType)) {
          throw opBinding.newError(
              RESOURCE.typesShouldAllBeMap(
                  opBinding.getOperator().getName(),
                  operandType.getFullTypeString()));
        }
      }
      return requireNonNull(opBinding.getTypeFactory().leastRestrictive(operandTypes));
    }
  }

  /** The "MAP_CONCAT(map [, map]*)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_CONCAT =
      SqlBasicFunction.create(SqlKind.MAP_CONCAT,
          SqlLibraryOperators::deriveTypeMapConcat,
          OperandTypes.SAME_VARIADIC);

  /** The "MAP_ENTRIES(map)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_ENTRIES =
      SqlBasicFunction.create(SqlKind.MAP_ENTRIES,
          ReturnTypes.TO_MAP_ENTRIES_NULLABLE,
          OperandTypes.MAP);

  /** The "MAP_KEYS(map)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_KEYS =
      SqlBasicFunction.create(SqlKind.MAP_KEYS,
          ReturnTypes.TO_MAP_KEYS_NULLABLE,
          OperandTypes.MAP);

  /** The "MAP_VALUES(map)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_VALUES =
      SqlBasicFunction.create(SqlKind.MAP_VALUES,
          ReturnTypes.TO_MAP_VALUES_NULLABLE,
          OperandTypes.MAP);

  /** The "MAP_CONTAINS_KEY(map, key)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_CONTAINS_KEY =
      SqlBasicFunction.create(SqlKind.MAP_CONTAINS_KEY,
          ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.MAP_KEY);

  private static RelDataType deriveTypeMapFromArrays(SqlOperatorBinding opBinding) {
    final RelDataType keysArrayType = opBinding.getOperandType(0);
    final RelDataType valuesArrayType = opBinding.getOperandType(1);
    final boolean nullable = keysArrayType.isNullable() || valuesArrayType.isNullable();
    return SqlTypeUtil.createMapType(
        opBinding.getTypeFactory(),
        requireNonNull(keysArrayType.getComponentType(), "inferred key type"),
        requireNonNull(valuesArrayType.getComponentType(), "inferred value type"),
        nullable);
  }

  /** The "MAP_FROM_ARRAYS(keysArray, valuesArray)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_FROM_ARRAYS =
      SqlBasicFunction.create(SqlKind.MAP_FROM_ARRAYS,
          SqlLibraryOperators::deriveTypeMapFromArrays,
          OperandTypes.ARRAY_ARRAY);

  private static RelDataType deriveTypeMapFromEntries(SqlOperatorBinding opBinding) {
    final RelDataType entriesType = opBinding.collectOperandTypes().get(0);
    final RelDataType entryType = entriesType.getComponentType();
    requireNonNull(entryType, () -> "componentType of " + entriesType);
    return SqlTypeUtil.createMapType(
        opBinding.getTypeFactory(),
        requireNonNull(entryType.getFieldList().get(0).getType(), "inferred key type"),
        requireNonNull(entryType.getFieldList().get(1).getType(), "inferred value type"),
        entriesType.isNullable() || entryType.isNullable());
  }

  /** The "MAP_FROM_ENTRIES(arrayOfEntries)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction MAP_FROM_ENTRIES =
      SqlBasicFunction.create(SqlKind.MAP_FROM_ENTRIES,
          SqlLibraryOperators::deriveTypeMapFromEntries,
          OperandTypes.MAP_FROM_ENTRIES);

  /** The "STR_TO_MAP(string[, stringDelimiter[, keyValueDelimiter]])" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction STR_TO_MAP =
      SqlBasicFunction.create(SqlKind.STR_TO_MAP,
          ReturnTypes.IDENTITY_TO_MAP_NULLABLE,
          OperandTypes.STRING_OPTIONAL_STRING_OPTIONAL_STRING);

  /** The "SUBSTRING_INDEX(string, delimiter, count)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction SUBSTRING_INDEX =
      SqlBasicFunction.create(SqlKind.SUBSTRING_INDEX,
          ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.STRING_STRING_INTEGER)
          .withFunctionType(SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL})
  public static final SqlFunction REVERSE =
      SqlBasicFunction.create(SqlKind.REVERSE,
          ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.CHARACTER)
          .withFunctionType(SqlFunctionCategory.STRING);

  /** The "REVERSE(string|array)" function. */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction REVERSE_SPARK =
      SqlBasicFunction.create(SqlKind.REVERSE,
              ReturnTypes.ARG0_ARRAY_NULLABLE_VARYING,
              OperandTypes.CHARACTER.or(OperandTypes.ARRAY))
          .withFunctionType(SqlFunctionCategory.STRING)
          .withKind(SqlKind.REVERSE_SPARK);

  /** The "LEVENSHTEIN(string1, string2)" function. */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction LEVENSHTEIN =
      SqlBasicFunction.create("LEVENSHTEIN",
          ReturnTypes.INTEGER_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL})
  public static final SqlFunction FROM_BASE64 =
      SqlBasicFunction.create("FROM_BASE64",
          ReturnTypes.VARBINARY_FORCE_NULLABLE,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {MYSQL})
  public static final SqlFunction TO_BASE64 =
      SqlBasicFunction.create("TO_BASE64",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {HIVE})
  public static final SqlFunction BASE64 =
      SqlBasicFunction.create("BASE64",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING.or(OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {HIVE})
  public static final SqlFunction UN_BASE64 =
      SqlBasicFunction.create("UNBASE64",
          ReturnTypes.VARBINARY_FORCE_NULLABLE,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FROM_BASE32 =
      SqlBasicFunction.create("FROM_BASE32",
          ReturnTypes.VARBINARY_NULLABLE,
          OperandTypes.CHARACTER, SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TO_BASE32 =
      SqlBasicFunction.create("TO_BASE32",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.STRING,
          SqlFunctionCategory.STRING);

  /**
   * The "FROM_HEX(varchar)" function; converts a hexadecimal-encoded {@code varchar} into bytes.
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction FROM_HEX =
      SqlBasicFunction.create("FROM_HEX",
          ReturnTypes.VARBINARY_NULLABLE,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  /**
   * The "TO_HEX(binary)" function; converts {@code binary} into a hexadecimal varchar.
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TO_HEX =
      SqlBasicFunction.create("TO_HEX",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.BINARY,
          SqlFunctionCategory.STRING);

  /**
   * The "HEX(string)" function; converts {@code string} into a hexadecimal varchar.
   */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction HEX =
      SqlBasicFunction.create("HEX",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  /** The "FORMAT_NUMBER(value, decimalOrFormat)" function. */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction FORMAT_NUMBER =
      SqlBasicFunction.create("FORMAT_NUMBER",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.or(
              OperandTypes.NUMERIC_NUMERIC,
              OperandTypes.NUMERIC_CHARACTER),
          SqlFunctionCategory.STRING);

  /** The "TO_CHAR(timestamp, format)" function;
   * converts {@code timestamp} to string according to the given {@code format}.
   *
   * <p>({@code TO_CHAR} is not supported in MySQL, but it is supported in
   * MariaDB, a variant of MySQL covered by {@link SqlLibrary#MYSQL}.) */
  @LibraryOperator(libraries = {MYSQL, ORACLE, REDSHIFT})
  public static final SqlFunction TO_CHAR =
      SqlBasicFunction.create("TO_CHAR",
          ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.TIMESTAMP_STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "TO_CHAR(timestamp, format)" function;
   * converts {@code timestamp} to string according to the given {@code format}. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction TO_CHAR_PG =
      SqlBasicFunction.create("TO_CHAR", ReturnTypes.VARCHAR_NULLABLE,
          OperandTypes.TIMESTAMP_STRING, SqlFunctionCategory.TIMEDATE);

  /** The "TO_DATE(string1, string2)" function; casts string1
   * to a DATE using the format specified in string2. */
  @LibraryOperator(libraries = {ORACLE, REDSHIFT, HIVE})
  public static final SqlFunction TO_DATE =
      SqlBasicFunction.create("TO_DATE",
          ReturnTypes.DATE_NULLABLE,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "TO_DATE(string1, string2)" function for PostgreSQL; casts string1
   * to a DATE using the format specified in string2. */
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction TO_DATE_PG =
      SqlBasicFunction.create("TO_DATE", ReturnTypes.DATE_NULLABLE,
          OperandTypes.STRING_STRING, SqlFunctionCategory.TIMEDATE);

  /** The "TO_TIMESTAMP(string1, string2)" function; casts string1
   * to a TIMESTAMP using the format specified in string2. */
  @LibraryOperator(libraries = {POSTGRESQL, ORACLE})
  public static final SqlFunction TO_TIMESTAMP =
      new SqlFunction("TO_TIMESTAMP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_SECONDS(bigint)" function; returns a TIMESTAMP value
   * a given number of seconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP_SECONDS =
      new SqlFunction("TIMESTAMP_SECONDS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_MILLIS(bigint)" function; returns a TIMESTAMP value
   * a given number of milliseconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP_MILLIS =
      new SqlFunction("TIMESTAMP_MILLIS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "TIMESTAMP_MICROS(bigint)" function; returns a TIMESTAMP value
   * a given number of micro-seconds after 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TIMESTAMP_MICROS =
      new SqlFunction("TIMESTAMP_MICROS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_SECONDS(bigint)" function; returns the number of seconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction UNIX_SECONDS =
      new SqlFunction("UNIX_SECONDS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_MILLIS(bigint)" function; returns the number of milliseconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction UNIX_MILLIS =
      new SqlFunction("UNIX_MILLIS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
          SqlFunctionCategory.TIMEDATE);

  /** The "UNIX_MICROS(bigint)" function; returns the number of microseconds
   * since 1970-01-01 00:00:00. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction UNIX_MICROS =
      new SqlFunction("UNIX_MICROS", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
          SqlFunctionCategory.TIMEDATE);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction CHR =
      new SqlFunction("CHR",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.CHAR,
          null,
          OperandTypes.INTEGER,
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction TANH =
      new SqlFunction("TANH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction COSH =
      new SqlFunction("COSH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {ORACLE})
  public static final SqlFunction SINH =
      new SqlFunction("SINH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code FACTORIAL(integer)} function.
   * Returns the factorial of integer, the range of integer is [0, 20].
   * Otherwise, returns NULL. */
  @LibraryOperator(libraries = {HIVE, SPARK})
  public static final SqlFunction FACTORIAL =
      SqlBasicFunction.create("FACTORIAL",
          ReturnTypes.BIGINT_FORCE_NULLABLE,
          OperandTypes.INTEGER,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction MD5 =
      new SqlFunction("MD5", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  @LibraryOperator(libraries = {BIG_QUERY, MYSQL, POSTGRESQL, SPARK, HIVE})
  public static final SqlFunction SHA1 =
      new SqlFunction("SHA1", SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR)
              .andThen(SqlTypeTransforms.TO_NULLABLE),
          null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
          SqlFunctionCategory.STRING);

  /** The "IS_INF(value)" function. Returns whether value is infinite. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction IS_INF =
      SqlBasicFunction.create("IS_INF",
          ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "IS_NAN(value)" function. Returns whether value is NaN. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction IS_NAN =
      SqlBasicFunction.create("IS_NAN",
          ReturnTypes.BOOLEAN_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "LOG(value [, value2])" function.
   *
   * @see SqlStdOperatorTable#LN
   * @see SqlStdOperatorTable#LOG10
   */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction LOG =
      SqlBasicFunction.create("LOG",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC_OPTIONAL_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "LOG(numeric1 [, numeric2 ]) " function. Returns the logarithm of numeric2
   * to base numeric1.*/
  @LibraryOperator(libraries = {MYSQL, SPARK, HIVE})
  public static final SqlFunction LOG_MYSQL =
      SqlBasicFunction.create(SqlKind.LOG,
          ReturnTypes.DOUBLE_FORCE_NULLABLE,
          OperandTypes.NUMERIC_OPTIONAL_NUMERIC);

  /** The "LOG(numeric1 [, numeric2 ]) " function. Returns the logarithm of numeric2
   * to base numeric1.*/
  @LibraryOperator(libraries = {POSTGRESQL}, exceptLibraries = {REDSHIFT})
  public static final SqlFunction LOG_POSTGRES =
      SqlBasicFunction.create("LOG", ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC_OPTIONAL_NUMERIC, SqlFunctionCategory.NUMERIC);

  /** The "LOG2(numeric)" function. Returns the base 2 logarithm of numeric. */
  @LibraryOperator(libraries = {MYSQL, SPARK})
  public static final SqlFunction LOG2 =
      SqlBasicFunction.create("LOG2",
          ReturnTypes.DOUBLE_FORCE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "LOG1p(numeric)" function. Returns log(1 + numeric). */
  @LibraryOperator(libraries = {SPARK})
  public static final SqlFunction LOG1P =
      SqlBasicFunction.create("LOG1P",
          ReturnTypes.DOUBLE_FORCE_NULLABLE,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC);

  @LibraryOperator(libraries = {BIG_QUERY, SPARK})
  public static final SqlFunction POW =
      SqlBasicFunction.create("POW",
          ReturnTypes.DOUBLE_NULLABLE,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The {@code POWER(numeric, numeric)} function.
   *
   * <p>The return type is {@code DECIMAL} if either argument is a
   * {@code DECIMAL}. In all other cases, the return type is a double.
   */
  @LibraryOperator(libraries = { POSTGRESQL })
  public static final SqlFunction POWER_PG =
      SqlBasicFunction.create("POWER",
          ReturnTypes.DECIMAL_OR_DOUBLE_NULLABLE,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.NUMERIC);

  /** The "TRUNC(numeric1 [, integer2])" function. Identical to the standard <code>TRUNCATE</code>
  * function except the return type should be a double if numeric1 is an integer. */
  @LibraryOperator(libraries = {BIG_QUERY})
  public static final SqlFunction TRUNC_BIG_QUERY = SqlStdOperatorTable.TRUNCATE
          .withName("TRUNC")
          .withReturnTypeInference(ReturnTypes.ARG0_EXCEPT_INTEGER_NULLABLE);

  /** Infix "::" cast operator used by PostgreSQL, for example
   * {@code '100'::INTEGER}. */
  @LibraryOperator(libraries = { POSTGRESQL })
  public static final SqlOperator INFIX_CAST =
      new SqlCastOperator();

  /** NULL-safe "&lt;=&gt;" equal operator used by MySQL, for example
   * {@code 1<=>NULL}. */
  @LibraryOperator(libraries = { MYSQL })
  public static final SqlOperator NULL_SAFE_EQUAL =
      new SqlBinaryOperator(
          "<=>",
          SqlKind.IS_NOT_DISTINCT_FROM,
          30,
          true,
          ReturnTypes.BOOLEAN,
          InferTypes.FIRST_KNOWN,
          OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED);
}
