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
package org.apache.calcite.sql.validate;

import org.apache.calcite.test.SqlValidatorFixture;
import org.apache.calcite.test.SqlValidatorTestCase;

import org.junit.jupiter.api.Test;

/**
 * Tests to verify that aggregate functions without explicit aliases
 * use default behavior when normalizeAggregateColumnNames config is
 * disabled (the default).
 */
public class AggregateColumnNameTest extends SqlValidatorTestCase {

  @Test public void testSingleAggregateWithoutAlias() {
    // Default behavior: aggregate without alias gets synthesized name
    sql("SELECT COUNT(*) FROM emp").type("RecordType(BIGINT NOT NULL EXPR$0) NOT NULL");
  }

  @Test public void testMultipleAggregatesWithoutAliases() {
    // Default behavior: multiple aggregates get EXPR$0, EXPR$1, etc.
    sql("SELECT COUNT(*), COUNT(*), MAX(empno) FROM emp")
        .type("RecordType(BIGINT NOT NULL EXPR$0, BIGINT NOT NULL EXPR$1, INTEGER EXPR$2) NOT NULL");
  }

  @Test public void testExplicitAlias() {
    // Explicit aliases are always preserved
    sql("SELECT COUNT(*) AS cnt FROM emp")
        .type("RecordType(BIGINT NOT NULL CNT) NOT NULL");
  }
}

/**
 * Tests to verify that aggregate functions without explicit aliases
 * are assigned standardized column names when
 * normalizeAggregateColumnNames config is enabled.
 */
class AggregateColumnNameNormalizedTest extends SqlValidatorTestCase {

  @Override public SqlValidatorFixture fixture() {
    return super.fixture()
        .withValidatorConfig(c -> c.withNormalizeAggregateColumnNames(true));
  }

  @Test public void testSingleAggregateWithoutAlias() {
    // Single aggregate function without alias should get EXPR$0 as column name
    sql("SELECT COUNT(*) FROM emp").type("RecordType(BIGINT NOT NULL EXPR$0) NOT NULL");
  }

  @Test public void testMultipleAggregatesWithoutAliases() {
    // Multiple aggregate functions without aliases should get EXPR$0, EXPR$1, etc.
    sql("SELECT COUNT(*), COUNT(*), MAX(empno) FROM emp")
        .type("RecordType(BIGINT NOT NULL EXPR$0, BIGINT NOT NULL EXPR$1, INTEGER EXPR$2) NOT NULL");
  }

  @Test public void testMixedExplicitAndImplicitAliases() {
    // Mix of explicit and implicit aliases
    sql("SELECT COUNT(*) AS cnt1, MAX(empno) FROM emp")
        .type("RecordType(BIGINT NOT NULL CNT1, INTEGER EXPR$1) NOT NULL");
  }

  @Test public void testAllExplicitAliases() {
    // All explicit aliases should preserve user-provided names even when config is enabled
    sql("SELECT COUNT(*) AS cnt1, MAX(empno) AS mx FROM emp")
        .type("RecordType(BIGINT NOT NULL CNT1, INTEGER MX) NOT NULL");
  }
}
