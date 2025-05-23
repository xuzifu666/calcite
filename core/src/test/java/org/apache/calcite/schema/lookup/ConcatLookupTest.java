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
package org.apache.calcite.schema.lookup;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test for ConcatLookup.
 */
class ConcatLookupTest {
  private final Lookup<String> testee =
      Lookup.concat(new FakeLookup("a", "1"),
          new FakeLookup("b", "2"),
          new FakeLookup("d", "4"),
          new FakeLookup("d", "5"));

  @Test void testNull() {
    assertThat(testee.get("c"), nullValue());
  }

  @Test void test() {
    assertThat(testee.get("a"), equalTo("1"));
  }

  @Test void testIgnoreCase() {
    assertThat(testee.getIgnoreCase("B"), equalTo(new Named<>("b", "2")));
  }

  @Test void testCommonNames() {
    assertThat(testee.getIgnoreCase("D"), equalTo(new Named<>("d", "4")));
  }
}
