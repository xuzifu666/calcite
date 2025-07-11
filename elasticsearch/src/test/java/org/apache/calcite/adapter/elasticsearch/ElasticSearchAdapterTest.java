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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ElasticsearchChecker;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.TestUtil;

import org.apache.http.HttpHost;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static java.util.Objects.requireNonNull;

/**
 * Set of tests for ES adapter. Uses real instance via {@link EmbeddedElasticsearchPolicy}. Document
 * source is local {@code zips-mini.json} file (located in test classpath).
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
class ElasticSearchAdapterTest {

  public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

  /** Default index/type name. */
  private static final String ZIPS = "zips";
  private static final String ZIPS_ALIAS = "zips_alias";
  private static final int ZIPS_SIZE = 149;

  /**
   * Used to create {@code zips} index and insert zip data in bulk.
   *
   * @throws Exception when instance setup failed
   */
  @BeforeAll
  public static void setupInstance() throws Exception {
    final Map<String, String> mapping =
        ImmutableMap.of("city", "keyword", "state", "keyword", "pop", "long");

    NODE.createIndex(ZIPS, mapping);
    NODE.createAlias(ZIPS, ZIPS_ALIAS);

    // load records from file
    final List<ObjectNode> bulk = new ArrayList<>();
    final URL url =
        requireNonNull(
            ElasticSearchAdapterTest.class.getResource("/zips-mini.json"),
            "url");
    Resources.readLines(url,
        StandardCharsets.UTF_8, new LineProcessor<Void>() {
          @Override public boolean processLine(String line) throws IOException {
            line = line.replace("_id", "id"); // _id is a reserved attribute in ES
            bulk.add((ObjectNode) NODE.mapper().readTree(line));
            return true;
          }

          @Override public Void getResult() {
            return null;
          }
        });

    if (bulk.isEmpty()) {
      throw new IllegalStateException("No records to index. Empty file ?");
    }

    NODE.insertBulk(ZIPS, bulk);
  }

  private static Connection createConnection() throws SQLException {
    final Connection connection =
        DriverManager.getConnection("jdbc:calcite:lex=JAVA");
    final SchemaPlus root =
        connection.unwrap(CalciteConnection.class).getRootSchema();

    root.add("elastic",
        new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), null));

    // add calcite view programmatically
    final String viewSql = "select cast(_MAP['city'] AS varchar(20)) AS \"city\", "
        + " cast(_MAP['loc'][0] AS float) AS \"longitude\",\n"
        + " cast(_MAP['loc'][1] AS float) AS \"latitude\",\n"
        + " cast(_MAP['pop'] AS integer) AS \"pop\", "
        + " cast(_MAP['state'] AS varchar(2)) AS \"state\", "
        + " cast(_MAP['id'] AS varchar(5)) AS \"id\" "
        + "from \"elastic\".\"zips\"";

    root.add("zips",
        ViewTable.viewMacro(root, viewSql,
            Collections.singletonList("elastic"),
            Arrays.asList("elastic", "view"), false));

    return connection;
  }

  private CalciteAssert.AssertThat calciteAssert() {
    return CalciteAssert.that()
        .with(ElasticSearchAdapterTest::createConnection);
  }

  /** Tests using a Calcite view. */
  @Test void view() {
    calciteAssert()
        .query("select * from zips where city = 'BROOKLYN'")
        .returns("city=BROOKLYN; longitude=-73.956985; latitude=40.646694; "
            + "pop=111396; state=NY; id=11226\n")
        .returnsCount(1);
  }

  @Test void emptyResult() {
    calciteAssert()
        .query("select * from zips limit 0")
        .returnsCount(0);

    calciteAssert()
        .query("select * from elastic.zips where _MAP['Foo'] = '_MISSING_'")
        .returnsCount(0);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6386">[CALCITE-6386]
   * NPE when using ES adapter with model.json and no specified username, password
   * or pathPrefix</a>. */
  @Test void testConnectNoSpecifiedUserOrpathPrefix() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
    final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    final ElasticsearchSchemaFactory esSchemaFactory = new ElasticsearchSchemaFactory();

    Map<String, Object> options = new HashMap<>();
    String coordinates = String.format(Locale.ROOT, "[\"%s\"]", NODE.httpHost());
    options.put("hosts", coordinates);

    final Schema esSchmea =
        esSchemaFactory.create(calciteConnection.getRootSchema(), "elasticsearch", options);
    assertNotNull(esSchmea);
  }

  @Test void testDisableSSL() throws SQLException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:lex=JAVA");
    final SchemaPlus root =
        connection.unwrap(CalciteConnection.class).getRootSchema();

    final CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);

    final ElasticsearchSchemaFactory esSchemaFactory = new ElasticsearchSchemaFactory();
    Map<String, Object> options = new HashMap<>();
    String hosts = "[\"" + NODE.restClient().getNodes()
        .get(0).getHost().toString() + "\"]";
    options.put("username", "user1");
    options.put("password", "password");
    options.put("pathPrefix", "");
    options.put("disableSSLVerification", "true");
    options.put("hosts", hosts);

    final Schema esSchmea =
        esSchemaFactory.create(calciteConnection.getRootSchema(), "es_no_ssl", options);

    assertNotNull(esSchmea);
  }

  /** Test for <a href="https://issues.apache.org/jira/browse/CALCITE-7068">[CALCITE-7068]
   *  ElasticSearch adapter support LIKE operator</a>. */
  @Test void basic() {
    calciteAssert()
        // by default elastic returns max 10 records
        .query("select * from elastic.zips")
        .runs();

    calciteAssert()
        .query("select * from elastic.zips where _MAP['city'] = 'BROOKLYN'")
        .returnsCount(1);

    calciteAssert()
        .query("select * from elastic.zips where"
            + " _MAP['city'] in ('BROOKLYN', 'WASHINGTON')")
        .returnsCount(2);

    // lower-case
    calciteAssert()
        .query("select * from elastic.zips where "
            + "_MAP['city'] in ('brooklyn', 'Brooklyn', 'BROOK') ")
        .returnsCount(0);

    // missing field
    calciteAssert()
        .query("select * from elastic.zips where _MAP['CITY'] = 'BROOKLYN'")
        .returnsCount(0);

    // limit 0
    calciteAssert()
        .query("select * from elastic.zips limit 0")
        .returnsCount(0);

    // test with ESCAPE
    calciteAssert()
        // this case would covnert to like 'BRO%IK*', match no one, count is 0
        .query(
            "select * from elastic.zips where _MAP['city'] like 'BRO\\%OK%' ESCAPE '\\' limit 10")
        .returnsOrdered("")
        .returnsCount(0);

    calciteAssert()
        // this case would covnert to like 'BRO*IK*', match one result, count is 1
        .query(
            "select * from elastic.zips where _MAP['city'] like 'B\\R\\O%OK%' ESCAPE '\\' limit 10")
        .returnsOrdered(

            "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}")
        .returnsCount(1);


    calciteAssert()
        // this case would covnert to like 'BROIK*', match one result, count is 1
        .query(
            "select * from elastic.zips where _MAP['city'] like 'BRO!OK%' ESCAPE '!' limit 10")
        .returnsOrdered(

            "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}")
        .returnsCount(1);

    calciteAssert()
        // this case would covnert to like 'BR!OIK*', match no one, count is 0
        .query(
            "select * from elastic.zips where _MAP['city'] like 'BRO!!OK%' ESCAPE '!' limit 10")
        .returnsOrdered("")
        .returnsCount(0);

    // test with %
    calciteAssert()
        .query("select * from elastic.zips where _MAP['city'] like 'BROOK%' limit 10")
        .returnsOrdered(
            "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}")
        .returnsCount(1);

    calciteAssert()
        .query("select * from elastic.zips where _MAP['city'] like '%ROOK%' limit 10")
        .returnsOrdered(
            "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}")
        .returnsCount(1);

    // test with _
    calciteAssert()
        .query("select * from elastic.zips where _MAP['city'] like 'BROOKLY_' limit 10")
        .returnsOrdered(
            "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}")
        .returnsCount(1);

    calciteAssert()
        .query("select * from elastic.zips where _MAP['city'] like 'BROOKL_' limit 10")
        .returnsOrdered("")
        .returnsCount(0);

    calciteAssert()
        .query("select * from elastic.zips where _MAP['city'] like 'BROOKL__' limit 10")
        .returnsOrdered(
            "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}")
        .returnsCount(1);

    calciteAssert()
        .query("select * from elastic.zips where _MAP['city'] like '_ROOKLY_' limit 10")
        .returnsOrdered(
            "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}")
        .returnsCount(1);

    calciteAssert()
        .query("select * from elastic.zips where _MAP['city'] like '_ROO*Y_' limit 10")
        .returnsOrdered(
            "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}")
        .returnsCount(1);
  }


  /**
   * A test for ReplaceWildcard with escape.
   */
  @Test void testReplaceWildcard() {
    HashMap<String, String> kv = new HashMap<>();
    kv.put("%", "*");
    kv.put("_", "?");

    String value = "aa\\%b%";
    String source = "%";
    String target = "*";
    String escape = "\\";
    assertEquals(QueryBuilders.RegexpQueryBuilder.replaceWildcard(value, kv, escape),
        "aa%b*");

    value = "aa\\\\%b%";
    source = "%";
    target = "*";
    escape = "\\";
    assertEquals(QueryBuilders.RegexpQueryBuilder.replaceWildcard(value, kv, escape),
        "aa\\*b*");

    value = "aa\\\\\\%b%";
    source = "%";
    target = "*";
    escape = "\\";
    assertEquals(QueryBuilders.RegexpQueryBuilder.replaceWildcard(value, kv, escape),
        "aa\\%b*");

    value = "aa!%b%";
    source = "%";
    target = "*";
    escape = "!";
    assertEquals(QueryBuilders.RegexpQueryBuilder.replaceWildcard(value, kv, escape),
        "aa%b*");

    value = "aa!!%b%";
    source = "%";
    target = "*";
    escape = "!";
    assertEquals(QueryBuilders.RegexpQueryBuilder.replaceWildcard(value, kv, escape),
        "aa!*b*");

    value = "aa!!!!%b%";
    source = "%";
    target = "*";
    escape = "!";
    assertEquals(QueryBuilders.RegexpQueryBuilder.replaceWildcard(value, kv, escape),
        "aa!!*b*");

    value = "aa!!!%b%";
    source = "%";
    target = "*";
    escape = "!";
    assertEquals(QueryBuilders.RegexpQueryBuilder.replaceWildcard(value, kv, escape),
        "aa!%b*");
  }

  @Test void testAlias() {
    calciteAssert()
        .query("select * from elastic.zips_alias")
        .returnsCount(ZIPS_SIZE);
  }

  @Test void testSort() {
    final String explain = "PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchSort(sort0=[$4], dir0=[ASC])\n"
        + "    ElasticsearchProject(city=[CAST(ITEM($0, 'city')):VARCHAR(20)], longitude=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], latitude=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], pop=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2)], id=[CAST(ITEM($0, 'id')):VARCHAR(5)])\n"
        + "      ElasticsearchTableScan(table=[[elastic, zips]])";

    calciteAssert()
        .query("select * from zips order by state")
        .returnsCount(ZIPS_SIZE)
        .returns(sortedResultSetChecker("state", RelFieldCollation.Direction.ASCENDING))
        .explainContains(explain);
  }

  @Test void testSortLimit() {
    final String sql = "select state, pop from zips\n"
        + "order by state, pop offset 2 rows fetch next 3 rows only";
    calciteAssert()
        .query(sql)
        .returnsUnordered("state=AK; pop=32383",
            "state=AL; pop=42124",
            "state=AL; pop=43862")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "'_source' : ['state', 'pop']",
                "sort: [ {state: {'missing':'_last', 'order':'asc'}}, "
                    + "{pop: {'missing':'_last', 'order':'asc'}}]",
                "from: 2",
                "size: 3"));
  }

  /**
   * Throws {@code AssertionError} if result set is not sorted by {@code column}.
   * {@code null}s are ignored.
   *
   * @param column column to be extracted (as comparable object).
   * @param direction ascending / descending
   * @return consumer which throws exception
   */
  private static Consumer<ResultSet> sortedResultSetChecker(String column,
      RelFieldCollation.Direction direction) {
    requireNonNull(column, "column");
    return rset -> {
      try {
        final List<Comparable<?>> states = new ArrayList<>();
        while (rset.next()) {
          Object object = rset.getObject(column);
          if (object != null && !(object instanceof Comparable)) {
            final String message = String.format(Locale.ROOT, "%s is not comparable", object);
            throw new IllegalStateException(message);
          }
          if (object != null) {
            //noinspection rawtypes
            states.add((Comparable) object);
          }
        }
        for (int i = 0; i < states.size() - 1; i++) {
          //noinspection rawtypes
          final Comparable current = states.get(i);
          //noinspection rawtypes
          final Comparable next = states.get(i + 1);
          //noinspection unchecked
          final int cmp = current.compareTo(next);
          if (direction == RelFieldCollation.Direction.ASCENDING ? cmp > 0 : cmp < 0) {
            final String message =
                String.format(Locale.ROOT,
                    "Column %s NOT sorted (%s): %s (index:%d) > %s (index:%d) count: %d",
                    column, direction, current, i, next, i + 1, states.size());
            throw new AssertionError(message);
          }
        }
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  /**
   * Sorting (and aggregating) directly on items without a view.
   *
   * <p>Queries of type:
   * {@code select _MAP['a'] from elastic order by _MAP['b']}
   */
  @Test void testSortNoSchema() {
    calciteAssert()
        .query("select * from elastic.zips order by _MAP['city']")
        .returnsCount(ZIPS_SIZE);

    calciteAssert()
        .query("select * from elastic.zips where _MAP['state'] = 'NY' order by _MAP['city']")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
            "query:{'constant_score':{filter:{term:{state:'NY'}}}}",
            "sort:[{city:{'missing':'_last', 'order':'asc'}}]",
            String.format(Locale.ROOT, "size:%s", ElasticsearchTransport.DEFAULT_FETCH_SIZE)))
        .returnsOrdered(
          "_MAP={id=11226, city=BROOKLYN, loc=[-73.956985, 40.646694], pop=111396, state=NY}",
          "_MAP={id=11373, city=JACKSON HEIGHTS, loc=[-73.878551, 40.740388], pop=88241, state=NY}",
          "_MAP={id=10021, city=NEW YORK, loc=[-73.958805, 40.768476], pop=106564, state=NY}");

    calciteAssert()
        .query("select _MAP['state'] from elastic.zips order by _MAP['city']")
        .returnsCount(ZIPS_SIZE);

    calciteAssert()
        .query("select * from elastic.zips where _MAP['state'] = 'NY' or "
            + "_MAP['city'] = 'BROOKLYN'"
            + " order by _MAP['city']")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "query:{'dis_max':{'queries':[{'bool':{'should':"
                    + "[{'term':{'state':'NY'}},{'term':"
                    + "{'city':'BROOKLYN'}}]}}]}},'sort':[{'city':{'missing':'_last', 'order':'asc'}}]",
                String.format(Locale.ROOT, "size:%s",
                    ElasticsearchTransport.DEFAULT_FETCH_SIZE)));

    calciteAssert()
        .query("select _MAP['city'] from elastic.zips where _MAP['state'] = 'NY' "
            + "order by _MAP['city']")
        .returnsOrdered("EXPR$0=BROOKLYN",
            "EXPR$0=JACKSON HEIGHTS",
            "EXPR$0=NEW YORK");

    calciteAssert()
        .query("select _MAP['city'] as city, _MAP['state'] from elastic.zips "
            + "order by _MAP['city'] asc")
        .returns(sortedResultSetChecker("city", RelFieldCollation.Direction.ASCENDING))
        .returnsCount(ZIPS_SIZE);

    calciteAssert()
        .query("select _MAP['city'] as city, _MAP['state'] from elastic.zips "
            + "order by _MAP['city'] desc")
        .returns(sortedResultSetChecker("city", RelFieldCollation.Direction.DESCENDING))
        .returnsCount(ZIPS_SIZE);

    calciteAssert()
        .query("select max(_MAP['pop']), min(_MAP['pop']), _MAP['state'] from elastic.zips "
            + "group by _MAP['state'] order by _MAP['state'] limit 3")
        .returnsOrdered("EXPR$0=32383.0; EXPR$1=23238.0; EXPR$2=AK",
             "EXPR$0=44165.0; EXPR$1=42124.0; EXPR$2=AL",
             "EXPR$0=53532.0; EXPR$1=37428.0; EXPR$2=AR");

    calciteAssert()
        .query("select max(_MAP['pop']), min(_MAP['pop']), _MAP['state'] from elastic.zips "
            + "where _MAP['state'] = 'NY' group by _MAP['state'] order by _MAP['state'] limit 3")
        .returns("EXPR$0=111396.0; EXPR$1=88241.0; EXPR$2=NY\n");
  }

  /** Tests sorting by multiple fields (in different direction: asc/desc). */
  @Test void sortAscDesc() {
    final String sql = "select city, state, pop from zips\n"
        + "order by pop desc, state asc, city desc limit 3";
    calciteAssert()
        .query(sql)
        .returnsOrdered("city=CHICAGO; state=IL; pop=112047",
             "city=BROOKLYN; state=NY; pop=111396",
             "city=NEW YORK; state=NY; pop=106564")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "'_source':['city','state','pop']",
                "sort:[{pop:{'missing':'_first', 'order':'desc'}}, "
                    + "{state:{'missing':'_last', 'order':'asc'}}, "
                    + "{city:{'missing':'_first', 'order':'desc'}}]",
                "size:3"));
  }

  @Test void testOffsetLimit() {
    final String sql = "select state, id from zips\n"
        + "offset 2 fetch next 3 rows only";
    calciteAssert()
        .query(sql)
        .runs()
        .returnsCount(3)
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "_source : ['state', 'id']",
                "from: 2",
                "size: 3"));
  }

  @Test void testLimit() {
    final String sql = "select state, id from zips\n"
        + "fetch next 3 rows only";

    calciteAssert()
        .query(sql)
        .runs()
        .returnsCount(3)
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "'_source':['state','id']",
                "size:3"));
  }

  @Test void limit2() {
    final String sql = "select id from zips limit 5";
    calciteAssert()
        .query(sql)
        .runs()
        .returnsCount(5)
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker(
                "'_source':['id']",
                "size:5"));
  }

  @Test void testFilterSort() {
    final String sql = "select * from zips\n"
        + "where state = 'CA' and pop >= 94000\n"
        + "order by state, pop";
    final String explain = "PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchSort(sort0=[$4], sort1=[$3], dir0=[ASC], dir1=[ASC])\n"
        + "    ElasticsearchProject(city=[CAST(ITEM($0, 'city')):VARCHAR(20)], longitude=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], latitude=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], pop=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2)], id=[CAST(ITEM($0, 'id')):VARCHAR(5)])\n"
        + "      ElasticsearchFilter(condition=[AND(=(CAST(CAST(ITEM($0, 'state')):VARCHAR(2)):CHAR(2), 'CA'), >=(CAST(ITEM($0, 'pop')):INTEGER, 94000))])\n"
        + "        ElasticsearchTableScan(table=[[elastic, zips]])\n\n";
    calciteAssert()
        .query(sql)
        .returnsOrdered("city=NORWALK; longitude=-118.081767; latitude=33.90564;"
                + " pop=94188; state=CA; id=90650",
            "city=LOS ANGELES; longitude=-118.258189; latitude=34.007856;"
                + " pop=96074; state=CA; id=90011",
            "city=BELL GARDENS; longitude=-118.17205; latitude=33.969177;"
                + " pop=99568; state=CA; id=90201")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'query' : "
                    + "{'constant_score':{filter:{bool:"
                    + "{must:[{term:{state:'CA'}},"
                    + "{range:{pop:{gte:94000}}}]}}}}",
                "'script_fields': {longitude:{script:'params._source.loc[0]'}, "
                    + "latitude:{script:'params._source.loc[1]'}, "
                    + "city:{script: 'params._source.city'}, "
                    + "pop:{script: 'params._source.pop'}, "
                    + "state:{script: 'params._source.state'}, "
                    + "id:{script: 'params._source.id'}}",
                "sort: [ {state: {'missing':'_last', 'order':'asc'}}, "
                    + "{pop: {'missing':'_last', 'order':'asc'}}]",
                String.format(Locale.ROOT, "size:%s", ElasticsearchTransport.DEFAULT_FETCH_SIZE)))
        .explainContains(explain);
  }

  @Test void testDismaxQuery() {
    final String sql = "select * from zips\n"
        + "where state = 'CA' or pop >= 94000\n"
        + "order by state, pop";
    final String explain = "PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchSort(sort0=[$4], sort1=[$3], dir0=[ASC], dir1=[ASC])\n"
        + "    ElasticsearchProject(city=[CAST(ITEM($0, 'city')):VARCHAR(20)], longitude=[CAST(ITEM(ITEM($0, 'loc'), 0)):FLOAT], latitude=[CAST(ITEM(ITEM($0, 'loc'), 1)):FLOAT], pop=[CAST(ITEM($0, 'pop')):INTEGER], state=[CAST(ITEM($0, 'state')):VARCHAR(2)], id=[CAST(ITEM($0, 'id')):VARCHAR(5)])\n"
        + "      ElasticsearchFilter(condition=[OR(=(CAST(CAST(ITEM($0, 'state')):VARCHAR(2)):CHAR(2), 'CA'), >=(CAST(ITEM($0, 'pop')):INTEGER, 94000))])\n"
        + "        ElasticsearchTableScan(table=[[elastic, zips]])\n\n";
    calciteAssert()
        .query(sql)
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'query' : "
                    + "{'dis_max':{'queries':[{bool:"
                    + "{should:[{term:{state:'CA'}},"
                    + "{range:{pop:{gte:94000}}}]}}]}}",
                "'script_fields': {longitude:{script:'params._source.loc[0]'}, "
                    + "latitude:{script:'params._source.loc[1]'}, "
                    + "city:{script: 'params._source.city'}, "
                    + "pop:{script: 'params._source.pop'}, "
                    + "state:{script: 'params._source.state'}, "
                    + "id:{script: 'params._source.id'}}",
                "sort: [ {state: {'missing':'_last', 'order':'asc'}}, "
                    + "{pop: {'missing':'_last', 'order':'asc'}}]",
                String.format(Locale.ROOT, "size:%s",
                    ElasticsearchTransport.DEFAULT_FETCH_SIZE)))
        .explainContains(explain);
  }

  @Test void testFilterSortDesc() {
    Assumptions.assumeTrue(Bug.CALCITE_4645_FIXED, "CALCITE-4645");
    final String sql = "select * from zips\n"
        + "where pop BETWEEN 95000 AND 100000\n"
        + "order by state desc, pop";
    calciteAssert()
        .query(sql)
        .limit(4)
        .returnsOrdered(
            "city=LOS ANGELES; longitude=-118.258189; latitude=34.007856; pop=96074; state=CA; id=90011",
            "city=BELL GARDENS; longitude=-118.17205; latitude=33.969177; pop=99568; state=CA; id=90201");
  }

  @Test void testInPlan() {
    final String[] searches = {
        "query: {'constant_score':{filter:{terms:{pop:"
            + "[96074, 99568]}}}}",
        "script_fields: {longitude:{script:'params._source.loc[0]'}, "
            +  "latitude:{script:'params._source.loc[1]'}, "
            +  "city:{script: 'params._source.city'}, "
            +  "pop:{script: 'params._source.pop'}, "
            +  "state:{script: 'params._source.state'}, "
            +  "id:{script: 'params._source.id'}}",
        String.format(Locale.ROOT, "size:%d", ElasticsearchTransport.DEFAULT_FETCH_SIZE)
    };

    calciteAssert()
        .query("select * from zips where pop in (96074, 99568)")
        .returnsUnordered(
            "city=BELL GARDENS; longitude=-118.17205; latitude=33.969177; pop=99568; state=CA; id=90201",
            "city=LOS ANGELES; longitude=-118.258189; latitude=34.007856; pop=96074; state=CA; id=90011")
        .queryContains(ElasticsearchChecker.elasticsearchChecker(searches));
  }

  @Test void testZips() {
    calciteAssert()
        .query("select state, city from zips")
        .returnsCount(ZIPS_SIZE);
  }

  @Test void testProject() {
    final String sql = "select state, city, 0 as zero\n"
        + "from zips\n"
        + "order by state, city";

    calciteAssert()
        .query(sql)
        .limit(2)
        .returnsUnordered("state=AK; city=ANCHORAGE; zero=0",
            "state=AK; city=FAIRBANKS; zero=0")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("script_fields:"
                    + "{zero:{script:'0'},"
                    + "state:{script:'params._source.state'},"
                    + "city:{script:'params._source.city'}}",
                "sort:[{state:{'missing':'_last', 'order':'asc'}},"
                    + "{city:{'missing':'_last', 'order':'asc'}}]",
                String.format(Locale.ROOT, "size:%d", ElasticsearchTransport.DEFAULT_FETCH_SIZE)));
  }

  @Test void testFilter() {
    final String explain = "PLAN=ElasticsearchToEnumerableConverter\n"
        + "  ElasticsearchProject(state=[CAST(ITEM($0, 'state')):VARCHAR(2)], city=[CAST(ITEM($0, 'city')):VARCHAR(20)])\n"
        + "    ElasticsearchFilter(condition=[=(CAST(CAST(ITEM($0, 'state')):VARCHAR(2)):CHAR(2), 'CA')])\n"
        + "      ElasticsearchTableScan(table=[[elastic, zips]])";

    calciteAssert()
        .query("select state, city from zips where state = 'CA'")
        .limit(3)
        .returnsUnordered("state=CA; city=BELL GARDENS",
            "state=CA; city=LOS ANGELES",
            "state=CA; city=NORWALK")
        .explainContains(explain);
  }

  @Test void testFilterReversed() {
    calciteAssert()
        .query("select state, city from zips where 'WI' < state order by city")
        .limit(2)
        .returnsUnordered("state=WV; city=BECKLEY",
            "state=WY; city=CHEYENNE");
    calciteAssert()
        .query("select state, city from zips where state > 'WI' order by city")
        .limit(2)
        .returnsUnordered("state=WV; city=BECKLEY",
            "state=WY; city=CHEYENNE");
  }

  @Test void agg1() {
    calciteAssert()
        .query("select count(*) from zips")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
            "size:0", "'stored_fields': '_none_'", "track_total_hits:true"))
        .returns("EXPR$0=149\n");

    // check with limit (should still return correct result).
    calciteAssert()
        .query("select count(*) from zips limit 1")
        .returns("EXPR$0=149\n");

    calciteAssert()
        .query("select count(*) as cnt from zips")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
            "'stored_fields': '_none_'",
            "size:0", "track_total_hits:true"))
        .returns("cnt=149\n");

    calciteAssert()
        .query("select min(pop), max(pop) from zips")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
            "size:0",
            "track_total_hits:true",
            "'stored_fields': '_none_'",
            "aggregations:{'EXPR$0':{min:{field:'pop'}},'EXPR$1':{max:"
                + "{field:'pop'}}}"))
        .returns("EXPR$0=21; EXPR$1=112047\n");

    calciteAssert()
        .query("select min(pop) as min1, max(pop) as max1 from zips")
        .returns("min1=21; max1=112047\n");

    calciteAssert()
        .query("select count(*), max(pop), min(pop), sum(pop), avg(pop) from zips")
        .returns("EXPR$0=149; EXPR$1=112047; EXPR$2=21; EXPR$3=7865489; EXPR$4=52788\n");
  }

  @Test void groupBy() {
    // distinct
    calciteAssert()
        .query("select distinct state\n"
            + "from zips\n"
            + "limit 6")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("_source:false",
                "size:0", "'stored_fields': '_none_'",
                "aggregations:{'g_state':{'terms':{'field':'state','missing':'__MISSING__', 'size' : 6}}}"))
        .returnsOrdered("state=AK",
            "state=AL",
            "state=AR",
            "state=AZ",
            "state=CA",
            "state=CO");

    // without aggregate function
    calciteAssert()
        .query("select state, city\n"
            + "from zips\n"
            + "group by state, city\n"
            + "order by city limit 10")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
                "size:0", "'stored_fields': '_none_'",
                "aggregations:{'g_city':{'terms':{'field':'city','missing':'__MISSING__','size':10,'order':{'_key':'asc'}}",
                "aggregations:{'g_state':{'terms':{'field':'state','missing':'__MISSING__','size':10}}}}}}"))
        .returnsOrdered("state=SD; city=ABERDEEN",
            "state=SC; city=AIKEN",
            "state=TX; city=ALTON",
            "state=IA; city=AMES",
            "state=AK; city=ANCHORAGE",
            "state=MD; city=BALTIMORE",
            "state=ME; city=BANGOR",
            "state=KS; city=BAVARIA",
            "state=NJ; city=BAYONNE",
            "state=OR; city=BEAVERTON");

    // ascending
    calciteAssert()
        .query("select min(pop), max(pop), state\n"
            + "from zips\n"
            + "group by state\n"
            + "order by state limit 3")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
                "size:0", "'stored_fields': '_none_'",
                "aggregations:{'g_state':{terms:{field:'state',missing:'__MISSING__',size:3,"
                    + " order:{'_key':'asc'}}",
                "aggregations:{'EXPR$0':{min:{field:'pop'}},'EXPR$1':{max:{field:'pop'}}}}}"))
        .returnsOrdered("EXPR$0=23238; EXPR$1=32383; state=AK",
            "EXPR$0=42124; EXPR$1=44165; state=AL",
            "EXPR$0=37428; EXPR$1=53532; state=AR");

    // just one aggregation function
    calciteAssert()
        .query("select min(pop), state\n"
            + "from zips\n"
            + "group by state\n"
            + "order by state limit 3")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
                "size:0",
                "'stored_fields': '_none_'",
                "aggregations:{'g_state':{terms:{field:'state',missing:'__MISSING__',"
                    + "size:3, order:{'_key':'asc'}}",
                "aggregations:{'EXPR$0':{min:{field:'pop'}} }}}"))
        .returnsOrdered("EXPR$0=23238; state=AK",
            "EXPR$0=42124; state=AL",
            "EXPR$0=37428; state=AR");

    // group by count
    calciteAssert()
        .query("select count(city), state\n"
            + "from zips\n"
            + "group by state\n"
            + "order by state limit 3")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
                "size:0",
                "'stored_fields': '_none_'",
                "aggregations:{'g_state':{terms:{field:'state',missing:'__MISSING__',"
                    + " size:3, order:{'_key':'asc'}}",
                "aggregations:{'EXPR$0':{'value_count':{field:'city'}} }}}"))
        .returnsOrdered("EXPR$0=3; state=AK",
            "EXPR$0=3; state=AL",
            "EXPR$0=3; state=AR");

    // descending
    calciteAssert()
        .query("select min(pop), max(pop), state\n"
            + "from zips\n"
            + "group by state\n"
            + "order by state desc limit 3")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
                "size:0",
                "'stored_fields': '_none_'",
                "aggregations:{'g_state':{terms:{field:'state',missing:'__MISSING__',"
                    + "size:3, order:{'_key':'desc'}}",
                "aggregations:{'EXPR$0':{min:{field:'pop'}},'EXPR$1':"
                    + "{max:{field:'pop'}}}}}"))
        .returnsOrdered("EXPR$0=25968; EXPR$1=33107; state=WY",
            "EXPR$0=45196; EXPR$1=70185; state=WV",
            "EXPR$0=51008; EXPR$1=57187; state=WI");
  }

  /** Tests the {@code NOT} operator. */
  @Test void notOperator() {
    // largest zips (states) in mini-zip by pop (sorted) : IL, NY, CA, MI
    calciteAssert()
        .query("select count(*), max(pop) from zips where state not in ('IL')")
        .returns("EXPR$0=146; EXPR$1=111396\n");

    calciteAssert()
        .query("select count(*), max(pop) from zips where not state in ('IL')")
        .returns("EXPR$0=146; EXPR$1=111396\n");

    calciteAssert()
        .query("select count(*), max(pop) from zips where not state not in ('IL')")
        .returns("EXPR$0=3; EXPR$1=112047\n");

    calciteAssert()
        .query("select count(*), max(pop) from zips where state not in ('IL', 'NY')")
        .returns("EXPR$0=143; EXPR$1=99568\n");

    calciteAssert()
        .query("select count(*), max(pop) from zips where state not in ('IL', 'NY', 'CA')")
        .returns("EXPR$0=140; EXPR$1=84712\n");

  }

  /**
   * Test of {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#APPROX_COUNT_DISTINCT} which
   * will be translated to
   * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html">Cardinality Aggregation</a>
   * (approximate counts using HyperLogLog++ algorithm).
   */
  @Test void approximateCount() {
    calciteAssert()
        .query("select state, approx_count_distinct(city), approx_count_distinct(pop) from zips"
            + " group by state order by state limit 3")
        .queryContains(
            ElasticsearchChecker.elasticsearchChecker("'_source':false",
            "size:0", "'stored_fields': '_none_'",
            "aggregations:{'g_state':{terms:{field:'state', missing:'__MISSING__', size:3, "
                + "order:{'_key':'asc'}}",
            "aggregations:{'EXPR$1':{cardinality:{field:'city'}}",
                "'EXPR$2':{cardinality:{field:'pop'}} "
                + " }}}"))
        .returnsOrdered("state=AK; EXPR$1=3; EXPR$2=3",
            "state=AL; EXPR$1=3; EXPR$2=3",
            "state=AR; EXPR$1=3; EXPR$2=3");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6725">[CALCITE-6725]
   * The caching mechanism key in ElasticsearchSchemaFactory is affected by the order of hosts</a>.
   */
  @Test void testSortHosts() {
    HttpHost host1 = HttpHost.create("192.168.1.200:8080");
    HttpHost host2 = HttpHost.create("192.168.1.100:8080");
    HttpHost host3 = HttpHost.create("192.168.1.150:8080");
    List<HttpHost> hosts = Arrays.asList(host1, host2, host3);
    List<HttpHost> sortedHosts = ElasticsearchSchemaFactory.getSortedHost(hosts);
    assertEquals(3, sortedHosts.size());
    assertEquals("http://192.168.1.100:8080", sortedHosts.get(0).toString());
    assertEquals("http://192.168.1.150:8080", sortedHosts.get(1).toString());
    assertEquals("http://192.168.1.200:8080", sortedHosts.get(2).toString());
  }

}
