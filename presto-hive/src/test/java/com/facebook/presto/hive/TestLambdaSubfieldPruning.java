/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;

@Test(singleThreaded = true)
public class TestLambdaSubfieldPruning
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(
                ImmutableList.of(TpchTable.LINE_ITEM),
                ImmutableMap.of(
                        "experimental.pushdown-subfields-enabled", "true",
                        "pushdown-subfields-from-lambda-enabled", "true",
                        "experimental.pushdown-dereference-enabled", "true"),
                "sql-standard",
                ImmutableMap.<String, String>builder()
                        .put("hive.pushdown-filter-enabled", "true")
                        .put("hive.parquet.pushdown-filter-enabled", "false") // Parquet does not support selective reader yet.
                        .build(),
                Optional.empty());

        return createLineItemExTable(queryRunner);
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(
                ImmutableList.of(TpchTable.LINE_ITEM),
                ImmutableMap.of(
                        "experimental.pushdown-subfields-enabled", "false",
                        "pushdown-subfields-from-lambda-enabled", "false",
                        "experimental.pushdown-dereference-enabled", "true"),
                "sql-standard",
                ImmutableMap.<String, String>builder()
                        .put("hive.pushdown-filter-enabled", "true")
                        .put("hive.parquet.pushdown-filter-enabled", "false")
                        .build(),
                Optional.empty());

        return createLineItemExTable(queryRunner);
    }

    private static DistributedQueryRunner createLineItemExTable(DistributedQueryRunner queryRunner)
    {
        for (String fileFormatName : ImmutableSet.of("ORC", "DWRF", "PARQUET")) {
            queryRunner.execute(noPushdownFilter(queryRunner.getDefaultSession()),
                    "CREATE TABLE lineitem_ex_" + fileFormatName + " ( \n" +
                            "    array_of_varchar_keys, \n" +
                            "    array_of_rows, \n" +
                            "    array_of_non_null_rows, \n" +
                            "    array_of_array_of_rows, \n" +
                            "    row_with_array_of_rows, \n" +
                            "    row_with_map_varchar_key_row_value, \n" +
                            "    map_varchar_key_row_value, \n" +
                            "    map_varchar_key_array_of_row_value, \n" +
                            "    array_of_map_entries_varchar_key_row_value, \n" +
                            "    END_OF_LIST \n" +
                            ") WITH (format = '" + fileFormatName + "') AS \n" +
                            "SELECT \n" +
                            "   ARRAY['orderkey', 'linenumber', 'partkey'] AS array_of_varchar_keys, \n" +
                            "   IF (orderkey % 49 = 0, NULL, CAST(ARRAY[ \n" +
                            "        ROW(IF (orderkey % 17 = 0, NULL, orderkey),  comment), \n" +
                            "        ROW(IF (linenumber % 7 = 0, NULL, linenumber),  upper(comment)), \n" +
                            "        ROW(IF (partkey % 5 = 0, NULL, partkey),  shipmode)  \n" +
                            "     ] AS ARRAY(ROW(itemdata BIGINT, comment VARCHAR)))) AS array_of_rows, \n" +
                            "   CAST(ARRAY[ \n" +
                            "        ROW(orderkey,  comment), \n" +
                            "        ROW(linenumber,  upper(comment)), \n" +
                            "        ROW(partkey,  shipmode)  \n" +
                            "     ] AS ARRAY(ROW(itemdata BIGINT, comment VARCHAR))) AS array_of_non_null_rows, \n" +
                            "   IF (orderkey % 49 = 0, NULL, CAST(ARRAY[ARRAY[ \n" +
                            "        ROW(IF (orderkey % 17 = 0, NULL, orderkey),  comment), \n" +
                            "        ROW(IF (linenumber % 7 = 0, NULL, linenumber),  upper(comment)), \n" +
                            "        ROW(IF (partkey % 5 = 0, NULL, partkey),  shipmode) \n" +
                            "     ]] AS ARRAY(ARRAY(ROW(itemdata BIGINT, comment VARCHAR))))) AS array_of_array_of_rows, \n" +
                            "   CAST(ROW(ARRAY[ROW(orderkey, comment)]) AS ROW(array_of_rows ARRAY(ROW(itemdata BIGINT, comment VARCHAR)))) row_with_array_of_rows, \n" +
                            "   CAST(ROW(MAP_FROM_ENTRIES(ARRAY[ \n" +
                            "         ROW('orderdata',    ROW(IF (orderkey % 17 = 0, 1, orderkey), linenumber, partkey)), \n" +
                            "         ROW('orderdata_ex', ROW(orderkey + 100, linenumber + 100, partkey + 100))])) \n" +
                            "      AS ROW(map_varchar_key_row_value MAP(VARCHAR, ROW(orderkey BIGINT, linenumber BIGINT, partkey BIGINT)))) row_with_map_varchar_key_row_value, \n" +
                            "   CAST(MAP_FROM_ENTRIES(ARRAY[ \n" +
                            "         ROW('orderdata',    ROW(IF (orderkey % 17 = 0, 1, orderkey), linenumber, partkey)), \n" +
                            "         ROW('orderdata_ex', ROW(orderkey + 100, linenumber + 100, partkey + 100))]) \n" +
                            "      AS MAP(VARCHAR, ROW(orderkey BIGINT, linenumber BIGINT, partkey BIGINT))) AS map_varchar_key_row_value, \n" +
                            "   CAST(MAP_FROM_ENTRIES(ARRAY[ \n" +
                            "         ROW('orderdata',    IF (orderkey % 13 = 0, NULL, ARRAY[ROW( IF (orderkey % 17 = 0, NULL, orderkey), linenumber, partkey)])), \n" +
                            "         ROW('orderdata_ex', ARRAY[ ROW(orderkey + 100, linenumber + 100, partkey + 100)])]) \n" +
                            "      AS MAP(VARCHAR, ARRAY(ROW(orderkey BIGINT, linenumber BIGINT, partkey BIGINT)))) AS map_varchar_key_array_of_row_value, \n" +
                            "   CAST(ARRAY[ \n" +
                            "         ROW('orderdata',     IF (orderkey % 13 = 0, NULL, ROW( IF (orderkey % 17 = 0, NULL, orderkey), linenumber, partkey))), \n" +
                            "         ROW('orderdata_ex',  ROW(orderkey + 100, linenumber + 100, partkey + 100))] \n" +
                            "      AS ARRAY(ROW(key VARCHAR, value ROW(orderkey BIGINT, linenumber BIGINT, partkey BIGINT)))) AS array_of_map_entries_varchar_key_row_value, \n" +
                            "   true AS END_OF_LIST \n" +

                            "FROM lineitem  \n");
        }
        return queryRunner;
    }

    private static Session noPushdownFilter(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "false")
                .build();
    }

    @Test
    public void testPushDownSubfieldsFromLambdas()
    {
        for (String fileFormatName : ImmutableSet.of("ORC", "DWRF", "PARQUET")) {
            testPushDownSubfieldsFromLambdas("lineitem_ex_" + fileFormatName);
        }
    }

    private void testPushDownSubfieldsFromLambdas(String tableName)
    {
        // functions that are not outputting all subfields
        assertQuery("SELECT ALL_MATCH(array_of_rows, x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT ALL_MATCH(array_of_rows, x -> true) FROM " + tableName);
        assertQuery("SELECT ALL_MATCH(array_of_rows, x -> x IS NULL) FROM " + tableName);
        assertQuery("SELECT ANY_MATCH(array_of_rows, x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT NONE_MATCH(array_of_rows, x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(array_of_rows, x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT CARDINALITY(array_of_rows) FROM " + tableName);
        assertQuery("SELECT CARDINALITY(FLATTEN(array_of_array_of_rows)) FROM " + tableName);
        assertQuery("SELECT CARDINALITY(FILTER(array_of_rows, x -> POSITION('T' IN x.comment) > 0)) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(row_with_array_of_rows.array_of_rows, x -> CAST(ROW(x.comment, x.comment) AS ROW(d1 VARCHAR, d2 VARCHAR))) FROM " + tableName);
        assertQuery("SELECT TRANSFORM_VALUES(map_varchar_key_row_value, (k,v) -> v.orderkey) FROM " + tableName);
        assertQuery("SELECT ZIP_WITH(array_of_rows, row_with_array_of_rows.array_of_rows, (x, y) -> CAST(ROW(x.itemdata, y.comment) AS ROW(d1 BIGINT, d2 VARCHAR))) FROM " + tableName);
        assertQuery("SELECT MAP_ZIP_WITH(map_varchar_key_row_value, row_with_map_varchar_key_row_value.map_varchar_key_row_value, (k, v1, v2) -> v1.orderkey + v2.orderkey) FROM " + tableName);

        // functions that outputing all subfields and accept functional parameter
        assertQuery("SELECT FILTER(array_of_rows, x -> POSITION('T' IN x.comment) > 0) FROM " + tableName);
        assertQuery("SELECT ARRAY_SORT(array_of_rows, (l, r) -> IF(l.itemdata < r.itemdata, 1, IF(l.itemdata = r.itemdata, 0, -1))) FROM " + tableName);
        assertQuery("SELECT ANY_MATCH(SLICE(ARRAY_SORT(array_of_rows, (l, r) -> IF(l.itemdata < r.itemdata, 1, IF(l.itemdata = r.itemdata, 0, -1))), 1, 3), x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(ARRAY_SORT(array_of_non_null_rows), x -> x.itemdata) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(COMBINATIONS(array_of_non_null_rows, 3), x -> x[1].itemdata) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(FLATTEN(array_of_array_of_rows), x -> x.itemdata) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(REVERSE(array_of_rows), x -> x.itemdata) FROM " + tableName);
        assertQuery("SELECT ARRAY_SORT(TRANSFORM(SHUFFLE(array_of_non_null_rows), x -> x.itemdata)) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(SLICE(array_of_rows, 1, 5), x -> x.itemdata) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(TRIM_ARRAY(array_of_rows, 2), x -> x.itemdata) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(CONCAT(array_of_rows, row_with_array_of_rows.array_of_rows), x -> x.itemdata) FROM " + tableName);
        assertQuery("SELECT TRANSFORM(array_of_rows || row_with_array_of_rows.array_of_rows, x -> x.itemdata) FROM " + tableName);
        assertQuery("SELECT TRANSFORM_VALUES(MAP_CONCAT(map_varchar_key_row_value, row_with_map_varchar_key_row_value.map_varchar_key_row_value), (k,v) -> v.orderkey) FROM " + tableName);
        assertQuery("SELECT TRANSFORM_VALUES(MAP_REMOVE_NULL_VALUES(row_with_map_varchar_key_row_value.map_varchar_key_row_value), (k,v) -> v.orderkey) FROM " + tableName);
        assertQuery("SELECT TRANSFORM_VALUES(MAP_SUBSET(row_with_map_varchar_key_row_value.map_varchar_key_row_value, ARRAY['orderdata_ex']), (k,v) -> v.orderkey) FROM " + tableName);
        assertQuery("SELECT ANY_MATCH(MAP_VALUES(map_varchar_key_row_value), x -> x.orderkey % 2 = 0) FROM " + tableName);
        assertQuery("SELECT ANY_MATCH(MAP_TOP_N_VALUES(map_varchar_key_row_value, 10, (x, y) -> IF(x.orderkey < y.orderkey, -1, IF(x.orderkey = y.orderkey, 0, 1))), x -> x.orderkey % 2 = 0) FROM " + tableName);
        assertQuery("SELECT ANY_MATCH(MAP_VALUES(map_varchar_key_row_value), x -> x.orderkey % 2 = 0) FROM " + tableName);

        // Simple test of different column type of the array argument
        assertQuery("SELECT ALL_MATCH(array_of_rows, x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT ALL_MATCH(row_with_array_of_rows.array_of_rows, x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT ALL_MATCH(array_of_array_of_rows[1], x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT ALL_MATCH(map_varchar_key_array_of_row_value['orderdata'], x -> x.orderkey > 0) FROM " + tableName);

        // element_at
        assertQuery("SELECT ALL_MATCH(ELEMENT_AT(map_varchar_key_array_of_row_value, 'orderdata'), x -> x.orderkey > 0) FROM " + tableName);
        assertQuery("SELECT ALL_MATCH(ELEMENT_AT(array_of_array_of_rows, 1), x -> x.itemdata > 0) FROM " + tableName);

        // Queries that reference variables in lambdas
        assertQuery("SELECT ALL_MATCH(SLICE(array_of_rows, 1, row_with_array_of_rows.array_of_rows[1].itemdata), x -> x.itemdata > 0) FROM " + tableName);
        assertQuery("SELECT ALL_MATCH(array_of_rows, x -> x.itemdata > row_with_array_of_rows.array_of_rows[1].itemdata) FROM " + tableName);

        // Queries that lack full support
    }
}
