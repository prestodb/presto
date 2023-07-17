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
package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;

public abstract class AbstractTestNativeArrayFunctionQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createOrdersEx(queryRunner);
    }

    @Test
    public void testRepeat()
    {
        assertQuery("SELECT repeat(orderkey, linenumber) FROM lineitem");
        assertQuery("SELECT repeat(orderkey, 3) FROM lineitem");
        assertQuery("SELECT repeat(orderkey, NULL) FROM lineitem");
        assertQuery("SELECT try(repeat(orderkey, -2)) FROM lineitem");
        assertQuery("SELECT try(repeat(orderkey, 10001)) FROM lineitem");
        assertQueryFails("SELECT repeat(orderkey, -2) FROM lineitem",
                ".*Count argument of repeat function must be greater than or equal to 0.*");
        assertQueryFails("SELECT repeat(orderkey, 10001) FROM lineitem",
                ".*Count argument of repeat function must be less than or equal to 10000.*");
    }

    @Test
    public void testShuffle()
    {
        assertQuerySucceeds("SELECT shuffle(quantities) FROM orders_ex");
        assertQuerySucceeds("SELECT shuffle(array_sort(quantities)) FROM orders_ex");
        assertQuery("SELECT array_sort(shuffle(quantities)) FROM orders_ex");
    }

    @Test
    public void testArrayMatch()
    {
        // test all_match
        assertQuery("SELECT all_match(quantities, x -> ((10 / x) > 2)) FROM orders_ex");
        assertQuery("SELECT all_match(quantities, x -> TRY(((10 / x) > 2))) FROM orders_ex");
        assertQuery("SELECT TRY(all_match(quantities, x -> ((10 / x) > 2))) FROM orders_ex");
        assertQuery("SELECT all_match(shuffle(quantities), x -> (x > 500.0)) FROM orders_ex");

        // test any_match
        assertQuery("SELECT any_match(quantities, x -> ((10 / x) > 2)) FROM orders_ex");
        assertQuery("SELECT any_match(quantities, x -> TRY(((10 / x) > 2))) FROM orders_ex");
        assertQuery("SELECT TRY(any_match(quantities, x -> ((10 / x) > 2))) FROM orders_ex");
        assertQuery("SELECT any_match(shuffle(quantities), x -> (x > 500.0)) FROM orders_ex");

        // test none_match
        assertQuery("SELECT none_match(quantities, x -> ((10 / x) > 2)) FROM orders_ex");
        assertQuery("SELECT none_match(quantities, x -> TRY(((10 / x) > 2))) FROM orders_ex");
        assertQuery("SELECT TRY(none_match(quantities, x -> ((10 / x) > 2))) FROM orders_ex");
        assertQuery("SELECT none_match(shuffle(quantities), x -> (x > 500.0)) FROM orders_ex");
    }

    @Test
    public void testArraySort()
    {
        assertQuery("SELECT array_sort(quantities), array_sort_desc(quantities) FROM orders_ex");
        assertQuery("SELECT array_sort(quantities, (x, y) -> if (x < y, 1, if (x > y, -1, 0))) FROM orders_ex",
                "SELECT array_sort_desc(quantities) FROM orders_ex");
    }

    @Test
    public void testArrayTrim()
    {
        assertQuery("SELECT trim_array(quantities, 0) FROM orders_ex");
        assertQuery("SELECT trim_array(quantities, 1) FROM orders_ex where cardinality(quantities) > 5");
        assertQuery("SELECT trim_array(quantities, 2) FROM orders_ex where cardinality(quantities) > 5");
        assertQuery("SELECT trim_array(quantities, 3) FROM orders_ex where cardinality(quantities) > 5");
        assertQueryFails("SELECT trim_array(quantities, 3) FROM orders_ex where cardinality(quantities) = 2", ".*size must not exceed array cardinality.*");
    }

    @Test
    public void testArrayConcat()
    {
        // Concatenate two integer arrays.
        assertQuery("SELECT concat(ARRAY[linenumber], ARRAY[orderkey, partkey]) FROM lineitem");
        assertQuery("SELECT ARRAY[linenumber] || ARRAY[orderkey, partkey] FROM lineitem");
        // Concatenate two integer arrays with null.
        assertQuery("SELECT concat(ARRAY[linenumber, NULL], ARRAY[orderkey, partkey]) FROM lineitem");
        assertQuery("SELECT concat(ARRAY[linenumber], NULL, ARRAY[orderkey, partkey]) FROM lineitem");
        assertQuery("SELECT ARRAY[linenumber, NULL] || ARRAY[orderkey, partkey] FROM lineitem");
        // Concatenate more than two arrays.
        assertQuery("SELECT concat(ARRAY[linenumber], ARRAY[partkey], ARRAY[orderkey, partkey], ARRAY[123, 456], ARRAY[quantity]) FROM lineitem");
        assertQuery("SELECT ARRAY[linenumber] || ARRAY[partkey] || ARRAY[orderkey, partkey] || ARRAY[123, 456] || ARRAY[quantity] FROM lineitem");
        // Concatenate complex types.
        assertQuery("SELECT concat(ARRAY[ARRAY[linenumber], ARRAY[suppkey, orderkey]], ARRAY[ARRAY[orderkey, partkey]]) FROM lineitem");
        assertQuery("SELECT ARRAY[ARRAY[linenumber], ARRAY[suppkey, orderkey]] || ARRAY[ARRAY[orderkey, partkey]] FROM lineitem");
        // Concatenate array with a single element.
        assertQuery("SELECT concat(linenumber, ARRAY[orderkey, partkey]) FROM lineitem");
        assertQuery("SELECT concat(ARRAY[orderkey, partkey], linenumber) FROM lineitem");
        assertQuery("SELECT linenumber || ARRAY[orderkey, partkey] FROM lineitem");
        assertQuery("SELECT ARRAY[orderkey, partkey] || linenumber FROM lineitem");
        // Concatenate array with a null.
        assertQuery("SELECT concat(CAST(NULL AS INTEGER), ARRAY[orderkey, partkey]) FROM lineitem");
        assertQuery("SELECT concat(ARRAY[orderkey, partkey], CAST(NULL AS INTEGER)) FROM lineitem");
        assertQuery("SELECT CAST(NULL AS INTEGER) || ARRAY[orderkey, partkey] FROM lineitem");
        assertQuery("SELECT ARRAY[orderkey, partkey] || CAST(NULL AS INTEGER) FROM lineitem");
        // Test nested concatenation.
        assertQuery("SELECT concat(linenumber, concat(orderkey, ARRAY[suppkey, partkey])) FROM lineitem");
        assertQuery("SELECT linenumber || concat(orderkey, ARRAY[suppkey, partkey]) FROM lineitem");
        assertQuery("SELECT concat(linenumber, orderkey || ARRAY[suppkey, partkey]) FROM lineitem");
    }
}
