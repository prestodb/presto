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

import org.testng.annotations.Test;

public class TestHiveArrayFunctionQueries
        extends AbstractTestHiveQueries
{
    public TestHiveArrayFunctionQueries()
    {
        super(false);
    }

    @Test
    public void testRepeat()
    {
        this.assertQuery("SELECT repeat(orderkey, linenumber) FROM lineitem");
        this.assertQuery("SELECT repeat(orderkey, 3) FROM lineitem");
        this.assertQuery("SELECT repeat(orderkey, NULL) FROM lineitem");
        this.assertQuery("SELECT try(repeat(orderkey, -2)) FROM lineitem");
        this.assertQuery("SELECT try(repeat(orderkey, 10001)) FROM lineitem");
        this.assertQueryFails("SELECT repeat(orderkey, -2) FROM lineitem",
                ".*Count argument of repeat function must be greater than or equal to 0.*");
        this.assertQueryFails("SELECT repeat(orderkey, 10001) FROM lineitem",
                ".*Count argument of repeat function must be less than or equal to 10000.*");
    }

    @Test
    public void testShuffle()
    {
        this.assertQuerySucceeds("SELECT shuffle(quantities) FROM orders_ex");
        this.assertQuerySucceeds("SELECT shuffle(array_sort(quantities)) FROM orders_ex");
        this.assertQuery("SELECT array_sort(shuffle(quantities)) FROM orders_ex");
    }

    @Test
    public void testArrayMatch()
    {
        // test all_match
        this.assertQuery("SELECT all_match(quantities, x -> ((10 / x) > 2)) FROM orders_ex");
        this.assertQuery("SELECT all_match(quantities, x -> TRY(((10 / x) > 2))) FROM orders_ex");
        this.assertQuery("SELECT TRY(all_match(quantities, x -> ((10 / x) > 2))) FROM orders_ex");
        this.assertQuery("SELECT all_match(shuffle(quantities), x -> (x > 500.0)) FROM orders_ex");

        // test any_match
        this.assertQuery("SELECT any_match(quantities, x -> ((10 / x) > 2)) FROM orders_ex");
        this.assertQuery("SELECT any_match(quantities, x -> TRY(((10 / x) > 2))) FROM orders_ex");
        this.assertQuery("SELECT TRY(any_match(quantities, x -> ((10 / x) > 2))) FROM orders_ex");
        this.assertQuery("SELECT any_match(shuffle(quantities), x -> (x > 500.0)) FROM orders_ex");

        // test none_match
        this.assertQuery("SELECT none_match(quantities, x -> ((10 / x) > 2)) FROM orders_ex");
        this.assertQuery("SELECT none_match(quantities, x -> TRY(((10 / x) > 2))) FROM orders_ex");
        this.assertQuery("SELECT TRY(none_match(quantities, x -> ((10 / x) > 2))) FROM orders_ex");
        this.assertQuery("SELECT none_match(shuffle(quantities), x -> (x > 500.0)) FROM orders_ex");
    }
}
