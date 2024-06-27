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
package com.facebook.presto.tests;

import org.testng.annotations.Test;

public abstract class AbstractTestNativeTpchConnectorQueries
        extends AbstractTestQueryFramework
{
    @Test
    public void testTpchTinyTables()
    {
        assertQuery("SELECT count(*) FROM customer");
        assertQuery("SELECT count(*) FROM lineitem");
        assertQuery("SELECT count(*) FROM orders");
        assertQuery("SELECT count(*) FROM nation");
        assertQuery("SELECT count(*) FROM part");
        assertQuery("SELECT count(*) FROM partsupp");
        assertQuery("SELECT count(*) FROM region");
        assertQuery("SELECT count(*) FROM supplier");
        assertQuery("SELECT c_custkey, c_name, c_nationkey FROM customer",
                "SELECT custkey, name, nationkey FROM customer");
        assertQuery("SELECT n_nationkey, n_name, n_regionkey FROM nation",
                "SELECT nationkey, name, regionkey FROM nation");
        assertQuery("SELECT r_regionkey, r_name FROM region",
                "SELECT regionkey, name FROM region");
        assertQuery("SELECT SUM(l_discount) FROM lineitem WHERE l_discount != 0.04",
                "SELECT SUM(discount) FROM lineitem WHERE discount != 0.04");
        assertQuery("SELECT ps_partkey, ps_availqty, ps_supplycost FROM partsupp WHERE ps_availqty < 5",
                "SELECT partkey, availqty, supplycost FROM partsupp WHERE availqty < 5");
        assertQuery("SELECT p_partkey, p_mfgr, p_brand FROM part WHERE p_size = 4 and p_partkey < 1000",
                "SELECT partkey, mfgr, brand FROM part WHERE size = 4 and partkey < 1000");
        assertQuery("SELECT s_suppkey, s_phone FROM supplier WHERE s_acctbal < 100",
                "SELECT suppkey, phone FROM supplier WHERE acctbal < 100");
        assertQuery("SELECT c_custkey, c_phone FROM customer WHERE c_acctbal > 9900",
                "SELECT custkey, phone FROM customer WHERE acctbal > 9900");
    }
}
