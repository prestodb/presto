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

public abstract class AbstractTestTextFileReader
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_text")) {
            queryRunner.execute("CREATE TABLE orders_text WITH (format = 'TEXTFILE') AS " +
                    "SELECT orderkey, custkey, orderstatus, totalprice, " +
                    "orderpriority, clerk, shippriority, comment FROM tpch.tiny.orders");
        }
    }

    @Test
    public void testSimpleSelect()
    {
        assertQuery("SELECT * FROM orders_text");
        assertQuery("SELECT orderkey, orderstatus, totalprice FROM orders_text");
    }
}
