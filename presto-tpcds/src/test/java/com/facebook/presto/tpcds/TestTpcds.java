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
package com.facebook.presto.tpcds;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

public class TestTpcds
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpcdsQueryRunner.createQueryRunner();
    }

    @Test
    public void testSelect()
    {
        MaterializedResult actual = computeActual(
                "SELECT c_first_name, c_last_name, ca_address_sk, ca_gmt_offset " +
                        "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk " +
                        "WHERE ca_address_sk = 4");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                // note that c_first_name and c_last_name are both of type CHAR(X) so the results
                // are padded with whitespace
                .row("James               ", "Brown                         ", 4L, new BigDecimal("-7.00"))
                .build();
        assertEquals(expected, actual);

        actual = computeActual(
                "SELECT c_first_name, c_last_name " +
                        "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk " +
                        "WHERE ca_address_sk = 4 AND ca_gmt_offset = DECIMAL '-7.00'");
        expected = resultBuilder(getSession(), actual.getTypes())
                .row("James               ", "Brown                         ")
                .build();
        assertEquals(expected, actual);
    }

    @Test
    public void testLargeInWithShortDecimal()
    {
        // TODO add a test with long decimal
        String longValues = range(0, 5000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));

        assertQuery("SELECT typeof(i_current_price) FROM item LIMIT 1", "VALUES 'decimal(7,2)'"); // decimal(7,2) is a short decimal
        assertQuerySucceeds("SELECT i_current_price FROM item WHERE i_current_price IN (" + longValues + ")");
        assertQuerySucceeds("SELECT i_current_price FROM item WHERE i_current_price NOT IN (" + longValues + ")");
        assertQuerySucceeds("SELECT i_current_price FROM item WHERE i_current_price IN (i_wholesale_cost, " + longValues + ")");
        assertQuerySucceeds("SELECT i_current_price FROM item WHERE i_current_price NOT IN (i_wholesale_cost, " + longValues + ")");
    }
}
