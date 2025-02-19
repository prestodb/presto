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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Map;

import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestTpcdsWithCharColumnsAsVarchar
        extends AbstractTestTpcds
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> tpcdsProperties = ImmutableMap.<String, String>builder()
                .put("tpcds.use-varchar-type", "true")
                .build();
        return TpcdsQueryRunner.createQueryRunner(ImmutableMap.of(), tpcdsProperties);
    }

    @Override
    @Test
    public void testSelect()
    {
        MaterializedResult actual = computeActual(
                "SELECT c_first_name, c_last_name, ca_address_sk, ca_gmt_offset " +
                        "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk " +
                        "WHERE ca_address_sk = 4");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("James", "Brown", 4L, new BigDecimal("-7.00"))
                .build();
        assertEquals(expected, actual);

        actual = computeActual(
                "SELECT c_first_name, c_last_name " +
                        "FROM customer JOIN customer_address ON c_current_addr_sk = ca_address_sk " +
                        "WHERE ca_address_sk = 4 AND ca_gmt_offset = DECIMAL '-7.00'");
        expected = resultBuilder(getSession(), actual.getTypes())
                .row("James", "Brown")
                .build();
        assertEquals(expected, actual);
    }
}
