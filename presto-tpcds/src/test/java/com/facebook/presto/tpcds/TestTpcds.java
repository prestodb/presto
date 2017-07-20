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
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestTpcds
        extends AbstractTestQueryFramework
{
    @SuppressWarnings("unused")
    public TestTpcds()
            throws Exception
    {
        super(TpcdsQueryRunner::createQueryRunner);
    }

    @Test
    public void testSelect()
            throws Exception
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
    }
}
