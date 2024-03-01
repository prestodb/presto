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

import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

public class TestThriftSupport
{
    @Test
    public void smokeTestThriftSupport()
            throws Exception
    {
        // run TPCH Q6 with thrift enabled to smoke test thrift support
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setSingleExtraProperty("experimental.internal-communication.thrift-transport-enabled", "true")
                .build()) {
            queryRunner.execute("SELECT sum(l.extendedprice * l.discount) AS revenue " +
                    "FROM lineitem l WHERE" +
                    " l.shipdate >= DATE '1994-01-01'" +
                    " AND l.shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR" +
                    " AND l.discount BETWEEN .06 - 0.01 AND .06 + 0.01" +
                    " AND l.quantity < 24");
        }
    }
}
