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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.CONNECTOR;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveTableLayoutHandle.canonicalizeDomainPredicate;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveTableLayoutHandle
{
    @Test
    public void testCanonicalizeDomain()
    {
        Map<String, HiveColumnHandle> predicateColumns = ImmutableMap.of(
                "ds", getColumnHandle("ds", true),
                "col", getColumnHandle("col", false));
        TupleDomain<Subfield> domain = TupleDomain.withColumnDomains(ImmutableMap.of(
                new Subfield("ds"), singleValue(VARCHAR, utf8Slice("2022-01-01")),
                new Subfield("col"), singleValue(VARCHAR, utf8Slice("id"))));
        TupleDomain<Subfield> newDomain = canonicalizeDomainPredicate(domain, predicateColumns, CONNECTOR);
        assertTrue(newDomain.getDomains().isPresent());
        assertEquals(newDomain.getDomains().get().size(), 1);
        assertEquals(newDomain.getDomains().get().get(new Subfield("col")), singleValue(VARCHAR, utf8Slice("id")));
    }

    private HiveColumnHandle getColumnHandle(String name, boolean partitioned)
    {
        return new HiveColumnHandle(
                name,
                HIVE_STRING,
                HIVE_STRING.getTypeSignature(),
                1,
                partitioned ? PARTITION_KEY : REGULAR,
                Optional.empty(),
                Optional.empty());
    }
}
