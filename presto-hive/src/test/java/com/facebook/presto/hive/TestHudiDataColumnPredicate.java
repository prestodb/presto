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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveSplitManager.getHudiDataColumnPredicate;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static org.testng.Assert.assertEquals;

public class TestHudiDataColumnPredicate
{
    @Test
    public void testOnlyTopLevelDataColumns()
    {
        Domain domain = Domain.singleValue(BIGINT, 10L);
        Map<String, HiveColumnHandle> columns = ImmutableMap.of(
                "x", column("x", REGULAR),
                "p", column("p", PARTITION_KEY),
                "s", column("s", SYNTHESIZED),
                "nested", column("nested", REGULAR));
        TupleDomain<Subfield> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                new Subfield("x"), domain,
                new Subfield("p"), domain,
                new Subfield("s"), domain,
                new Subfield("nested.field"), domain,
                new Subfield("unknown"), domain));
        assertEquals(getHudiDataColumnPredicate(predicate, columns), TupleDomain.withColumnDomains(ImmutableMap.of("x", domain)));
        assertEquals(getHudiDataColumnPredicate(TupleDomain.none(), columns), TupleDomain.none());
        assertEquals(getHudiDataColumnPredicate(TupleDomain.all(), columns), TupleDomain.all());
    }

    private static HiveColumnHandle column(String name, BaseHiveColumnHandle.ColumnType columnType)
    {
        return new HiveColumnHandle(name, HIVE_LONG, BIGINT.getTypeSignature(), 0, columnType, Optional.empty(), Optional.empty());
    }
}
