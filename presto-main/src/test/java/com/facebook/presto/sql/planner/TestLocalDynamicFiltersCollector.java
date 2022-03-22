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

package com.facebook.presto.sql.planner;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestLocalDynamicFiltersCollector
{
    @Test
    public void testCollector()
    {
        VariableReferenceExpression variable = new VariableReferenceExpression("variable", BIGINT);

        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        assertEquals(collector.getPredicate(), TupleDomain.all());

        collector.intersect(TupleDomain.all());
        assertEquals(collector.getPredicate(), TupleDomain.all());

        collector.intersect(tupleDomain(variable, 1L, 2L));
        assertEquals(collector.getPredicate(), tupleDomain(variable, 1L, 2L));

        collector.intersect(tupleDomain(variable, 2L, 3L));
        assertEquals(collector.getPredicate(), tupleDomain(variable, 2L));

        collector.intersect(tupleDomain(variable, 0L));
        assertEquals(collector.getPredicate(), TupleDomain.none());
    }

    private TupleDomain<VariableReferenceExpression> tupleDomain(VariableReferenceExpression variable, Long... values)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(variable, Domain.multipleValues(BIGINT, ImmutableList.copyOf(values))));
    }
}
