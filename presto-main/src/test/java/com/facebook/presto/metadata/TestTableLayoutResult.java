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

package com.facebook.presto.metadata;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.TableLayoutResult.computeEnforced;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.fail;

public class TestTableLayoutResult
{
    @Test
    public void testComputeEnforced()
    {
        assertComputeEnforced(TupleDomain.all(), TupleDomain.all(), TupleDomain.all());
        assertComputeEnforcedFails(TupleDomain.all(), TupleDomain.none());
        assertComputeEnforced(TupleDomain.none(), TupleDomain.all(), TupleDomain.none());
        assertComputeEnforced(TupleDomain.none(), TupleDomain.none(), TupleDomain.all());

        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))));
        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.all());
        assertComputeEnforcedFails(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.none());
        assertComputeEnforcedFails(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 9999L))));
        assertComputeEnforcedFails(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c9999"), Domain.singleValue(BIGINT, 1L))));

        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))));
        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))));
        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))));
        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.all());
    }

    private void assertComputeEnforcedFails(TupleDomain<ColumnHandle> predicate, TupleDomain<ColumnHandle> unenforced)
    {
        try {
            TupleDomain<ColumnHandle> enforced = computeEnforced(predicate, unenforced);
            fail(String.format("expected IllegalArgumentException but found [%s]", enforced.toString(SESSION.getSqlFunctionProperties())));
        }
        catch (IllegalArgumentException e) {
            // do nothing
        }
    }

    private void assertComputeEnforced(TupleDomain<ColumnHandle> predicate, TupleDomain<ColumnHandle> unenforced, TupleDomain<ColumnHandle> expectedEnforced)
    {
        TupleDomain<ColumnHandle> enforced = computeEnforced(predicate, unenforced);
        if (!enforced.equals(expectedEnforced)) {
            fail(String.format("expected [%s] but found [%s]", expectedEnforced.toString(SESSION.getSqlFunctionProperties()), enforced.toString(SESSION.getSqlFunctionProperties())));
        }
    }
}
