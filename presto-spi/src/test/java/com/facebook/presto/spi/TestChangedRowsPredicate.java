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
package com.facebook.presto.spi;

import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestChangedRowsPredicate
{
    @Test
    public void testCopiesDataDisjuncts()
    {
        List<TupleDomain<ColumnHandle>> dataDisjuncts = new ArrayList<>();
        dataDisjuncts.add(TupleDomain.all());

        ChangedRowsPredicate predicate = new ChangedRowsPredicate(dataDisjuncts, TupleDomain.all());
        dataDisjuncts.clear();

        assertEquals(predicate.getDataDisjuncts(), ImmutableList.of(TupleDomain.all()));
        assertEquals(predicate.getRefreshBound(), TupleDomain.all());
    }

    @Test
    public void testEmptyPredicate()
    {
        ChangedRowsPredicate predicate = ChangedRowsPredicate.empty();

        assertTrue(predicate.getDataDisjuncts().isEmpty());
        assertEquals(predicate.getRefreshBound(), TupleDomain.all());
    }

    /**
     * The shorter constructors have to read as the answers that are safe when unknown, because a
     * connector that has not been taught about a field gets them by default. Claiming additions
     * only would leave a modified row's stale materialized row in place; claiming a removed-rows
     * relation that does not exist would have the consumer scan nothing and conclude nothing was
     * removed.
     */
    @Test
    public void testOmittedFieldsDefaultToTheConservativeAnswer()
    {
        ChangedRowsPredicate twoArg = new ChangedRowsPredicate(ImmutableList.of(TupleDomain.all()), TupleDomain.all());
        assertFalse(twoArg.isAdditionsOnly());
        assertFalse(twoArg.getRemovedRows().isPresent());

        ChangedRowsPredicate threeArg = new ChangedRowsPredicate(ImmutableList.of(TupleDomain.all()), TupleDomain.all(), true);
        assertTrue(threeArg.isAdditionsOnly());
        assertFalse(threeArg.getRemovedRows().isPresent());

        assertFalse(ChangedRowsPredicate.empty().isAdditionsOnly());
        assertFalse(ChangedRowsPredicate.empty().getRemovedRows().isPresent());
    }

    @Test
    public void testCarriesTheRemovedRowsRelation()
    {
        ConnectorTableHandle table = new ConnectorTableHandle() {};
        ChangedRowsPredicate predicate = new ChangedRowsPredicate(
                ImmutableList.of(TupleDomain.all()),
                TupleDomain.all(),
                false,
                Optional.of(new ChangedRowsPredicate.RemovedRows(table, "rowdata")));

        assertTrue(predicate.getRemovedRows().isPresent());
        assertEquals(predicate.getRemovedRows().get().getTable(), table);
        assertEquals(predicate.getRemovedRows().get().getRowColumn(), "rowdata");
    }

    /**
     * A removed-rows relation with no table, or no column naming the removed row, is unusable: the
     * consumer has nothing to scan or nothing to read out of it. Rejecting at construction keeps
     * that from surfacing later as a null dereference inside a plan.
     */
    @Test
    public void testRemovedRowsRequiresBothParts()
    {
        assertThrows(NullPointerException.class, () -> new ChangedRowsPredicate.RemovedRows(null, "rowdata"));
        assertThrows(NullPointerException.class, () -> new ChangedRowsPredicate.RemovedRows(new ConnectorTableHandle() {}, null));
        assertThrows(NullPointerException.class, () ->
                new ChangedRowsPredicate(ImmutableList.of(), TupleDomain.all(), false, null));
    }
}
