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
package com.facebook.presto.spi.materializedview;

import com.facebook.presto.spi.relation.ConstantExpression;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.materializedview.MaterializedViewPartitionMapping.Kind.HALF_OPEN_RANGE;
import static com.facebook.presto.spi.materializedview.MaterializedViewPartitionMapping.Kind.SINGLE_VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

public class TestMaterializedViewPartitionMapping
{
    @Test
    public void testSingleValue()
    {
        ConstantExpression value = new ConstantExpression(Optional.empty(), 7L, BIGINT);

        MaterializedViewPartitionMapping mapping = MaterializedViewPartitionMapping.singleValue(value);

        assertEquals(mapping.getKind(), SINGLE_VALUE);
        assertSame(mapping.getSingleValue(), value);
        expectThrows(IllegalStateException.class, mapping::getRange);
    }

    @Test
    public void testHalfOpenRange()
    {
        ConstantExpression lower = new ConstantExpression(Optional.empty(), 7L, BIGINT);
        ConstantExpression upper = new ConstantExpression(Optional.empty(), 9L, BIGINT);

        MaterializedViewPartitionMapping mapping = MaterializedViewPartitionMapping.halfOpenRange(lower, upper);

        assertEquals(mapping.getKind(), HALF_OPEN_RANGE);
        assertSame(mapping.getRange().getLowerBound(), lower);
        assertSame(mapping.getRange().getUpperBound(), upper);
        expectThrows(IllegalStateException.class, mapping::getSingleValue);
    }

    @Test
    public void testRejectsNullSingleValue()
    {
        ConstantExpression value = new ConstantExpression(Optional.empty(), null, BIGINT);

        expectThrows(IllegalArgumentException.class, () -> MaterializedViewPartitionMapping.singleValue(value));
    }
}
