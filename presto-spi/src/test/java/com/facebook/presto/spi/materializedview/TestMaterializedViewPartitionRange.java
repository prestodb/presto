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
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestMaterializedViewPartitionRange
{
    @Test
    public void testHalfOpenRange()
    {
        ConstantExpression lower = new ConstantExpression(Optional.empty(), utf8Slice("2026-01-01"), VARCHAR);
        ConstantExpression upper = new ConstantExpression(Optional.empty(), utf8Slice("2026-02-01"), VARCHAR);

        MaterializedViewPartitionRange range = new MaterializedViewPartitionRange(lower, upper);

        assertEquals(range.getLowerBound(), lower);
        assertEquals(range.getUpperBound(), upper);
    }

    @Test
    public void testRejectsDifferentBoundTypes()
    {
        ConstantExpression lower = new ConstantExpression(Optional.empty(), utf8Slice("2026-01-01"), VARCHAR);
        ConstantExpression upper = new ConstantExpression(Optional.empty(), 1L, BIGINT);

        expectThrows(IllegalArgumentException.class, () -> new MaterializedViewPartitionRange(lower, upper));
    }

    @Test
    public void testRejectsNullBound()
    {
        ConstantExpression lower = new ConstantExpression(Optional.empty(), null, VARCHAR);
        ConstantExpression upper = new ConstantExpression(Optional.empty(), utf8Slice("2026-02-01"), VARCHAR);

        expectThrows(IllegalArgumentException.class, () -> new MaterializedViewPartitionRange(lower, upper));
    }
}
