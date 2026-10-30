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
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProperties.MappingCardinality.MANY_TO_ONE;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestMaterializedViewPartitionTransformProperties
{
    @Test
    public void testSourceAndConstantArguments()
    {
        MaterializedViewPartitionTransformArgument source = MaterializedViewPartitionTransformArgument.sourceColumn(VARCHAR);
        ConstantExpression value = new ConstantExpression(Optional.empty(), utf8Slice("1h"), VARCHAR);
        MaterializedViewPartitionTransformArgument constant = MaterializedViewPartitionTransformArgument.constant(value);

        assertTrue(source.isSourceColumn());
        assertFalse(source.getConstant().isPresent());
        assertFalse(constant.isSourceColumn());
        assertEquals(constant.getConstant(), Optional.of(value));
        assertEquals(constant.getType(), VARCHAR);
    }

    @Test
    public void testProperties()
    {
        MaterializedViewPartitionTransformProperties properties = new MaterializedViewPartitionTransformProperties(0, ImmutableSet.of(1), MANY_TO_ONE, 1);

        assertEquals(properties.getSourceArgumentIndex(), 0);
        assertEquals(properties.getConstantArgumentIndexes(), ImmutableSet.of(1));
        assertEquals(properties.getMappingCardinality(), MANY_TO_ONE);
        assertEquals(properties.getFormatVersion(), 1);
    }

    @Test
    public void testRejectsInvalidProperties()
    {
        expectThrows(IllegalArgumentException.class, () -> new MaterializedViewPartitionTransformProperties(-1, ImmutableSet.of(1), MANY_TO_ONE, 1));
        expectThrows(IllegalArgumentException.class, () -> new MaterializedViewPartitionTransformProperties(0, ImmutableSet.of(0), MANY_TO_ONE, 1));
        expectThrows(IllegalArgumentException.class, () -> new MaterializedViewPartitionTransformProperties(0, ImmutableSet.of(1), MANY_TO_ONE, 0));
    }
}
