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
package com.facebook.plugin.arrow;

import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestArrowAbstractMetadata
{
    public TestArrowAbstractMetadata() throws IOException
    {
    }

    @Test
    public void testGetTableMetadata()
    {
        // Mock dependencies
        ArrowAbstractMetadata metadata = mock(ArrowAbstractMetadata.class);
        Mockito.doCallRealMethod().when(metadata).getTableMetadata(Mockito.any(ConnectorSession.class), Mockito.any(ConnectorTableHandle.class));
        ConnectorSession session = mock(ConnectorSession.class);
        ArrowTableHandle tableHandle = new ArrowTableHandle("testSchema", "testTable");

        // Mock the behavior of getColumnsList
        List<Field> columnList = Arrays.asList(
                new Field("column1", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("column2", FieldType.notNullable(new ArrowType.Decimal(10, 2)), null),
                new Field("column3", FieldType.notNullable(new ArrowType.Bool()), null));

        when(metadata.getColumnsList("testSchema", "testTable", session)).thenReturn(columnList);
        // Call the method under test
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);

        // Verify the result
        assertNotNull(tableMetadata);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("testSchema", "testTable"));
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        assertEquals(columns.size(), 3);
        assertEquals(columns.get(0), new ColumnMetadata("column1", IntegerType.INTEGER));
        assertEquals(columns.get(1), new ColumnMetadata("column2", DecimalType.createDecimalType(10, 2)));
        assertEquals(columns.get(2), new ColumnMetadata("column3", BooleanType.BOOLEAN));
    }

    @Test
    public void testGetTableLayouts()
    {
        // Mock dependencies
        ConnectorSession session = mock(ConnectorSession.class);
        ConnectorTableHandle tableHandle = new ArrowTableHandle("testSchema", "testTable");

        // Mock the constraint
        Constraint<ColumnHandle> trueConstraint = Constraint.alwaysTrue();

        Set<ColumnHandle> desiredColumns = new HashSet<>();
        desiredColumns.add(new ArrowColumnHandle("column1", IntegerType.INTEGER, new ArrowTypeHandle(1, "typename", 10, 3, Optional.empty())));
        desiredColumns.add(new ArrowColumnHandle("column2", VarcharType.VARCHAR, new ArrowTypeHandle(2, "typename", 10, 3, Optional.empty())));

        // Call the method under test
        ArrowAbstractMetadata metadata = mock(ArrowAbstractMetadata.class);
        Mockito.doCallRealMethod().when(metadata).getTableLayouts(Mockito.any(ConnectorSession.class), Mockito.any(ConnectorTableHandle.class), Mockito.any(Constraint.class), Mockito.any(Optional.class));

        List<ConnectorTableLayoutResult> tableLayouts = metadata.getTableLayouts(session, tableHandle, trueConstraint, Optional.of(desiredColumns));

        // Verify the result
        assertNotNull(tableLayouts);
        assertEquals(tableLayouts.size(), 1);
        ConnectorTableLayoutResult layoutResult = tableLayouts.get(0);
        assertNotNull(layoutResult);
        assertNotNull(layoutResult.getTableLayout());
        assertEquals(((ArrowTableLayoutHandle) layoutResult.getTableLayout().getHandle()).getTableHandle(), tableHandle);
        assertEquals(((ArrowTableLayoutHandle) layoutResult.getTableLayout().getHandle()).getColumnHandles(), desiredColumns);
        assertEquals(layoutResult.getTableLayout().getPredicate(), trueConstraint.getSummary());
    }
}
