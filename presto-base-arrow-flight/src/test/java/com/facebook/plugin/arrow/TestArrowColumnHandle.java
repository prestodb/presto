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

import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.spi.ColumnMetadata;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestArrowColumnHandle
{
    @Test
    public void testConstructorAndGetters()
    {
        // Given
        String columnName = "testColumn";
        // When
        ArrowColumnHandle columnHandle = new ArrowColumnHandle(columnName, IntegerType.INTEGER);

        // Then
        assertEquals(columnHandle.getColumnName(), columnName, "Column name should match the input");
        assertEquals(columnHandle.getColumnType(), IntegerType.INTEGER, "Column type should match the input");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorWithNullColumnName()
    {
        // Given
        // When
        new ArrowColumnHandle(null, IntegerType.INTEGER); // Should throw NullPointerException
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorWithNullColumnType()
    {
        // Given
        String columnName = "testColumn";

        // When
        new ArrowColumnHandle(columnName, null); // Should throw NullPointerException
    }

    @Test
    public void testGetColumnMetadata()
    {
        // Given
        String columnName = "testColumn";
        ArrowColumnHandle columnHandle = new ArrowColumnHandle(columnName, IntegerType.INTEGER);

        // When
        ColumnMetadata columnMetadata = columnHandle.getColumnMetadata();

        // Then
        assertNotNull(columnMetadata, "ColumnMetadata should not be null");
        assertEquals(columnMetadata.getName(), columnName, "ColumnMetadata name should match the column name");
        assertEquals(columnMetadata.getType(), IntegerType.INTEGER, "ColumnMetadata type should match the column type");
    }

    @Test
    public void testToString()
    {
        String columnName = "testColumn";
        ArrowColumnHandle columnHandle = new ArrowColumnHandle(columnName, IntegerType.INTEGER);
        String result = columnHandle.toString();
        String expected = columnName + ":" + IntegerType.INTEGER;
        assertEquals(result, expected, "toString() should return the correct string representation");
    }
}
