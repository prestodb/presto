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
package com.facebook.presto.parquet;

import com.facebook.presto.spi.predicate.TupleDomain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.testng.annotations.Test;

import static com.facebook.presto.parquet.ParquetTypeUtils.getPrestoType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.testng.Assert.assertEquals;

public class TestParquetTypeUtils
{
    @Test
    public void testMapInt32ToPrestoInteger()
    {
        PrimitiveType intType = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, "int_col", OriginalType.INT_32);
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[]{"int_col"}, PrimitiveTypeName.INT32, 0, 1);
        RichColumnDescriptor intColumn = new RichColumnDescriptor(columnDescriptor, intType);
        assertEquals(getPrestoType(TupleDomain.all(), intColumn), INTEGER);
    }

    @Test
    public void testMapInt32WithoutOriginalTypeToPrestoInteger()
    {
        // int32 primitive should default to Presto integer if original type metadata isn't available
        PrimitiveType intType = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, "int_col");
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[]{"int_col"}, PrimitiveTypeName.INT32, 0, 1);
        RichColumnDescriptor intColumn = new RichColumnDescriptor(columnDescriptor, intType);
        assertEquals(getPrestoType(TupleDomain.all(), intColumn), INTEGER);
    }

    @Test
    public void testMapInt32ToPrestoDate()
    {
        // int32 primitive with original type of date should map to a Presto date
        PrimitiveType dateType = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, "date_col", OriginalType.DATE);
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[]{"date_col"}, PrimitiveTypeName.INT32, 0, 1);
        RichColumnDescriptor dateColumn = new RichColumnDescriptor(columnDescriptor, dateType);
        assertEquals(getPrestoType(TupleDomain.all(), dateColumn), DATE);
    }
}
