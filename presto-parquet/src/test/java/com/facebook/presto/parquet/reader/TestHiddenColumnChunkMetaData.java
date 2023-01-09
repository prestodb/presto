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
package com.facebook.presto.parquet.reader;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.crypto.HiddenColumnChunkMetaData;
import org.apache.parquet.crypto.HiddenColumnException;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Set;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiddenColumnChunkMetaData
{
    @Test
    public void testIsHiddenColumn()
    {
        ColumnChunkMetaData column = new HiddenColumnChunkMetaData(ColumnPath.fromDotString("a.b.c"),
                "hdfs:/foo/bar/a.parquet");
        assertTrue(HiddenColumnChunkMetaData.isHiddenColumn(column));
    }

    @Test
    public void testIsNotHiddenColumn()
    {
        Set<Encoding> encodingSet = Collections.singleton(Encoding.RLE);
        ColumnChunkMetaData column = ColumnChunkMetaData.get(ColumnPath.fromDotString("a.b.c"), BINARY,
                CompressionCodecName.GZIP, encodingSet, -1, -1, -1, -1, -1);
        assertFalse(HiddenColumnChunkMetaData.isHiddenColumn(column));
    }

    @Test(expectedExceptions = HiddenColumnException.class)
    public void testHiddenColumnException()
    {
        ColumnChunkMetaData column = new HiddenColumnChunkMetaData(ColumnPath.fromDotString("a.b.c"),
                "hdfs:/foo/bar/a.parquet");
        column.getStatistics();
    }

    @Test
    public void testNoHiddenColumnException()
    {
        Set<Encoding> encodingSet = Collections.singleton(Encoding.RLE);
        ColumnChunkMetaData column = ColumnChunkMetaData.get(ColumnPath.fromDotString("a.b.c"), BINARY,
                CompressionCodecName.GZIP, encodingSet, -1, -1, -1, -1, -1);
        column.getStatistics();
    }
}
