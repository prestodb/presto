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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;

final class OrcTestingUtil
{
    private OrcTestingUtil() {}

    public static OrcRecordReader createReader(OrcDataSource dataSource, List<Long> columnIds, List<Type> types)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(dataSource, new OrcMetadataReader());

        List<String> columnNames = orcReader.getColumnNames();
        assertEquals(columnNames.size(), columnIds.size());

        Map<Integer, Type> includedColumns = new HashMap<>();
        int ordinal = 0;
        for (long columnId : columnIds) {
            assertEquals(columnNames.get(ordinal), String.valueOf(columnId));
            includedColumns.put(ordinal, types.get(ordinal));
            ordinal++;
        }

        return createRecordReader(orcReader, includedColumns);
    }

    public static OrcRecordReader createReaderNoRows(OrcDataSource dataSource)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(dataSource, new OrcMetadataReader());

        assertEquals(orcReader.getColumnNames().size(), 0);

        return createRecordReader(orcReader, ImmutableMap.of());
    }

    public static OrcRecordReader createRecordReader(OrcReader orcReader, Map<Integer, Type> includedColumns)
            throws IOException
    {
        return orcReader.createRecordReader(includedColumns, OrcPredicate.TRUE, DateTimeZone.UTC);
    }

    public static byte[] octets(int... values)
    {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = octet(values[i]);
        }
        return bytes;
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static byte octet(int b)
    {
        checkArgument((b >= 0) && (b <= 0xFF), "octet not in range: %s", b);
        return (byte) b;
    }
}
