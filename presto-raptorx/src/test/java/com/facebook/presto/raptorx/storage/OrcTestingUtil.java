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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.UnsignedBytes;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.raptorx.util.OrcUtil.createOrcRecordReader;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.Unit.PETABYTE;
import static org.testng.Assert.assertEquals;

final class OrcTestingUtil
{
    private OrcTestingUtil() {}

    public static OrcDataSource fileOrcDataSource(File file)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
    }

    public static OrcRecordReader createReader(OrcDataSource dataSource, List<Long> columnIds, List<Type> types)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, PETABYTE));

        List<String> columnNames = orcReader.getColumnNames();
        assertEquals(columnNames.size(), columnIds.size());

        Map<Integer, Type> includedColumns = new HashMap<>();
        int ordinal = 0;
        for (long columnId : columnIds) {
            assertEquals(columnNames.get(ordinal), String.valueOf(columnId));
            includedColumns.put(ordinal, types.get(ordinal));
            ordinal++;
        }

        return createOrcRecordReader(orcReader, includedColumns);
    }

    public static byte[] octets(int... values)
    {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = UnsignedBytes.checkedCast(values[i]);
        }
        return bytes;
    }
}
