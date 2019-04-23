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

import com.facebook.presto.orc.AbstractOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.raptorx.storage.OrcTestingUtil.createReader;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

public class TestOrcRead
{
    private static final DataSize SIZE = new DataSize(1, MEGABYTE);

    private static class InMemoryOrcDataSource
            extends AbstractOrcDataSource
    {
        private final byte[] data;

        public InMemoryOrcDataSource(byte[] data)
        {
            super(new OrcDataSourceId("memory"), data.length, SIZE, SIZE, SIZE, false);
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            System.arraycopy(data, toIntExact(position), buffer, bufferOffset, bufferLength);
        }
    }

    private String getOutputSlice(Slice slice)
    {
        String out = "";
        for (int i = 0; i < slice.length(); i++) {
            out += slice.getByte(i) + ",";
        }
        System.out.println(slice.toStringUtf8());
        return out;
    }

    @Test
    public void testRead()
            throws IOException
    {
        List<Long> columnIds = ImmutableList.of(38291L);
        List<Type> columnTypes = ImmutableList.of(createVarcharType(40));
        byte[] data = toByteArray(getResource("1col_603642767.orc"));
        OrcDataSource orcDataSource = new InMemoryOrcDataSource(data);
        OrcRecordReader reader = createReader(orcDataSource, columnIds, columnTypes);
        assertEquals(reader.getReaderRowCount(), 1);
        assertEquals(reader.getFileRowCount(), 1);
        assertEquals(reader.nextBatch(), 1);
        Block column0 = reader.readBlock(createVarcharType(40), 0);
        assertEquals(column0.getPositionCount(), 1);
        Slice slice = createVarcharType(40).getSlice(column0, 0);
        assertEquals(getOutputSlice(slice), "49,49,48,46,46,48,32,123,-53,114,-17,99,107,32,77,97,112,97,114,114,114,97,32,77,117,97,104,105,118,105,114,101,125,");
    }
}
