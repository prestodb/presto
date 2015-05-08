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
package com.facebook.presto.kinesis.decoder.csv;

import static com.facebook.presto.kinesis.decoder.util.DecoderTestUtil.checkValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import com.facebook.presto.kinesis.KinesisColumnHandle;
import com.facebook.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.kinesis.decoder.KinesisFieldDecoder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestCsvDecoder
{
    private static final CsvKinesisFieldDecoder DEFAULT_FIELD_DECODER = new CsvKinesisFieldDecoder();

    private static Map<KinesisColumnHandle, KinesisFieldDecoder<?>> buildMap(List<KinesisColumnHandle> columns)
    {
        ImmutableMap.Builder<KinesisColumnHandle, KinesisFieldDecoder<?>> map = ImmutableMap.builder();
        for (KinesisColumnHandle column : columns) {
            map.put(column, DEFAULT_FIELD_DECODER);
        }
        return map.build();
    }

    @Test
    public void testSimple()
    {
        String csv = "\"row 1\",row2,\"row3\",100,\"200\",300,4.5";

        CsvKinesisRowDecoder rowDecoder = new CsvKinesisRowDecoder();
        KinesisColumnHandle row1 = new KinesisColumnHandle("", 0, "row1", VarcharType.VARCHAR, "0", null, null, false, false);
        KinesisColumnHandle row2 = new KinesisColumnHandle("", 1, "row2", VarcharType.VARCHAR, "1", null, null, false, false);
        KinesisColumnHandle row3 = new KinesisColumnHandle("", 2, "row3", VarcharType.VARCHAR, "2", null, null, false, false);
        KinesisColumnHandle row4 = new KinesisColumnHandle("", 3, "row4", BigintType.BIGINT, "3", null, null, false, false);
        KinesisColumnHandle row5 = new KinesisColumnHandle("", 4, "row5", BigintType.BIGINT, "4", null, null, false, false);
        KinesisColumnHandle row6 = new KinesisColumnHandle("", 5, "row6", BigintType.BIGINT, "5", null, null, false, false);
        KinesisColumnHandle row7 = new KinesisColumnHandle("", 6, "row7", DoubleType.DOUBLE, "6", null, null, false, false);

        List<KinesisColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6, row7);
        Set<KinesisFieldValueProvider> providers = new HashSet<>();

        boolean valid = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), providers, columns, buildMap(columns));
        assertTrue(valid);

        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, "row 1");
        checkValue(providers, row2, "row2");
        checkValue(providers, row3, "row3");
        checkValue(providers, row4, 100);
        checkValue(providers, row5, 200);
        checkValue(providers, row6, 300);
        checkValue(providers, row7, 4.5d);
    }
}
