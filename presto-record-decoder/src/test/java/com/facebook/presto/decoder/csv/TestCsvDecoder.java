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
package com.facebook.presto.decoder.csv;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestCsvDecoder
{
    private static final CsvFieldDecoder DEFAULT_FIELD_DECODER = new CsvFieldDecoder();

    private static Map<DecoderColumnHandle, FieldDecoder<?>> buildMap(List<DecoderColumnHandle> columns)
    {
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> map = ImmutableMap.builder();
        for (DecoderColumnHandle column : columns) {
            map.put(column, DEFAULT_FIELD_DECODER);
        }
        return map.build();
    }

    @Test
    public void testSimple()
    {
        String csv = "\"row 1\",row2,\"row3\",100,\"200\",300,4.5";

        CsvRowDecoder rowDecoder = new CsvRowDecoder();
        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", createVarcharType(2), "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", createVarcharType(10), "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", createVarcharType(10), "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BigintType.BIGINT, "3", null, null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle("", 4, "row5", BigintType.BIGINT, "4", null, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle("", 5, "row6", BigintType.BIGINT, "5", null, null, false, false, false);
        DecoderTestColumnHandle row7 = new DecoderTestColumnHandle("", 6, "row7", DoubleType.DOUBLE, "6", null, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6, row7);
        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, "ro");
        checkValue(providers, row2, "row2");
        checkValue(providers, row3, "row3");
        checkValue(providers, row4, 100);
        checkValue(providers, row5, 200);
        checkValue(providers, row6, 300);
        checkValue(providers, row7, 4.5d);
    }

    @Test
    public void testBoolean()
    {
        String csv = "True,False,0,1,\"0\",\"1\",\"true\",\"false\"";

        CsvRowDecoder rowDecoder = new CsvRowDecoder();

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", BooleanType.BOOLEAN, "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", BooleanType.BOOLEAN, "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", BooleanType.BOOLEAN, "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BooleanType.BOOLEAN, "3", null, null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle("", 4, "row5", BooleanType.BOOLEAN, "4", null, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle("", 5, "row6", BooleanType.BOOLEAN, "5", null, null, false, false, false);
        DecoderTestColumnHandle row7 = new DecoderTestColumnHandle("", 6, "row7", BooleanType.BOOLEAN, "6", null, null, false, false, false);
        DecoderTestColumnHandle row8 = new DecoderTestColumnHandle("", 7, "row8", BooleanType.BOOLEAN, "7", null, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5, row6, row7, row8);

        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, true);
        checkValue(providers, row2, false);
        checkValue(providers, row3, false);
        checkValue(providers, row4, false);
        checkValue(providers, row5, false);
        checkValue(providers, row6, false);
        checkValue(providers, row7, true);
        checkValue(providers, row8, false);
    }

    @Test
    public void testNulls()
    {
        String csv = ",,,";

        CsvRowDecoder rowDecoder = new CsvRowDecoder();

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", createVarcharType(10), "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", BigintType.BIGINT, "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", DoubleType.DOUBLE, "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BooleanType.BOOLEAN, "3", null, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4);

        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8), null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, "");
        checkValue(providers, row2, 0);
        checkValue(providers, row3, 0.0d);
        checkValue(providers, row4, false);
    }
}
