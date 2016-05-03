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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestOrcRecordReader
{
    @Test
    public void testValidateOrcType()
            throws OrcCorruptionException
    {
        List<OrcType> types = ImmutableList.<OrcType>builder()
                .add(/* 0 */ new OrcType(OrcType.OrcTypeKind.STRUCT, ImmutableList.of(1, 2, 3, 7), ImmutableList.of("col_0_int", "col_1_map", "col_2_list", "col_3_struct")))
                .add(/* 1 */ new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of()))
                .add(/* 2 */ new OrcType(OrcType.OrcTypeKind.MAP, ImmutableList.of(4, 5), ImmutableList.of("col_0_string", "col_1_long")))
                .add(/* 3 */ new OrcType(OrcType.OrcTypeKind.LIST, ImmutableList.of(6), ImmutableList.of("col_0_double")))
                .add(/* 4 */ new OrcType(OrcType.OrcTypeKind.STRING, ImmutableList.of(), ImmutableList.of()))
                .add(/* 5 */ new OrcType(OrcType.OrcTypeKind.LONG, ImmutableList.of(), ImmutableList.of()))
                .add(/* 6 */ new OrcType(OrcType.OrcTypeKind.DOUBLE, ImmutableList.of(), ImmutableList.of()))
                .add(/* 7 */ new OrcType(OrcType.OrcTypeKind.STRUCT, ImmutableList.of(8, 9), ImmutableList.of("nested_col_0_double", "nested_col_1_boolean")))
                .add(/* 8 */ new OrcType(OrcType.OrcTypeKind.DOUBLE, ImmutableList.of(), ImmutableList.of()))
                .add(/* 9 */ new OrcType(OrcType.OrcTypeKind.BOOLEAN, ImmutableList.of(), ImmutableList.of()))
                .build();

        Map<Integer, Type> includedColumnsGood1 = ImmutableMap.of(
                0, BigintType.BIGINT,
                1, new MapType(VarcharType.createVarcharType(2), BigintType.BIGINT),
                2, new ArrayType(DoubleType.DOUBLE));
        Map<Integer, Type> includedColumnsGood2 = ImmutableMap.of(
                0, BigintType.BIGINT,
                1, new MapType(VarcharType.createVarcharType(2), BigintType.BIGINT),
                2, new ArrayType(DoubleType.DOUBLE),
                3, new RowType(ImmutableList.of(DoubleType.DOUBLE, BooleanType.BOOLEAN), Optional.of(ImmutableList.of("nested_col_0", "nested_col_1"))));

        Map<Integer, Type> includedColumnsBad1 = ImmutableMap.of(
                0, BooleanType.BOOLEAN, // should be bigint
                1, new MapType(VarcharType.createVarcharType(2), BigintType.BIGINT),
                2, new ArrayType(DoubleType.DOUBLE));
        Map<Integer, Type> includedColumnsBad2 = ImmutableMap.of(
                0, BigintType.BIGINT,
                1, new MapType(VarcharType.createVarcharType(2), DoubleType.DOUBLE), // should be map<varchar, bigint>
                2, new ArrayType(DoubleType.DOUBLE));
        Map<Integer, Type> includedColumnsBad3 = ImmutableMap.of(
                0, BigintType.BIGINT,
                1, new MapType(BigintType.BIGINT, VarcharType.createVarcharType(2)), // should be map<varchar, bigint>
                2, new ArrayType(DoubleType.DOUBLE));
        Map<Integer, Type> includedColumnsBad4 = ImmutableMap.of(
                0, BigintType.BIGINT,
                1, new MapType(VarcharType.createVarcharType(2), BigintType.BIGINT),
                2, new ArrayType(DoubleType.DOUBLE),
                3, new RowType(ImmutableList.of(DoubleType.DOUBLE), Optional.of(ImmutableList.of("nested_col_0")))); // should be row<double, boolean>
        Map<Integer, Type> includedColumnsBad5 = ImmutableMap.of(
                0, BigintType.BIGINT,
                1, new MapType(VarcharType.createVarcharType(2), BigintType.BIGINT),
                2, new ArrayType(DoubleType.DOUBLE),
                3, new RowType(ImmutableList.of(BigintType.BIGINT, BooleanType.BOOLEAN), Optional.of(ImmutableList.of("nested_col_0", "nested_col_1")))); // should be row<double, boolean>
        Map<Integer, Type> includedColumnsBad6 = ImmutableMap.of(
                0, BigintType.BIGINT,
                1, new MapType(VarcharType.createVarcharType(2), DateType.DATE), // should be map<varchar, bigint>
                2, new ArrayType(DoubleType.DOUBLE));
        Map<Integer, Type> includedColumnsBad7 = ImmutableMap.of(
                0, BigintType.BIGINT,
                1, new MapType(VarcharType.createVarcharType(2), TimestampType.TIMESTAMP), // should be map<varchar, bigint>
                2, new ArrayType(DoubleType.DOUBLE));

        OrcRecordReader.createStreamReaders(FakeOrcDataSource.INSTANCE, types, null, includedColumnsGood1);
        OrcRecordReader.createStreamReaders(FakeOrcDataSource.INSTANCE, types, null, includedColumnsGood2);

        assertExceptionInCreateStreamReaders(types, includedColumnsBad1);
        assertExceptionInCreateStreamReaders(types, includedColumnsBad2);
        assertExceptionInCreateStreamReaders(types, includedColumnsBad3);
        assertExceptionInCreateStreamReaders(types, includedColumnsBad4);
        assertExceptionInCreateStreamReaders(types, includedColumnsBad5);
        assertExceptionInCreateStreamReaders(types, includedColumnsBad6);
        assertExceptionInCreateStreamReaders(types, includedColumnsBad7);
    }

    public static void assertExceptionInCreateStreamReaders(List<OrcType> types, Map<Integer, Type> includedColumns)
    {
        try {
            OrcRecordReader.createStreamReaders(FakeOrcDataSource.INSTANCE, types, null, includedColumns);
            fail("expected exception");
        }
        catch (OrcCorruptionException exception) {
            assertEquals(exception.getMessage(), format("ORC type from file footer (%s) does not match expected type of table (%s)", types, includedColumns));
        }
    }
}
