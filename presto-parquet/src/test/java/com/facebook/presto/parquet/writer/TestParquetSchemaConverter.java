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
package com.facebook.presto.parquet.writer;

import com.facebook.presto.common.type.RowType;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RowType.field;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.tests.StructuralTestUtil.mapType;
import static java.util.Arrays.asList;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;

public class TestParquetSchemaConverter
{
    @Test
    public void testMapKeyRepetitionLevel()
    {
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                ImmutableList.of(mapType(VARCHAR, INTEGER)),
                ImmutableList.of("test"));
        GroupType mapType = schemaConverter.getMessageType().getType(0).asGroupType();
        GroupType keyValueValue = mapType.getType(0).asGroupType();
        assertEquals(keyValueValue.isRepetition(REPEATED), true);
        Type keyType = keyValueValue.getType(0).asPrimitiveType();
        assertEquals(keyType.isRepetition(REQUIRED), true);
        PrimitiveType valueType = keyValueValue.getType(1).asPrimitiveType();
        assertEquals(valueType.isRepetition(OPTIONAL), true);

        schemaConverter = new ParquetSchemaConverter(
                ImmutableList.of(mapType(RowType.from(asList(field("a", VARCHAR), field("b", BIGINT))), INTEGER)),
                ImmutableList.of("test"));
        mapType = schemaConverter.getMessageType().getType(0).asGroupType();
        keyValueValue = mapType.getType(0).asGroupType();
        assertEquals(keyValueValue.isRepetition(REPEATED), true);
        keyType = keyValueValue.getType(0).asGroupType();
        assertEquals(keyType.isRepetition(REQUIRED), true);
        assertEquals(keyType.asGroupType().getType(0).asPrimitiveType().isRepetition(OPTIONAL), true);
        assertEquals(keyType.asGroupType().getType(1).asPrimitiveType().isRepetition(OPTIONAL), true);
        valueType = keyValueValue.getType(1).asPrimitiveType();
        assertEquals(valueType.isRepetition(OPTIONAL), true);
    }
}
