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
package com.facebook.presto.pinot;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableMap;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestPinotColumnMetadata
{
    @Test
    public void testParsePinotSchemaToPinotColumns()
    {
        PinotConfig pinotConfig = new PinotConfig();
        pinotConfig.setInferDateTypeInSchema(true);
        pinotConfig.setInferTimestampTypeInSchema(true);

        Schema testPinotSchema = new Schema.SchemaBuilder()
                .addSingleValueDimension("singleValueIntDimension", FieldSpec.DataType.INT)
                .addSingleValueDimension("singleValueLongDimension", FieldSpec.DataType.LONG)
                .addSingleValueDimension("singleValueFloatDimension", FieldSpec.DataType.FLOAT)
                .addSingleValueDimension("singleValueDoubleDimension", FieldSpec.DataType.DOUBLE)
                .addSingleValueDimension("singleValueBytesDimension", FieldSpec.DataType.BYTES)
                .addSingleValueDimension("singleValueBooleanDimension", FieldSpec.DataType.BOOLEAN)
                .addSingleValueDimension("singleValueStringDimension", FieldSpec.DataType.STRING)
                .addMultiValueDimension("multiValueIntDimension", FieldSpec.DataType.INT)
                .addMultiValueDimension("multiValueLongDimension", FieldSpec.DataType.LONG)
                .addMultiValueDimension("multiValueFloatDimension", FieldSpec.DataType.FLOAT)
                .addMultiValueDimension("multiValueDoubleDimension", FieldSpec.DataType.DOUBLE)
                .addMultiValueDimension("multiValueBytesDimension", FieldSpec.DataType.BYTES)
                .addMultiValueDimension("multiValueBooleanDimension", FieldSpec.DataType.BOOLEAN)
                .addMultiValueDimension("multiValueStringDimension", FieldSpec.DataType.STRING)
                .addMetric("intMetric", FieldSpec.DataType.INT)
                .addMetric("longMetric", FieldSpec.DataType.LONG)
                .addMetric("floatMetric", FieldSpec.DataType.FLOAT)
                .addMetric("doubleMetric", FieldSpec.DataType.DOUBLE)
                .addMetric("bytesMetric", FieldSpec.DataType.BYTES)
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"))
                .addDateTime("epochDayDateTime", FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
                .addDateTime("epochMillisDateTime", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:SECONDS")
                .addDateTime("epochTenDayDateTime", FieldSpec.DataType.INT, "10:DAYS:EPOCH", "1:DAYS")
                .addDateTime("epochSecondsDateTime", FieldSpec.DataType.LONG, "1:SECONDS:EPOCH", "1:SECONDS")
                .build();

        Map<String, Type> expectedTypeMap = new ImmutableMap.Builder<String, Type>()
                .put("singlevalueintdimension", INTEGER)
                .put("singlevaluelongdimension", BIGINT)
                .put("singlevaluefloatdimension", DOUBLE)
                .put("singlevaluedoubledimension", DOUBLE)
                .put("singlevaluebytesdimension", VARBINARY)
                .put("singlevaluebooleandimension", BOOLEAN)
                .put("singlevaluestringdimension", VARCHAR)
                .put("multivalueintdimension", new ArrayType(INTEGER))
                .put("multivaluelongdimension", new ArrayType(BIGINT))
                .put("multivaluefloatdimension", new ArrayType(DOUBLE))
                .put("multivaluedoubledimension", new ArrayType(DOUBLE))
                .put("multivaluebytesdimension", new ArrayType(VARBINARY))
                .put("multivaluebooleandimension", new ArrayType(BOOLEAN))
                .put("multivaluestringdimension", new ArrayType(VARCHAR))
                .put("intmetric", INTEGER)
                .put("longmetric", BIGINT)
                .put("floatmetric", DOUBLE)
                .put("doublemetric", DOUBLE)
                .put("bytesmetric", VARBINARY)
                .put("dayssinceepoch", DateType.DATE)
                .put("epochdaydatetime", DateType.DATE)
                .put("epochmillisdatetime", TimestampType.TIMESTAMP)
                .put("epochtendaydatetime", INTEGER)
                .put("epochsecondsdatetime", BIGINT)
                .build();
        Map<String, String> expectedComment = new ImmutableMap.Builder<String, String>()
                .put("singlevalueintdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("singlevaluelongdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("singlevaluefloatdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("singlevaluedoubledimension", FieldSpec.FieldType.DIMENSION.name())
                .put("singlevaluebytesdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("singlevaluebooleandimension", FieldSpec.FieldType.DIMENSION.name())
                .put("singlevaluestringdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("multivalueintdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("multivaluelongdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("multivaluefloatdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("multivaluedoubledimension", FieldSpec.FieldType.DIMENSION.name())
                .put("multivaluebytesdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("multivaluebooleandimension", FieldSpec.FieldType.DIMENSION.name())
                .put("multivaluestringdimension", FieldSpec.FieldType.DIMENSION.name())
                .put("intmetric", FieldSpec.FieldType.METRIC.name())
                .put("longmetric", FieldSpec.FieldType.METRIC.name())
                .put("floatmetric", FieldSpec.FieldType.METRIC.name())
                .put("doublemetric", FieldSpec.FieldType.METRIC.name())
                .put("bytesmetric", FieldSpec.FieldType.METRIC.name())
                .put("dayssinceepoch", FieldSpec.FieldType.TIME.name())
                .put("epochdaydatetime", FieldSpec.FieldType.DATE_TIME.name())
                .put("epochmillisdatetime", FieldSpec.FieldType.DATE_TIME.name())
                .put("epochtendaydatetime", FieldSpec.FieldType.DATE_TIME.name())
                .put("epochsecondsdatetime", FieldSpec.FieldType.DATE_TIME.name())
                .build();

        List<PinotColumn> pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testPinotSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        for (PinotColumn column : pinotColumns) {
            assertEquals(column.getType(), expectedTypeMap.get(column.getName()), "Failed to compare column type for field - " + column.getName());
            assertEquals(column.getComment(), expectedComment.get(column.getName()), "Failed to compare column comment for field - " + column.getName());
            assertEquals(column.isNullable(), false);
        }
    }

    @Test
    public void testTimeFieldInPinotSchemaToPinotColumns()
    {
        PinotConfig pinotConfig = new PinotConfig();
        pinotConfig.setInferDateTypeInSchema(true);
        pinotConfig.setInferTimestampTypeInSchema(true);

        // Test Date
        Schema testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"))
                .build();
        List<PinotColumn> pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        PinotColumn column = pinotColumns.get(0);
        assertEquals(column.getName(), "dayssinceepoch");
        assertEquals(column.getType(), DateType.DATE);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"),
                        new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "dayssinceepoch");
        assertEquals(column.getType(), DateType.DATE);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        // Test Timestamp
        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"), new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "millissinceepoch");
        assertEquals(column.getType(), TimestampType.TIMESTAMP);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"),
                        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "millissinceepoch");
        assertEquals(column.getType(), TimestampType.TIMESTAMP);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        // Test fallback to BIGINT
        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "secondsSinceEpoch"), new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "secondsSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "secondssinceepoch");
        assertEquals(column.getType(), BIGINT);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"),
                        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "secondsSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "secondssinceepoch");
        assertEquals(column.getType(), BIGINT);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"),
                        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "secondsSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "secondssinceepoch");
        assertEquals(column.getType(), BIGINT);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());
    }

    @Test
    public void testConversionWithoutConfigSwitchOn()
    {
        PinotConfig pinotConfig = new PinotConfig();
        pinotConfig.setInferDateTypeInSchema(false);
        pinotConfig.setInferTimestampTypeInSchema(false);

        // Test Date
        Schema testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"))
                .build();
        List<PinotColumn> pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        PinotColumn column = pinotColumns.get(0);
        assertEquals(column.getName(), "dayssinceepoch");
        assertEquals(column.getType(), INTEGER);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"),
                        new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "dayssinceepoch");
        assertEquals(column.getType(), INTEGER);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        // Test Timestamp
        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"), new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "millissinceepoch");
        assertEquals(column.getType(), BIGINT);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"),
                        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "millissinceepoch");
        assertEquals(column.getType(), BIGINT);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        // Test fallback to BIGINT
        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "secondsSinceEpoch"), new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "secondsSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "secondssinceepoch");
        assertEquals(column.getType(), BIGINT);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "millisSinceEpoch"),
                        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "secondsSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "secondssinceepoch");
        assertEquals(column.getType(), BIGINT);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());

        testSchema = new Schema.SchemaBuilder()
                .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"),
                        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "secondsSinceEpoch"))
                .build();
        pinotColumns = PinotColumnUtils.getPinotColumnsForPinotSchema(testSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
        column = pinotColumns.get(0);
        assertEquals(column.getName(), "secondssinceepoch");
        assertEquals(column.getType(), BIGINT);
        assertEquals(column.getComment(), FieldSpec.FieldType.TIME.name());
    }
}
