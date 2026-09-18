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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergParquetSchemaUtil.convert;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Covers the Parquet logical annotations Iceberg's own converter cannot emit. The write
 * tests assert the default form end to end; these assert the forms a non-default CRS or
 * algorithm produces, and that the rest of the schema is still converted by Iceberg.
 */
public class TestIcebergParquetSchemaUtil
{
    @Test
    public void testDefaultGeographyEmitsParameterlessAnnotation()
    {
        PrimitiveType column = geospatialColumn(Types.GeographyType.crs84());
        assertEquals(column.getPrimitiveTypeName(), BINARY);
        assertEquals(column.getId().intValue(), 1);
        assertEquals(column.getLogicalTypeAnnotation(), LogicalTypeAnnotation.geographyType());
    }

    /**
     * A geography column declaring EPSG:4326 is readable, and rewriting such a table must
     * keep the CRS it declares rather than silently normalize it.
     */
    @Test
    public void testNonDefaultCrsIsPreservedInAnnotation()
    {
        PrimitiveType column = geospatialColumn(Types.GeographyType.of("EPSG:4326"));
        assertEquals(
                column.getLogicalTypeAnnotation(),
                LogicalTypeAnnotation.geographyType("EPSG:4326", LogicalTypeAnnotation.DEFAULT_ALGO));
    }

    @Test
    public void testExplicitAlgorithmIsPreservedInAnnotation()
    {
        PrimitiveType column = geospatialColumn(Types.GeographyType.of("EPSG:4326", EdgeAlgorithm.SPHERICAL));
        assertEquals(
                column.getLogicalTypeAnnotation(),
                LogicalTypeAnnotation.geographyType("EPSG:4326", EdgeInterpolationAlgorithm.SPHERICAL));
    }

    /**
     * Presto cannot write a geometry column, so the conversion must refuse rather than
     * produce a file whose declared CRS was never applied to the coordinates.
     */
    @Test
    public void testGeometryIsRejected()
    {
        try {
            geospatialColumn(Types.GeometryType.crs84());
            fail("Expected conversion of a geometry column to fail");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), "Writing to Iceberg geometry column 'geo' is not supported. Only geography columns can be written");
        }
    }

    /**
     * Annotations are restored by field id, so a geography nested in a struct, a list and a
     * map must each come back annotated while the surrounding structure stays exactly as
     * Iceberg converted it.
     */
    @Test
    public void testNestedGeographyIsAnnotated()
    {
        Schema schema = new Schema(ImmutableList.of(
                optional(1, "row", Types.StructType.of(optional(2, "geog", Types.GeographyType.crs84()))),
                optional(3, "list", Types.ListType.ofOptional(4, Types.GeographyType.crs84())),
                optional(5, "map", Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.GeographyType.crs84()))));
        MessageType messageType = convert(schema, "table");

        for (String[] path : new String[][] {
                {"row", "geog"},
                {"list", "list", "element"},
                {"map", "key_value", "value"}}) {
            PrimitiveType column = messageType.getType(path).asPrimitiveType();
            assertEquals(column.getPrimitiveTypeName(), BINARY, "physical type at " + String.join(".", path));
            assertEquals(
                    column.getLogicalTypeAnnotation(),
                    LogicalTypeAnnotation.geographyType(),
                    "annotation at " + String.join(".", path));
        }
    }

    /**
     * A schema with no geospatial column must convert exactly as Iceberg would, since the
     * annotations are restored only when there is something to restore.
     */
    @Test
    public void testSchemaWithoutGeospatialColumnsIsUnchanged()
    {
        Schema schema = new Schema(ImmutableList.of(
                optional(1, "id", Types.IntegerType.get()),
                optional(2, "name", Types.StringType.get())));
        assertEquals(convert(schema, "table"), org.apache.iceberg.parquet.ParquetSchemaUtil.convert(schema, "table"));
    }

    private static PrimitiveType geospatialColumn(org.apache.iceberg.types.Type type)
    {
        Schema schema = new Schema(ImmutableList.of(optional(1, "geo", type)));
        return convert(schema, "table").getType("geo").asPrimitiveType();
    }
}
