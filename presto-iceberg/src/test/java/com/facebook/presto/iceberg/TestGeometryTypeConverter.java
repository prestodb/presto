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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.testng.Assert.assertEquals;

public class TestGeometryTypeConverter
{
    private final TypeManager typeManager = createTestFunctionAndTypeManager();

    @Test
    public void testGeometryCRSMapsToPrestoGeometry()
    {
        assertEquals(toPrestoType(Types.GeometryType.crs84(), typeManager), GEOMETRY);
        assertEquals(toPrestoType(Types.GeometryType.of("OGC:CRS84"), typeManager), GEOMETRY);
        // Iceberg normalizes the default CRS case-insensitively
        assertEquals(toPrestoType(Types.GeometryType.of("ogc:crs84"), typeManager), GEOMETRY);
    }

    @Test
    public void testNestedGeometryCRSMapsToPrestoGeometry()
    {
        Type arrayType = toPrestoType(
                Types.ListType.ofOptional(2, Types.GeometryType.crs84()),
                typeManager);
        assertEquals(arrayType, new ArrayType(GEOMETRY));

        Type rowType = toPrestoType(
                Types.StructType.of(optional(2, "geom", Types.GeometryType.crs84())),
                typeManager);
        assertEquals(rowType, RowType.from(ImmutableList.of(RowType.field("geom", GEOMETRY))));
    }

    /**
     * The write direction must produce the canonical default form: leaving the CRS unset
     * serializes as "geometry" rather than spelling out the defaults, which
     * is what other writers emit and what readers compare against.
     */
    @Test
    public void testSphericalGeometryMapsToDefaultGeometry()
    {
        org.apache.iceberg.types.Type icebergType = TypeConverter.toIcebergType(GEOMETRY);
        assertEquals(icebergType, Types.GeometryType.crs84());
        // Unset CRS and algorithm are what serialize as the bare "geometry". The accessors
        // cannot tell: Iceberg 1.10 reports them as null and later versions as the defaults
        assertEquals(icebergType.toString(), "geometry");
    }
}
