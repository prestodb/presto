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
package io.prestosql.geospatial.serde;

import io.prestosql.geospatial.GeometryType;

public enum GeometrySerializationType
{
    POINT(0, GeometryType.POINT),
    MULTI_POINT(1, GeometryType.MULTI_POINT),
    LINE_STRING(2, GeometryType.LINE_STRING),
    MULTI_LINE_STRING(3, GeometryType.MULTI_LINE_STRING),
    POLYGON(4, GeometryType.POLYGON),
    MULTI_POLYGON(5, GeometryType.MULTI_POLYGON),
    GEOMETRY_COLLECTION(6, GeometryType.GEOMETRY_COLLECTION),
    ENVELOPE(7, GeometryType.POLYGON);

    private final int code;
    private final GeometryType geometryType;

    GeometrySerializationType(int code, GeometryType geometryType)
    {
        this.code = code;
        this.geometryType = geometryType;
    }

    public int code()
    {
        return code;
    }

    public GeometryType geometryType()
    {
        return geometryType;
    }

    public static GeometrySerializationType getForCode(int code)
    {
        switch (code) {
            case 0:
                return POINT;
            case 1:
                return MULTI_POINT;
            case 2:
                return LINE_STRING;
            case 3:
                return MULTI_LINE_STRING;
            case 4:
                return POLYGON;
            case 5:
                return MULTI_POLYGON;
            case 6:
                return GEOMETRY_COLLECTION;
            case 7:
                return ENVELOPE;
            default:
                throw new IllegalArgumentException("Invalid type code: " + code);
        }
    }
}
