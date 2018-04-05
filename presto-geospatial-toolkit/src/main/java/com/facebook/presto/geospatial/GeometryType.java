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
package com.facebook.presto.geospatial;

import static java.util.Objects.requireNonNull;

public enum GeometryType
{
    POINT(0, false),
    MULTI_POINT(1, true),
    LINE_STRING(2, false),
    MULTI_LINE_STRING(3, true),
    POLYGON(4, false),
    MULTI_POLYGON(5, true),
    GEOMETRY_COLLECTION(6, true);

    private final int code;
    private final boolean multitype;

    GeometryType(int code, boolean multitype)
    {
        this.code = code;
        this.multitype = multitype;
    }

    public int code()
    {
        return code;
    }

    public boolean isMultitype()
    {
        return multitype;
    }

    public static GeometryType getForEsriGeometryType(String type)
    {
        return getForInternalLibraryName(type);
    }

    public static GeometryType getForJtsGeometryType(String type)
    {
        return getForInternalLibraryName(type);
    }

    private static GeometryType getForInternalLibraryName(String type)
    {
        requireNonNull(type, "type is null");
        switch (type) {
            case "Point":
                return POINT;
            case "MultiPoint":
                return MULTI_POINT;
            case "LineString":
                return LINE_STRING;
            case "MultiLineString":
                return MULTI_LINE_STRING;
            case "Polygon":
                return POLYGON;
            case "MultiPolygon":
                return MULTI_POLYGON;
            case "GeometryCollection":
                return GEOMETRY_COLLECTION;
            default:
                throw new IllegalArgumentException("Invalid Geometry Type: " + type);
        }
    }

    public static GeometryType getForCode(int code)
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
            default:
                throw new IllegalArgumentException("Invalid type code: " + code);
        }
    }
}
