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

public enum GeometryType
{
    POINT(0),
    MULTI_POINT(1),
    LINE_STRING(2),
    MULTI_LINE_STRING(3),
    POLYGON(4),
    MULTI_POLYGON(5),
    GEOMETRY_COLLECTION(6);

    private final int code;

    GeometryType(int code)
    {
        this.code = code;
    }

    public int code()
    {
        return code;
    }
}
