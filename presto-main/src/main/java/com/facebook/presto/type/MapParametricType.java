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
package com.facebook.presto.type;

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

public final class MapParametricType
        implements ParametricType
{
    public static final MapParametricType MAP = new MapParametricType();
    // TODO: support types that don't use == for comparison of the bits in their stack type
    private static final Set<Type> SUPPORTED_KEY_TYPES = ImmutableSet.<Type>of(VARCHAR, BIGINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP);

    private MapParametricType()
    {
    }

    @Override
    public String getName()
    {
        return StandardTypes.MAP;
    }

    @Override
    public MapType createType(List<Type> types)
    {
        checkArgument(types.size() == 2, "Expected two types");
        checkArgument(SUPPORTED_KEY_TYPES.contains(types.get(0)), "Unsupported key type; %s", types.get(0));
        return new MapType(types.get(0), types.get(1));
    }
}
