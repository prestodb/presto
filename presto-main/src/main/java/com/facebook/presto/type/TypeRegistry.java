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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.MapParametricType.MAP;
import static com.facebook.presto.type.RegexpType.REGEXP;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;

@ThreadSafe
public final class TypeRegistry
        implements TypeManager
{
    private final ConcurrentMap<String, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    public TypeRegistry()
    {
        this(ImmutableSet.<Type>of());
    }

    @Inject
    public TypeRegistry(Set<Type> types)
    {
        checkNotNull(types, "types is null");

        // Manually register UNKNOWN type without a verifyTypeClass call since it is a special type that can not be used by functions
        this.types.put(UNKNOWN.getName().toLowerCase(ENGLISH), UNKNOWN);

        // always add the built-in types; Presto will not function without these
        addType(BOOLEAN);
        addType(BIGINT);
        addType(DOUBLE);
        addType(VARCHAR);
        addType(VARBINARY);
        addType(DATE);
        addType(TIME);
        addType(TIME_WITH_TIME_ZONE);
        addType(TIMESTAMP);
        addType(TIMESTAMP_WITH_TIME_ZONE);
        addType(INTERVAL_YEAR_MONTH);
        addType(INTERVAL_DAY_TIME);
        addType(HYPER_LOG_LOG);
        addType(REGEXP);
        addType(LIKE_PATTERN);
        addType(JSON_PATH);
        addType(COLOR);
        addType(JSON);
        addParametricType(ARRAY);
        addParametricType(MAP);

        for (Type type : types) {
            addType(type);
        }
    }

    @Override
    public Type getType(String typeName)
    {
        String key = typeName.toLowerCase(ENGLISH);
        Type type = types.get(key);
        if (type == null) {
            instantiateParametricType(key);
            return types.get(key);
        }
        return type;
    }

    @Override
    public Type getParameterizedType(String parametricTypeName, List<String> typeNames)
    {
        List<String> lowerCaseTypeNames = FluentIterable.from(typeNames).transform(new Function<String, String>() {
            @Override
            public String apply(String input)
            {
                return input.toLowerCase(ENGLISH);
            }
        }).toList();

        return getType(parametricTypeName.toLowerCase(ENGLISH) + "<" + Joiner.on(",").join(lowerCaseTypeNames) + ">");
    }

    private synchronized void instantiateParametricType(String typeName)
    {
        if (types.containsKey(typeName)) {
            return;
        }
        TypeSignature signature = TypeSignature.parseTypeSignature(typeName);
        ImmutableList.Builder<Type> parameterTypes = ImmutableList.builder();
        for (TypeSignature parameter : signature.getParameters()) {
            parameterTypes.add(getType(parameter.toString()));
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase());
        if (parametricType == null) {
            return;
        }
        Type instantiatedType = parametricType.createType(parameterTypes.build());
        checkState(instantiatedType.getName().equalsIgnoreCase(typeName), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType.getName(), typeName);
        addType(instantiatedType);
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.copyOf(types.values());
    }

    public void addType(Type type)
    {
        verifyTypeClass(type);
        Type existingType = types.putIfAbsent(type.getName().toLowerCase(ENGLISH), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type.getName());
    }

    public void addParametricType(ParametricType parametricType)
    {
        checkArgument(!parametricTypes.containsKey(parametricType.getName()),
                "Parametric type already registered: %s", parametricType.getName());
        parametricTypes.putIfAbsent(parametricType.getName(), parametricType);
    }

    public static void verifyTypeClass(Type type)
    {
        checkNotNull(type, "type is null");
    }
}
