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
package com.facebook.presto.sidecar.typemanager;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ExactTypeSignature;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.type.TypeManagerProvider;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.common.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalParametricType.DECIMAL;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.FunctionParametricType.FUNCTION;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.common.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.common.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.common.type.IpPrefixType.IPPREFIX;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.NoOpMapType.NO_OP_MAP;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.RowParametricType.ROW;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharParametricType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class NativeTypeManager
        implements TypeManagerProvider
{
    private static final Logger log = Logger.get(NativeTypeManager.class);

    private static final List<Type> TYPES =
            ImmutableList.of(
                    BIGINT,
                    UNKNOWN,
                    REAL,
                    VARBINARY,
                    TIMESTAMP,
                    TINYINT,
                    BOOLEAN,
                    DATE,
                    INTEGER,
                    DOUBLE,
                    SMALLINT,
                    HYPER_LOG_LOG,
                    JSON,
                    TIMESTAMP_WITH_TIME_ZONE,
                    UUID,
                    IPADDRESS,
                    IPPREFIX,
                    INTERVAL_DAY_TIME,
                    INTERVAL_YEAR_MONTH);

    private static final List<ParametricType> PARAMETRIC_TYPES =
            ImmutableList.of(
                    ARRAY,
                    DECIMAL,
                    FUNCTION,
                    NO_OP_MAP,
                    ROW,
                    VARCHAR);

    private final FunctionMetadataManager functionMetadataManager;
    private final LoadingCache<ExactTypeSignature, Type> parametricTypeCache;
    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    @Inject
    public NativeTypeManager(FunctionMetadataManager functionMetadataManager)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        parametricTypeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::instantiateParametricType));
        addAllTypes();
    }

    @Override
    public Optional<Type> getType(TypeSignature typeSignature)
    {
        Type type = types.get(typeSignature);
        if (type != null) {
            return Optional.of(type);
        }
        try {
            // Todo: Functions are getting parsed with char type signatures in it, native execution does not support char type signatures.
            // Once support for native function namespace manager is merged, clean this up.
            if (typeSignature.getBase().equals("char")) {
                typeSignature = new TypeSignature(
                        "varchar",
                        typeSignature.getParameters());
            }
            return Optional.ofNullable(parametricTypeCache.getUnchecked(new ExactTypeSignature(typeSignature)));
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public Map<String, ParametricType> getParametricTypes()
    {
        return parametricTypes;
    }

    private Type instantiateParametricType(ExactTypeSignature exactTypeSignature)
    {
        return functionMetadataManager.instantiateParametricType(exactTypeSignature);
    }

    private void addAllTypes()
    {
        for (Type type : TYPES) {
            addType(type);
        }

        for (ParametricType type : PARAMETRIC_TYPES) {
            addParametricType(type);
        }
    }

    private void addType(Type type)
    {
        requireNonNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }
}
