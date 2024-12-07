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
import com.facebook.presto.common.type.ExactTypeSignature;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.type.TypesProvider;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.DATE;
import static com.facebook.presto.common.type.StandardTypes.DECIMAL;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.FUNCTION;
import static com.facebook.presto.common.type.StandardTypes.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static com.facebook.presto.common.type.StandardTypes.IPADDRESS;
import static com.facebook.presto.common.type.StandardTypes.IPPREFIX;
import static com.facebook.presto.common.type.StandardTypes.JSON;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.REAL;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.StandardTypes.UUID;
import static com.facebook.presto.common.type.StandardTypes.VARBINARY;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class NativeTypeManager
        implements TypesProvider
{
    private static final Logger log = Logger.get(NativeTypeManager.class);

    private static final Set<String> NATIVE_ENGINE_SUPPORTED_TYPES =
            ImmutableSet.of(
                    BIGINT,
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
                    INTERVAL_DAY_TO_SECOND,
                    INTERVAL_YEAR_TO_MONTH,
                    VARCHAR);

    private static final Set<String> NATIVE_ENGINE_SUPPORTED_PARAMETRIC_TYPES =
            ImmutableSet.of(
                    ARRAY,
                    DECIMAL,
                    MAP,
                    ROW,
                    FUNCTION);

    private final TypeManager typeManager;
    private final LoadingCache<ExactTypeSignature, Type> parametricTypeCache;
    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    @Inject
    public NativeTypeManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        parametricTypeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::instantiateParametricType));
        addAllTypes(
                filterSupportedTypes(NATIVE_ENGINE_SUPPORTED_TYPES, typeManager.getTypes(), Type::getDisplayName),
                filterSupportedTypes(NATIVE_ENGINE_SUPPORTED_PARAMETRIC_TYPES, typeManager.getParametricTypes(), ParametricType::getName));
    }

    @Override
    public Optional<Type> getType(TypeSignature typeSignature)
    {
        // Todo: Functions are getting parsed with char/parametrized varchar type signatures in it, native execution does not support char type signatures.
        // Once support for native function namespace manager is merged, clean this up.
        if (typeSignature.getBase().equals("char") ||
                (typeSignature.getBase().equals("varchar") && !typeSignature.getParameters().isEmpty())) {
            typeSignature = parseTypeSignature("varchar");
        }
        Type type = types.get(typeSignature);
        if (type != null) {
            return Optional.of(type);
        }
        try {
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
        return typeManager.instantiateParametricType(exactTypeSignature);
    }

    private void addAllTypes(List<Type> typesList, List<ParametricType> parametricTypesList)
    {
        for (Type type : typesList) {
            addType(type);
        }
        // todo: Fix this hack
        // Native engine does not support parameterized varchar, and varchar isn't in the lists of types returned from the engine
        addType(VarcharType.VARCHAR);

        for (ParametricType type : parametricTypesList) {
            addParametricType(type);
        }
    }

    private void addType(Type type)
    {
        requireNonNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    private void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }

    private static <T> List<T> filterSupportedTypes(
            Set<String> actualTypes,
            List<T> types,
            Function<T, String> typeSignatureExtractor)
    {
        return types.stream()
                .filter(type -> actualTypes.contains(typeSignatureExtractor.apply(type)))
                .collect(toImmutableList());
    }
}
