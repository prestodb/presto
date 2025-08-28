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

import com.facebook.presto.common.type.DistinctTypeInfo;
import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.type.UnknownTypeException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
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
import static com.facebook.presto.common.type.StandardTypes.GEOMETRY;
import static com.facebook.presto.common.type.StandardTypes.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static com.facebook.presto.common.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static com.facebook.presto.common.type.StandardTypes.IPADDRESS;
import static com.facebook.presto.common.type.StandardTypes.IPPREFIX;
import static com.facebook.presto.common.type.StandardTypes.JSON;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.QDIGEST;
import static com.facebook.presto.common.type.StandardTypes.REAL;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TDIGEST;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.StandardTypes.UNKNOWN;
import static com.facebook.presto.common.type.StandardTypes.UUID;
import static com.facebook.presto.common.type.StandardTypes.VARBINARY;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class NativeTypeManager
        implements TypeManager
{
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
                    VARCHAR,
                    UNKNOWN,
                    GEOMETRY);

    private static final Set<String> NATIVE_ENGINE_SUPPORTED_PARAMETRIC_TYPES =
            ImmutableSet.of(
                    ARRAY,
                    DECIMAL,
                    MAP,
                    QDIGEST,
                    ROW,
                    TDIGEST,
                    FunctionType.NAME);

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
                .build(CacheLoader.from(this::instantiateParametricType));
        addAllTypes(
                filterSupportedTypes(NATIVE_ENGINE_SUPPORTED_TYPES, typeManager.getTypes(), Type::getDisplayName),
                filterSupportedTypes(
                        NATIVE_ENGINE_SUPPORTED_PARAMETRIC_TYPES,
                        new ArrayList<>(typeManager.getParametricTypes()),
                        ParametricType::getName));
    }

    @Override
    public Type getType(TypeSignature typeSignature)
    {
        // Todo: Fix this hack, native execution does not support parameterized varchar type signatures.
        if (typeSignature.getBase().equals(VARCHAR)) {
            typeSignature = createUnboundedVarcharType().getTypeSignature();
        }
        Type type = types.get(typeSignature);
        if (type != null) {
            return type;
        }
        try {
            return parametricTypeCache.getUnchecked(new ExactTypeSignature(typeSignature));
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public boolean hasType(TypeSignature typeSignature)
    {
        try {
            getType(typeSignature);
            return true;
        }
        catch (UnknownTypeException e) {
            return false;
        }
    }

    @Override
    public Collection<ParametricType> getParametricTypes()
    {
        return parametricTypes.values();
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.copyOf(types.values());
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canCoerce(Type actualType, Type expectedType)
    {
        throw new UnsupportedOperationException();
    }

    private void addAllTypes(List<Type> typesList, List<ParametricType> parametricTypesList)
    {
        typesList.forEach(this::addType);
        // todo: Fix this hack
        // Native engine does not support parameterized varchar, and varchar isn't in the lists of types returned from the engine
        addType(VarcharType.VARCHAR);

        parametricTypesList.forEach(this::addParametricType);
    }

    private Type instantiateParametricType(ExactTypeSignature exactTypeSignature)
    {
        return typeManager.instantiateParametricType(exactTypeSignature.getTypeSignature());
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

    /**
     * TypeSignature has overridden equals(). Here, we compare exact signature of any underlying distinct
     * types. Some distinct types may have extra information on their lazily loaded parents, and same parent
     * information is compared in equals(). This is needed to cache types in parametricTypesCache.
     */
    private static class ExactTypeSignature
    {
        private final TypeSignature typeSignature;

        public ExactTypeSignature(TypeSignature typeSignature)
        {
            this.typeSignature = typeSignature;
        }

        public TypeSignature getTypeSignature()
        {
            return typeSignature;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(typeSignature);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ExactTypeSignature other = (ExactTypeSignature) o;
            return equals(typeSignature, other.typeSignature);
        }

        private static boolean equals(TypeSignature left, TypeSignature right)
        {
            if (!left.equals(right)) {
                return false;
            }

            if (left.isDistinctType() && right.isDistinctType()) {
                return equals(left.getDistinctTypeInfo(), right.getDistinctTypeInfo());
            }
            int index = 0;
            for (TypeSignatureParameter leftParameter : left.getParameters()) {
                TypeSignatureParameter rightParameter = right.getParameters().get(index++);
                if (!leftParameter.getKind().equals(rightParameter.getKind())) {
                    return false;
                }

                switch (leftParameter.getKind()) {
                    case TYPE:
                        if (!equals(leftParameter.getTypeSignature(), rightParameter.getTypeSignature())) {
                            return false;
                        }
                        break;
                    case NAMED_TYPE:
                        if (!equals(leftParameter.getNamedTypeSignature().getTypeSignature(), rightParameter.getNamedTypeSignature().getTypeSignature())) {
                            return false;
                        }
                        break;
                    case DISTINCT_TYPE:
                        if (!equals(leftParameter.getDistinctTypeInfo(), rightParameter.getDistinctTypeInfo())) {
                            return false;
                        }
                        break;
                }
            }
            return true;
        }

        private static boolean equals(DistinctTypeInfo left, DistinctTypeInfo right)
        {
            return Objects.equals(left.getName(), right.getName()) &&
                    Objects.equals(left.getBaseType(), right.getBaseType()) &&
                    Objects.equals(left.isOrderable(), right.isOrderable()) &&
                    Objects.equals(left.getTopMostAncestor(), right.getTopMostAncestor()) &&
                    Objects.equals(left.getOtherAncestors(), right.getOtherAncestors());
        }
    }
}
