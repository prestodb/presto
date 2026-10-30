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
package com.facebook.presto.metadata;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProvider;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform.TRANSFORM_PROPERTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class MaterializedViewPartitionTransformRegistry
{
    private final CatalogSchemaName defaultNamespace;
    private final Map<String, MaterializedViewPartitionTransformProvider> providersByFunctionName = new ConcurrentHashMap<>();
    private final Map<String, MaterializedViewPartitionTransformProvider> providersByTransformId = new ConcurrentHashMap<>();

    public MaterializedViewPartitionTransformRegistry(CatalogSchemaName defaultNamespace)
    {
        this.defaultNamespace = requireNonNull(defaultNamespace, "defaultNamespace is null");
    }

    public synchronized void addProvider(MaterializedViewPartitionTransformProvider provider)
    {
        requireNonNull(provider, "provider is null");
        Set<String> functionNames = canonicalize(provider.getFunctionNames(), "functionNames");
        Set<String> transformIds = canonicalize(provider.getTransformIds(), "transformIds");
        for (String functionName : functionNames) {
            if (providersByFunctionName.containsKey(functionName)) {
                throw new IllegalArgumentException(format("Materialized view partition transform function is already registered: %s", functionName));
            }
        }
        for (String transformId : transformIds) {
            if (providersByTransformId.containsKey(transformId)) {
                throw new IllegalArgumentException(format("Materialized view partition transform ID is already registered: %s", transformId));
            }
        }
        functionNames.forEach(functionName -> providersByFunctionName.put(functionName, provider));
        transformIds.forEach(transformId -> providersByTransformId.put(transformId, provider));
    }

    public Optional<MaterializedViewPartitionTransformProvider> getProvider(FunctionMetadata function)
    {
        requireNonNull(function, "function is null");
        CatalogSchemaName namespace = function.getName().getCatalogSchemaName();
        if (!namespace.equals(JAVA_BUILTIN_NAMESPACE) && !namespace.equals(defaultNamespace)) {
            return Optional.empty();
        }
        MaterializedViewPartitionTransformProvider provider = providersByFunctionName.get(canonicalize(function.getName().getObjectName()));
        if (provider == null) {
            return Optional.empty();
        }
        if (function.getFunctionKind() != SCALAR) {
            throw new IllegalArgumentException(format("Materialized view partition transform function must be scalar: %s", function.getName()));
        }
        if (!function.isDeterministic()) {
            throw new IllegalArgumentException(format("Materialized view partition transform function must be deterministic: %s", function.getName()));
        }
        if (function.isCalledOnNullInput()) {
            throw new IllegalArgumentException(format("Materialized view partition transform function must return null on null input: %s", function.getName()));
        }
        return Optional.of(provider);
    }

    public Optional<MaterializedViewPartitionTransform> decode(Map<String, String> properties)
    {
        requireNonNull(properties, "properties is null");
        String transformId = properties.get(TRANSFORM_PROPERTY);
        if (transformId == null) {
            return Optional.empty();
        }
        String canonicalTransformId = canonicalize(transformId);
        MaterializedViewPartitionTransformProvider provider = providersByTransformId.get(canonicalTransformId);
        if (provider == null) {
            throw new IllegalArgumentException(format("Unknown materialized view partition transform: %s", transformId));
        }
        MaterializedViewPartitionTransform transform = requireNonNull(provider.decode(properties), "decoded transform is null");
        if (!canonicalTransformId.equals(canonicalize(transform.getTransformId()))) {
            throw new IllegalArgumentException(format("Decoded materialized view partition transform ID does not match %s", transformId));
        }
        return Optional.of(transform);
    }

    private static Set<String> canonicalize(Set<String> values, String name)
    {
        requireNonNull(values, name + " is null");
        if (values.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        Set<String> canonicalValues = new LinkedHashSet<>();
        for (String value : values) {
            String canonicalValue = canonicalize(value);
            if (!canonicalValues.add(canonicalValue)) {
                throw new IllegalArgumentException(format("Duplicate %s entry: %s", name, value));
            }
        }
        return canonicalValues;
    }

    private static String canonicalize(String value)
    {
        String canonicalValue = requireNonNull(value, "value is null").trim().toLowerCase(Locale.ENGLISH);
        if (canonicalValue.isEmpty()) {
            throw new IllegalArgumentException("value is empty");
        }
        return canonicalValue;
    }
}
