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
package com.facebook.presto.spi.materializedview;

import com.facebook.presto.spi.function.FunctionMetadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Supplies explicitly supported materialized view partition transforms. Function names must be
 * globally unique among installed providers. Implementations may be invoked concurrently and
 * must be thread-safe.
 *
 * A registered function name is also persisted as the transform name in materialized view
 * metadata. The provider must retain backward-compatible decoding and semantics for every name
 * it has stored.
 */
public interface MaterializedViewPartitionTransformProvider
{
    /**
     * Returns the unqualified, case-insensitive function names owned by this provider. A provider
     * owns every supported overload of each name.
     */
    Set<String> getFunctionNames();

    /**
     * Analyzes a resolved call to an owned function. The provider must validate the argument
     * count, types, constants and any function-specific restrictions, and throw
     * {@link IllegalArgumentException} when the call is not a supported partition transform.
     */
    MaterializedViewPartitionTransform analyze(
            FunctionMetadata function,
            List<MaterializedViewPartitionTransformArgument> arguments);

    /**
     * Reconstructs a transform from metadata previously returned by
     * {@link MaterializedViewPartitionTransform#encode()}.
     */
    MaterializedViewPartitionTransform decode(Map<String, String> properties);
}
