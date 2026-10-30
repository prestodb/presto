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
import java.util.Optional;
import java.util.Set;

/**
 * Supplies explicitly supported materialized view partition transforms. Function names and
 * transform identifiers must be globally unique among installed providers. Implementations may
 * be invoked concurrently and must be thread-safe.
 *
 * Transform identifiers are persisted in materialized view metadata. An identifier must retain
 * backward-compatible decoding and transform semantics and must not be reassigned to a different
 * transform.
 */
public interface MaterializedViewPartitionTransformProvider
{
    Set<String> getTransformIds();

    Set<String> getFunctionNames();

    Optional<MaterializedViewPartitionTransform> analyze(
            FunctionMetadata function,
            List<MaterializedViewPartitionTransformArgument> arguments);

    MaterializedViewPartitionTransform decode(Map<String, String> properties);
}
