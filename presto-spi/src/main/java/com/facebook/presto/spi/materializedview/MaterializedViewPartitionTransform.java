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

import com.facebook.presto.spi.relation.ConstantExpression;

import java.util.Map;

/**
 * An analyzed transform between a source partition column and a materialized view partition
 * column. Implementations must encode all configuration needed to reconstruct the transform.
 */
public interface MaterializedViewPartitionTransform
{
    String TRANSFORM_PROPERTY = "transform";

    /**
     * Returns all metadata needed to reconstruct this transform. The map must contain a
     * {@value #TRANSFORM_PROPERTY} entry matching a function name owned by the provider.
     */
    Map<String, String> encode();

    /**
     * Returns the exact source mapping represented by a target partition value. The mapping is
     * either one source value or one half-open source range. The implementation must throw
     * {@link IllegalArgumentException} when the value has the wrong type, is malformed or is not
     * canonical for the transform.
     */
    MaterializedViewPartitionMapping getSourceMapping(ConstantExpression targetValue);
}
