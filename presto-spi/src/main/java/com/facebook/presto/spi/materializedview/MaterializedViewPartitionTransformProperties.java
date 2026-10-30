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

import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

/**
 * Structural properties of an analyzed partition transform. Version one transforms are
 * deterministic, null preserving and monotonically non-decreasing. A transform either preserves
 * one source partition value or folds a contiguous range of source values into one target value.
 */
public final class MaterializedViewPartitionTransformProperties
{
    public enum MappingCardinality
    {
        IDENTITY,
        MANY_TO_ONE
    }

    private final int sourceArgumentIndex;
    private final Set<Integer> constantArgumentIndexes;
    private final MappingCardinality mappingCardinality;
    private final int formatVersion;

    public MaterializedViewPartitionTransformProperties(
            int sourceArgumentIndex,
            Set<Integer> constantArgumentIndexes,
            MappingCardinality mappingCardinality,
            int formatVersion)
    {
        if (sourceArgumentIndex < 0) {
            throw new IllegalArgumentException("sourceArgumentIndex is negative");
        }
        requireNonNull(constantArgumentIndexes, "constantArgumentIndexes is null");
        if (!constantArgumentIndexes.stream().allMatch(index -> index != null && index >= 0)) {
            throw new IllegalArgumentException("constant argument index is null or negative");
        }
        if (constantArgumentIndexes.contains(sourceArgumentIndex)) {
            throw new IllegalArgumentException("source argument cannot also be constant");
        }
        if (formatVersion <= 0) {
            throw new IllegalArgumentException("formatVersion must be positive");
        }
        this.sourceArgumentIndex = sourceArgumentIndex;
        this.constantArgumentIndexes = unmodifiableSet(new LinkedHashSet<>(constantArgumentIndexes));
        this.mappingCardinality = requireNonNull(mappingCardinality, "mappingCardinality is null");
        this.formatVersion = formatVersion;
    }

    public int getSourceArgumentIndex()
    {
        return sourceArgumentIndex;
    }

    public Set<Integer> getConstantArgumentIndexes()
    {
        return constantArgumentIndexes;
    }

    public MappingCardinality getMappingCardinality()
    {
        return mappingCardinality;
    }

    public int getFormatVersion()
    {
        return formatVersion;
    }
}
