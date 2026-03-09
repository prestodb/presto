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
package com.facebook.presto.util;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.ValueSet;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.StreamWriteConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.airlift.slice.Slice;

/**
 * Utility class for configuring ObjectMapper instances to handle Jackson 2.18.6 serialization issues.
 * This includes:
 * - Registering Jdk8Module for Optional support
 * - Configuring serialization features to handle circular references
 * - Adding MixIns to ignore problematic getter methods
 * - Increasing nesting depth limit for complex Presto plan structures
 */
public final class JsonObjectMapperUtils
{
    private JsonObjectMapperUtils() {}

    /**
     * Configures an ObjectMapper with settings required for Jackson 2.18.6 compatibility.
     * This method modifies the provided ObjectMapper in place.
     *
     * @param objectMapper the ObjectMapper to configure
     * @return the configured ObjectMapper (same instance as input)
     */
    public static ObjectMapper configureObjectMapperForJacksonFix(ObjectMapper objectMapper)
    {
        // Configure serialization features
        objectMapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        // Register Jdk8Module for Optional support
        objectMapper.registerModule(new Jdk8Module());

        // Increase nesting depth limit for complex plan structures
        objectMapper.getFactory().setStreamWriteConstraints(
                StreamWriteConstraints.builder()
                        .maxNestingDepth(2000)
                        .build());

        // Add MixIns to ignore methods that cause circular references or throw exceptions
        objectMapper.addMixIn(Block.class, IgnoreProblematicMethodsMixIn.class);
        objectMapper.addMixIn(Slice.class, IgnoreProblematicMethodsMixIn.class);
        objectMapper.addMixIn(ValueSet.class, IgnoreProblematicMethodsMixIn.class);
        objectMapper.addMixIn(SortedRangeSet.class, IgnoreProblematicMethodsMixIn.class);
        objectMapper.addMixIn(Range.class, IgnoreProblematicMethodsMixIn.class);
        objectMapper.addMixIn(Domain.class, IgnoreProblematicMethodsMixIn.class);

        return objectMapper;
    }

    /**
     * Creates a new ObjectMapper configured for Jackson 2.18.6 compatibility.
     *
     * @return a new configured ObjectMapper
     */
    public static ObjectMapper createConfiguredObjectMapper()
    {
        return configureObjectMapperForJacksonFix(new ObjectMapper());
    }

    /**
     * MixIn class to ignore methods that cause circular references or throw exceptions during serialization.
     * This single MixIn is applied to multiple classes (Block, Slice, ValueSet, etc.) to avoid duplication.
     */
    private abstract static class IgnoreProblematicMethodsMixIn
    {
        // Ignore Block.getLoadedBlock() - causes circular reference
        @JsonIgnore
        abstract Object getLoadedBlock();

        // Ignore Slice.getOutput() - causes circular reference with BasicSliceOutput
        @JsonIgnore
        abstract Object getOutput();

        // Ignore ValueSet.getDiscreteValues() - throws UnsupportedOperationException
        @JsonIgnore
        abstract Object getDiscreteValues();

        // Ignore SortedRangeSet.getSingleValue() - throws IllegalStateException
        // Ignore Range.getSingleValue() - throws IllegalStateException
        // Ignore Domain.getSingleValue() - throws IllegalStateException
        @JsonIgnore
        abstract Object getSingleValue();

        // Ignore SortedRangeSet.getValuesProcessor() - causes issues
        @JsonIgnore
        abstract Object getValuesProcessor();

        // Ignore Domain.getNullableSingleValue() - throws IllegalStateException
        @JsonIgnore
        abstract Object getNullableSingleValue();
    }
}
