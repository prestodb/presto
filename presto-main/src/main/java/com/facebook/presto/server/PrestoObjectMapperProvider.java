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
package com.facebook.presto.server;

import com.facebook.airlift.json.ObjectMapperProvider;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamWriteConstraints;
import com.google.inject.Inject;

/**
 * Presto-owned {@link ObjectMapperProvider} that configures the central {@link JsonFactory}
 * with relaxed Jackson 2.18+ stream constraints for both reading and writing.
 *
 * <p>Jackson 2.18 introduced two new caps that are too restrictive for Presto workloads:
 * <ul>
 *   <li>{@link StreamReadConstraints} default {@code maxNameLength=50,000}: breaks
 *       {@code JsonCodec<PlanFragment>} on workers when {@code Assignments} map keys
 *       ({@code VariableReferenceExpression} names) exceed that length for complex
 *       projection chains over deeply nested struct schemas.</li>
 *   <li>{@link StreamWriteConstraints} default {@code maxNestingDepth=1,000}: breaks
 *       serialization of deeply nested {@code RowExpression} trees (e.g. 600-term additive
 *       chains from LightGBM calibrators that nest ~1,200 levels).</li>
 * </ul>
 *
 * <p>Wired via {@code Modules.override(new JsonModule()).with(...)} in both
 * {@code PrestoServer} and {@code TestingPrestoServer}, replacing the default
 * {@code JsonObjectMapperProvider} binding from {@code JsonModule} without
 * introducing a duplicate binding. Every {@link com.fasterxml.jackson.databind.ObjectMapper}
 * obtained through Guice — and therefore every Guice-backed
 * {@link com.facebook.airlift.json.JsonCodec} — inherits the relaxed constraints.
 */
public class PrestoObjectMapperProvider
        extends ObjectMapperProvider
{
    @Inject
    public PrestoObjectMapperProvider()
    {
        super(JsonFactory.builder()
                .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxNameLength(Integer.MAX_VALUE)
                        .build())
                .streamWriteConstraints(StreamWriteConstraints.builder()
                        .maxNestingDepth(Integer.MAX_VALUE)
                        .build())
                .build());
    }
}
