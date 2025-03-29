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
package com.facebook.presto.sidecar.nativechecker;

import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.inject.Inject;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class SimplePlanFragmentSerializer
        extends JsonSerializer<SimplePlanFragment>
{
    private final SimplePlanFragmentSerde simplePlanFragmentSerde;

    @Inject
    public SimplePlanFragmentSerializer(SimplePlanFragmentSerde simplePlanFragmentSerde)
    {
        this.simplePlanFragmentSerde = requireNonNull(simplePlanFragmentSerde, "planNodeSerde is null");
    }

    @Override
    public void serialize(SimplePlanFragment planNode, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException
    {
        jsonGenerator.writeRawValue(simplePlanFragmentSerde.serialize(planNode));
    }

    @Override
    public void serializeWithType(SimplePlanFragment planNode, JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer)
            throws IOException
    {
        serialize(planNode, jsonGenerator, serializerProvider);
    }
}
