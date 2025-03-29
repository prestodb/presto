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
package com.facebook.presto.sql.planner.plan;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.SimplePlanFragmentSerde;
import com.google.inject.Inject;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public class JsonCodecSimplePlanFragmentSerde
        implements SimplePlanFragmentSerde
{
    private final JsonCodec<SimplePlanFragment> codec;

    @Inject
    public JsonCodecSimplePlanFragmentSerde(JsonCodec<SimplePlanFragment> codec)
    {
        this.codec = requireNonNull(codec, "SimplePlanFragment JSON codec is null");
    }

    @Override
    public String serialize(SimplePlanFragment planFragment)
    {
        return new String(codec.toBytes(planFragment), StandardCharsets.UTF_8);
    }

    @Override
    public SimplePlanFragment deserialize(String value)
    {
        return codec.fromBytes(value.getBytes(StandardCharsets.UTF_8));
    }
}
