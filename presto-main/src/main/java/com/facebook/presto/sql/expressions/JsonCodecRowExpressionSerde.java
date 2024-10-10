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
package com.facebook.presto.sql.expressions;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.relation.RowExpression;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public class JsonCodecRowExpressionSerde
        implements RowExpressionSerde
{
    private final JsonCodec<RowExpression> codec;

    @Inject
    public JsonCodecRowExpressionSerde(JsonCodec<RowExpression> codec)
    {
        this.codec = requireNonNull(codec, "codec is null");
    }

    @Override
    public String serialize(RowExpression expression)
    {
        return new String(codec.toBytes(expression), StandardCharsets.UTF_8);
    }

    @Override
    public RowExpression deserialize(String data)
    {
        return codec.fromBytes(data.getBytes(StandardCharsets.UTF_8));
    }
}
