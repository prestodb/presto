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
