package com.facebook.presto.hive;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestHiveColumnHandle
{
    private final JsonCodec<HiveColumnHandle> codec = JsonCodec.jsonCodec(HiveColumnHandle.class);

    @Test
    public void testRoundTrip()
    {
        HiveColumnHandle expected = new HiveColumnHandle("client", "name", 42, HiveType.FLOAT, 88, true);

        String json = codec.toJson(expected);
        HiveColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getClientId(), expected.getClientId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getHiveType(), expected.getHiveType());
        assertEquals(actual.getHiveColumnIndex(), expected.getHiveColumnIndex());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
    }
}
