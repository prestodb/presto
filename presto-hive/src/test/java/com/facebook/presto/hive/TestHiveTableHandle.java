package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestHiveTableHandle {
    private final JsonCodec<HiveTableHandle> codec = JsonCodec.jsonCodec(HiveTableHandle.class);

    @Test
    public void testRoundTrip()
    {
        HiveTableHandle expected = new HiveTableHandle("client", new SchemaTableName("schema", "table"));

        String json = codec.toJson(expected);
        HiveTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getClientId(), expected.getClientId());
        assertEquals(actual.getTableName(), expected.getTableName());
    }
}
