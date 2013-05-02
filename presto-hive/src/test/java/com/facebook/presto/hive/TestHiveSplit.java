package com.facebook.presto.hive;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class TestHiveSplit
{
    private final JsonCodec<HiveSplit> codec = JsonCodec.jsonCodec(HiveSplit.class);

    @Test
    public void testJsonRoundTrip()
    {
        Properties schema = new Properties();
        schema.put("foo", "bar");
        schema.put("bar", "baz");

        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey("a", HiveType.STRING, "apple"), new HivePartitionKey("b", HiveType.LONG, "42"));
        ImmutableList<HostAddress> addresses = ImmutableList.of(HostAddress.fromParts("127.0.0.1", 44), HostAddress.fromParts("127.0.0.1", 45));
        HiveSplit expected = new HiveSplit("clientId", "partitionId", true, "path", 42, 88, schema, partitionKeys, addresses);

        String json = codec.toJson(expected);
        HiveSplit actual = codec.fromJson(json);

        assertEquals(actual.getClientId(), expected.getClientId());
        assertEquals(actual.getPartitionId(), expected.getPartitionId());
        assertEquals(actual.isLastSplit(), expected.isLastSplit());
        assertEquals(actual.getPath(), expected.getPath());
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getPartitionKeys(), expected.getPartitionKeys());
        assertEquals(actual.getAddresses(), expected.getAddresses());
    }
}
