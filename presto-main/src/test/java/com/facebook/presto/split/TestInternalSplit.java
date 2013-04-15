package com.facebook.presto.split;

import com.facebook.presto.metadata.HostAddress;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestInternalSplit
{
    @Test
    public void testSerialization()
            throws Exception
    {
        InternalTableHandle tableHandle = new InternalTableHandle(new QualifiedTableName("abc", "xyz", "foo"));
        Map<InternalColumnHandle, String> filters = ImmutableMap.of(new InternalColumnHandle(13), "bar");
        InternalSplit expected = new InternalSplit(tableHandle, filters, ImmutableList.of(HostAddress.fromParts("127.0.0.1", 0)));

        JsonCodec<InternalSplit> codec = jsonCodec(InternalSplit.class);
        InternalSplit actual = codec.fromJson(codec.toJson(expected));

        assertEquals(actual.getFilters().size(), 1);
        Map.Entry<InternalColumnHandle, String> actualEntry = getOnlyElement(actual.getFilters().entrySet());
        Map.Entry<InternalColumnHandle, String> expectedEntry = getOnlyElement(expected.getFilters().entrySet());
        assertEquals(actualEntry.getKey().getColumnIndex(), expectedEntry.getKey().getColumnIndex());
    }
}
