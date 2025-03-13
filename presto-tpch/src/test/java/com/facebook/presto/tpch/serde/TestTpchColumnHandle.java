package com.facebook.presto.tpch.serde;

import com.facebook.presto.common.experimental.ColumnHandleAdapter;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;

public class TestTpchColumnHandle
{
    @Test
    public void TestSelfSerDe()
    {
        TpchColumnHandle handle = new TpchColumnHandle("test-column", createVarcharType(1));

        byte[] bytes = ThriftSerializationRegistry.serialize(handle);

        TpchColumnHandle deserialized = (TpchColumnHandle) ThriftSerializationRegistry.deserialize(handle.getImplementationType(), bytes);
        assertEquals(deserialized, handle);
    }

    @Test
    public void TestInterfaceSerDe()
    {
        ColumnHandle handle = new TpchColumnHandle("test-column", createVarcharType(1));
        byte[] bytes = ColumnHandleAdapter.serialize(handle);

        TpchColumnHandle deserialized = (TpchColumnHandle) ColumnHandleAdapter.deserialize(bytes);
        assertEquals(deserialized, handle);
    }
}
