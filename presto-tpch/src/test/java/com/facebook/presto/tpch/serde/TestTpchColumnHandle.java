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
