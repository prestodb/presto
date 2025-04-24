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
package com.facebook.presto.common.serde;

import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TField;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.TStruct;
import com.facebook.drift.protocol.TType;
import com.facebook.presto.common.Subfield;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestPathElement
{
    private ThriftCodecManager codecManager;
    private PathElementCodec pathElementCodec;

    @BeforeMethod
    public void setup()
    {
        codecManager = new ThriftCodecManager();
        pathElementCodec = new PathElementCodec(codecManager);
    }

    @Test
    public void testNestedField()
            throws Exception
    {
        Subfield.NestedField nestedField = new Subfield.NestedField("test");

        TMemoryBuffer transport = new TMemoryBuffer(1024);
        TProtocolWriter writer = new TBinaryProtocol(transport);

        writer.writeStructBegin(new TStruct("PathElement"));
        writer.writeFieldBegin(new TField("typeId", TType.BYTE, (short) 1));
        writer.writeByte((byte) 1);
        writer.writeFieldEnd();
        codecManager.getCodec(Subfield.NestedField.class).write(nestedField, writer);
        writer.writeStructEnd();

        // Deserialize
        TProtocolReader reader = new TBinaryProtocol(transport);
        Subfield.PathElement deserializedNestedField = pathElementCodec.deserialize(reader);

        System.out.println(deserializedNestedField.getClass().getName());
        assertEquals(((Subfield.NestedField) deserializedNestedField).getName(), nestedField.getName());
    }

    @Test
    public void testMultiplePathElement()
            throws Exception
    {
        Subfield.NestedField nestedField = new Subfield.NestedField("test");
        Subfield.LongSubscript longSubscript = new Subfield.LongSubscript(1L);

        TMemoryBuffer transport = new TMemoryBuffer(1024);
        TProtocolWriter writer = new TBinaryProtocol(transport);

        writer.writeI32(2);
        writer.writeStructBegin(new TStruct("PathElement"));
        writer.writeFieldBegin(new TField("typeId", TType.BYTE, (short) 1));
        writer.writeByte((byte) 1);
        writer.writeFieldEnd();
        codecManager.getCodec(Subfield.NestedField.class).write(nestedField, writer);
        writer.writeStructEnd();

        writer.writeStructBegin(new TStruct("PathElement"));
        writer.writeFieldBegin(new TField("typeId", TType.BYTE, (short) 1));
        writer.writeByte((byte) 2);
        writer.writeFieldEnd();
        codecManager.getCodec(Subfield.LongSubscript.class).write(longSubscript, writer);
        writer.writeStructEnd();

        // Deserialize
        TProtocolReader reader = new TBinaryProtocol(transport);
        int count = reader.readI32();
        for (int i = 0; i < count; i++) {
            Subfield.PathElement deserializedNestedField = pathElementCodec.deserialize(reader);
            System.out.println(deserializedNestedField.getClass().getName());

            if (deserializedNestedField instanceof Subfield.NestedField) {
                assertEquals(((Subfield.NestedField) deserializedNestedField).getName(), nestedField.getName());
            }
            else if (deserializedNestedField instanceof Subfield.LongSubscript) {
                assertEquals(((Subfield.LongSubscript) deserializedNestedField).getIndex(), longSubscript.getIndex());
            }
        }
    }
}
