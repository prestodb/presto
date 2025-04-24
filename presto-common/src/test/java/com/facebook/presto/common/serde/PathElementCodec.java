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
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.presto.common.Subfield;

public class PathElementCodec
{
    private final ThriftCodecManager codecManager;

    public PathElementCodec(ThriftCodecManager codecManager)
    {
        this.codecManager = codecManager;
    }

    public Subfield.PathElement deserialize(TProtocolReader protocol)
            throws Exception
    {
        protocol.readStructBegin();
        protocol.readFieldBegin();
        byte typeId = protocol.readByte();
        System.out.println("typeId: " + typeId);
        protocol.readFieldEnd();

        switch (typeId) {
            case 1:
                return codecManager.getCodec(Subfield.NestedField.class).read(protocol);
            case 2:
                return codecManager.getCodec(Subfield.LongSubscript.class).read(protocol);
        }

        throw new IllegalArgumentException("unknown type");
    }
}
