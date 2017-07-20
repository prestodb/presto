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
package com.facebook.presto.decoder.thrift;

import io.airlift.log.Logger;
import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ThriftGenericRow implements TBase<ThriftGenericRow, ThriftGenericRow.Fields>
{
    private static final Logger log = Logger.get(ThriftGenericRow.class);
    private final Map<Short, Object> values = new HashMap<>();
    private byte[] buf = null;
    private int off = 0;
    private int len = 0;

    public ThriftGenericRow() {}

    public ThriftGenericRow(Map<Short, Object> values)
    {
        this.values.putAll(values);
    }

    public class Fields implements TFieldIdEnum
    {
        private final short thriftId;
        private final String fieldName;

        Fields(short thriftId, String fieldName)
        {
            this.thriftId = thriftId;
            this.fieldName = fieldName;
        }

        public short getThriftFieldId()
        {
            return thriftId;
        }

        public String getFieldName()
        {
            return fieldName;
        }
    }

    public void read(TProtocol iprot) throws TException
    {
        TTransport trans = iprot.getTransport();
        buf = trans.getBuffer();
        off = trans.getBufferPosition();
        TProtocolUtil.skip(iprot, TType.STRUCT);
        len = trans.getBufferPosition() - off;
    }

    public void parse() throws TException
    {
        parse(null);
    }

    public void parse(short[] thriftIds) throws TException
    {
        Set<Short> idSet = thriftIds == null ? null : new HashSet(Arrays.asList(ArrayUtils.toObject(thriftIds)));
        TMemoryInputTransport trans = new TMemoryInputTransport(buf, off, len);
        TBinaryProtocol iprot = new TBinaryProtocol(trans);
        TField field;
        iprot.readStructBegin();
        while (true) {
            field = iprot.readFieldBegin();
            if (field.type == TType.STOP) {
                break;
            }
            if (idSet != null && !idSet.remove(Short.valueOf(field.id))) {
                TProtocolUtil.skip(iprot, field.type);
            }
            else {
                values.put(field.id, readElem(iprot, field.type));
            }
            iprot.readFieldEnd();
        }
        iprot.readStructEnd();
    }

    private Object readElem(TProtocol iprot, byte type) throws TException
    {
        switch (type) {
            case TType.BOOL:
                return iprot.readBool();
            case TType.BYTE:
                return iprot.readByte();
            case TType.I16:
                return iprot.readI16();
            case TType.ENUM:
            case TType.I32:
                return iprot.readI32();
            case TType.I64:
                return iprot.readI64();
            case TType.DOUBLE:
                return iprot.readDouble();
            case TType.STRING:
                return iprot.readString();
            case TType.STRUCT:
                return readStruct(iprot);
            case TType.LIST:
                return readList(iprot);
            case TType.SET:
                return readSet(iprot);
            case TType.MAP:
                return readMap(iprot);
            default:
                TProtocolUtil.skip(iprot, type);
                return null;
        }
    }

    private Object readStruct(TProtocol iprot) throws TException
    {
        ThriftGenericRow elem = new ThriftGenericRow();
        elem.read(iprot);
        elem.parse();
        return elem;
    }

    private Object readList(TProtocol iprot) throws TException
    {
        TList ilist = iprot.readListBegin();
        List<Object> listValue = new ArrayList<>();
        for (int i = 0; i < ilist.size; ++i) {
            listValue.add(readElem(iprot, ilist.elemType));
        }
        iprot.readListEnd();
        return listValue;
    }

    private Object readSet(TProtocol iprot) throws TException
    {
        TSet iset = iprot.readSetBegin();
        List<Object> setValue = new ArrayList<>();
        for (int i = 0; i < iset.size; ++i) {
            setValue.add(readElem(iprot, iset.elemType));
        }
        iprot.readSetEnd();
        return setValue;
    }

    private Object readMap(TProtocol iprot) throws TException
    {
        TMap imap = iprot.readMapBegin();
        Map<Object, Object> mapValue = new HashMap<>();
        for (int i = 0; i < imap.size; ++i) {
            mapValue.put(readElem(iprot, imap.keyType), readElem(iprot, imap.valueType));
        }
        iprot.readMapEnd();
        return mapValue;
    }

    public Object getFieldValueForThriftId(short thriftId)
    {
        return values.get(thriftId);
    }

    public ThriftGenericRow deepCopy()
    {
        return new ThriftGenericRow(values);
    }

    public void clear() {}

    public Fields fieldForId(int fieldId)
    {
        return new Fields((short) fieldId, "dummy");
    }

    public Object getFieldValue(Fields field)
    {
        return values.get(field.thriftId);
    }

    public boolean isSet(Fields field)
    {
        return values.containsKey(field.getThriftFieldId());
    }

    public void setFieldValue(Fields field, Object value)
    {
        values.put(field.getThriftFieldId(), value);
    }

    public void write(TProtocol oprot) throws TException
    {
        throw new UnsupportedOperationException("ThriftGenericRow.write is not supported.");
    }

    public Map<Short, Object> getValues()
    {
        return values;
    }

    public int compareTo(ThriftGenericRow other)
    {
        throw new UnsupportedOperationException("ThriftGenericRow.compareTo is not supported.");
    }
}
