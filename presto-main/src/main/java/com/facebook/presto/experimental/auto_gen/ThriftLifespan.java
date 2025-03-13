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
/**
 * Autogenerated by Thrift Compiler (0.21.0)
 * <p>
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 * @generated
 */
package com.facebook.presto.experimental.auto_gen;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.21.0)", date = "2025-03-13")
public class ThriftLifespan
        implements org.apache.thrift.TBase<ThriftLifespan, ThriftLifespan._Fields>, java.io.Serializable, Cloneable, Comparable<ThriftLifespan>
{
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ThriftLifespan");

    private static final org.apache.thrift.protocol.TField GROUPED_FIELD_DESC = new org.apache.thrift.protocol.TField("grouped", org.apache.thrift.protocol.TType.BOOL, (short) 1);
    private static final org.apache.thrift.protocol.TField GROUP_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("groupId", org.apache.thrift.protocol.TType.I32, (short) 2);

    private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ThriftLifespanStandardSchemeFactory();
    private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ThriftLifespanTupleSchemeFactory();

    public boolean grouped; // required
    public int groupId; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields
            implements org.apache.thrift.TFieldIdEnum
    {
        GROUPED((short) 1, "grouped"),
        GROUP_ID((short) 2, "groupId");

        private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

        static {
            for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, or null if its not found.
         */
        @org.apache.thrift.annotation.Nullable
        public static _Fields findByThriftId(int fieldId)
        {
            switch (fieldId) {
                case 1: // GROUPED
                    return GROUPED;
                case 2: // GROUP_ID
                    return GROUP_ID;
                default:
                    return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception
         * if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId)
        {
            _Fields fields = findByThriftId(fieldId);
          if (fields == null) {
            throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
          }
            return fields;
        }

        /**
         * Find the _Fields constant that matches name, or null if its not found.
         */
        @org.apache.thrift.annotation.Nullable
        public static _Fields findByName(java.lang.String name)
        {
            return byName.get(name);
        }

        private final short _thriftId;
        private final java.lang.String _fieldName;

        _Fields(short thriftId, java.lang.String fieldName)
        {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        @Override
        public short getThriftFieldId()
        {
            return _thriftId;
        }

        @Override
        public java.lang.String getFieldName()
        {
            return _fieldName;
        }
    }

    // isset id assignments
    private static final int __GROUPED_ISSET_ID = 0;
    private static final int __GROUPID_ISSET_ID = 1;
    private byte __isset_bitfield = 0;
    public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;

    static {
        java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
        tmpMap.put(_Fields.GROUPED, new org.apache.thrift.meta_data.FieldMetaData("grouped", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
        tmpMap.put(_Fields.GROUP_ID, new org.apache.thrift.meta_data.FieldMetaData("groupId", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
        metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ThriftLifespan.class, metaDataMap);
    }

    public ThriftLifespan()
    {
    }

    public ThriftLifespan(
            boolean grouped,
            int groupId)
    {
        this();
        this.grouped = grouped;
        setGroupedIsSet(true);
        this.groupId = groupId;
        setGroupIdIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public ThriftLifespan(ThriftLifespan other)
    {
        __isset_bitfield = other.__isset_bitfield;
        this.grouped = other.grouped;
        this.groupId = other.groupId;
    }

    @Override
    public ThriftLifespan deepCopy()
    {
        return new ThriftLifespan(this);
    }

    @Override
    public void clear()
    {
        setGroupedIsSet(false);
        this.grouped = false;
        setGroupIdIsSet(false);
        this.groupId = 0;
    }

    public boolean isGrouped()
    {
        return this.grouped;
    }

    public ThriftLifespan setGrouped(boolean grouped)
    {
        this.grouped = grouped;
        setGroupedIsSet(true);
        return this;
    }

    public void unsetGrouped()
    {
        __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __GROUPED_ISSET_ID);
    }

    /** Returns true if field grouped is set (has been assigned a value) and false otherwise */
    public boolean isSetGrouped()
    {
        return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __GROUPED_ISSET_ID);
    }

    public void setGroupedIsSet(boolean value)
    {
        __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __GROUPED_ISSET_ID, value);
    }

    public int getGroupId()
    {
        return this.groupId;
    }

    public ThriftLifespan setGroupId(int groupId)
    {
        this.groupId = groupId;
        setGroupIdIsSet(true);
        return this;
    }

    public void unsetGroupId()
    {
        __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __GROUPID_ISSET_ID);
    }

    /** Returns true if field groupId is set (has been assigned a value) and false otherwise */
    public boolean isSetGroupId()
    {
        return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __GROUPID_ISSET_ID);
    }

    public void setGroupIdIsSet(boolean value)
    {
        __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __GROUPID_ISSET_ID, value);
    }

    @Override
    public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value)
    {
        switch (field) {
            case GROUPED:
                if (value == null) {
                    unsetGrouped();
                }
                else {
                    setGrouped((java.lang.Boolean) value);
                }
                break;

            case GROUP_ID:
                if (value == null) {
                    unsetGroupId();
                }
                else {
                    setGroupId((java.lang.Integer) value);
                }
                break;
        }
    }

    @org.apache.thrift.annotation.Nullable
    @Override
    public java.lang.Object getFieldValue(_Fields field)
    {
        switch (field) {
            case GROUPED:
                return isGrouped();

            case GROUP_ID:
                return getGroupId();
        }
        throw new java.lang.IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    @Override
    public boolean isSet(_Fields field)
    {
        if (field == null) {
            throw new java.lang.IllegalArgumentException();
        }

        switch (field) {
            case GROUPED:
                return isSetGrouped();
            case GROUP_ID:
                return isSetGroupId();
        }
        throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that)
    {
      if (that instanceof ThriftLifespan) {
        return this.equals((ThriftLifespan) that);
      }
        return false;
    }

    public boolean equals(ThriftLifespan that)
    {
      if (that == null) {
        return false;
      }
      if (this == that) {
        return true;
      }

        boolean this_present_grouped = true;
        boolean that_present_grouped = true;
        if (this_present_grouped || that_present_grouped) {
          if (!(this_present_grouped && that_present_grouped)) {
            return false;
          }
          if (this.grouped != that.grouped) {
            return false;
          }
        }

        boolean this_present_groupId = true;
        boolean that_present_groupId = true;
        if (this_present_groupId || that_present_groupId) {
          if (!(this_present_groupId && that_present_groupId)) {
            return false;
          }
          if (this.groupId != that.groupId) {
            return false;
          }
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int hashCode = 1;

        hashCode = hashCode * 8191 + ((grouped) ? 131071 : 524287);

        hashCode = hashCode * 8191 + groupId;

        return hashCode;
    }

    @Override
    public int compareTo(ThriftLifespan other)
    {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = java.lang.Boolean.compare(isSetGrouped(), other.isSetGrouped());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetGrouped()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.grouped, other.grouped);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = java.lang.Boolean.compare(isSetGroupId(), other.isSetGroupId());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetGroupId()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.groupId, other.groupId);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    @org.apache.thrift.annotation.Nullable
    @Override
    public _Fields fieldForId(int fieldId)
    {
        return _Fields.findByThriftId(fieldId);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot)
            throws org.apache.thrift.TException
    {
        scheme(iprot).read(iprot, this);
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot)
            throws org.apache.thrift.TException
    {
        scheme(oprot).write(oprot, this);
    }

    @Override
    public java.lang.String toString()
    {
        java.lang.StringBuilder sb = new java.lang.StringBuilder("ThriftLifespan(");
        boolean first = true;

        sb.append("grouped:");
        sb.append(this.grouped);
        first = false;
      if (!first) {
        sb.append(", ");
      }
        sb.append("groupId:");
        sb.append(this.groupId);
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate()
            throws org.apache.thrift.TException
    {
        // check for required fields
        // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws java.io.IOException
    {
        try {
            write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
        }
        catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws java.io.IOException, java.lang.ClassNotFoundException
    {
        try {
            // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
            __isset_bitfield = 0;
            read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
        }
        catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class ThriftLifespanStandardSchemeFactory
            implements org.apache.thrift.scheme.SchemeFactory
    {
        @Override
        public ThriftLifespanStandardScheme getScheme()
        {
            return new ThriftLifespanStandardScheme();
        }
    }

    private static class ThriftLifespanStandardScheme
            extends org.apache.thrift.scheme.StandardScheme<ThriftLifespan>
    {

        @Override
        public void read(org.apache.thrift.protocol.TProtocol iprot, ThriftLifespan struct)
                throws org.apache.thrift.TException
        {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // GROUPED
                        if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
                            struct.grouped = iprot.readBool();
                            struct.setGroupedIsSet(true);
                        }
                        else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 2: // GROUP_ID
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.groupId = iprot.readI32();
                            struct.setGroupIdIsSet(true);
                        }
                        else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    default:
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();

            // check for required fields of primitive type, which can't be checked in the validate method
            struct.validate();
        }

        @Override
        public void write(org.apache.thrift.protocol.TProtocol oprot, ThriftLifespan struct)
                throws org.apache.thrift.TException
        {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            oprot.writeFieldBegin(GROUPED_FIELD_DESC);
            oprot.writeBool(struct.grouped);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(GROUP_ID_FIELD_DESC);
            oprot.writeI32(struct.groupId);
            oprot.writeFieldEnd();
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }
    }

    private static class ThriftLifespanTupleSchemeFactory
            implements org.apache.thrift.scheme.SchemeFactory
    {
        @Override
        public ThriftLifespanTupleScheme getScheme()
        {
            return new ThriftLifespanTupleScheme();
        }
    }

    private static class ThriftLifespanTupleScheme
            extends org.apache.thrift.scheme.TupleScheme<ThriftLifespan>
    {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, ThriftLifespan struct)
                throws org.apache.thrift.TException
        {
            org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
            java.util.BitSet optionals = new java.util.BitSet();
            if (struct.isSetGrouped()) {
                optionals.set(0);
            }
            if (struct.isSetGroupId()) {
                optionals.set(1);
            }
            oprot.writeBitSet(optionals, 2);
            if (struct.isSetGrouped()) {
                oprot.writeBool(struct.grouped);
            }
            if (struct.isSetGroupId()) {
                oprot.writeI32(struct.groupId);
            }
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, ThriftLifespan struct)
                throws org.apache.thrift.TException
        {
            org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
            java.util.BitSet incoming = iprot.readBitSet(2);
            if (incoming.get(0)) {
                struct.grouped = iprot.readBool();
                struct.setGroupedIsSet(true);
            }
            if (incoming.get(1)) {
                struct.groupId = iprot.readI32();
                struct.setGroupIdIsSet(true);
            }
        }
    }

    private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto)
    {
        return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
    }
}

