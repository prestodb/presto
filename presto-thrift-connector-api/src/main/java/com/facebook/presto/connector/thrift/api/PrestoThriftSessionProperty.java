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
package com.facebook.presto.connector.thrift.api;

import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBigint;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftBoolean;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftDouble;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftInteger;
import com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftVarchar;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftSessionProperty
{
    private final String name;
    private final String description;
    private final PrestoThriftBlock defaultValue;
    private boolean hidden;
    public static final String PREFIX = "service.";

    @ThriftConstructor
    public PrestoThriftSessionProperty(String name, @Nullable String description, PrestoThriftBlock defaultValue, boolean hidden)
    {
        this.name = requireNonNull(name, "name is null");
        this.description = description;
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
        this.hidden = hidden;
        checkArgument(defaultValue.numberOfRecords() == 1, "defaultValue is not single value");
    }

    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @ThriftField(value = 2, requiredness = OPTIONAL)
    public String getDescription()
    {
        return description;
    }

    @ThriftField(4)
    public PrestoThriftBlock getDefaultValue()
    {
        return defaultValue;
    }

    @ThriftField(5)
    public boolean isHidden()
    {
        return hidden;
    }

    public <T> PropertyMetadata<?> getSessionProperty()
    {
        if (defaultValue.getBooleanData() != null) {
            return booleanSessionProperty(PREFIX + name, description, defaultValue.getBooleanData().getSingleValue(), hidden);
        }
        else if (defaultValue.getIntegerData() != null) {
            return integerSessionProperty(PREFIX + name, description, defaultValue.getIntegerData().getSingleValue(), hidden);
        }
        else if (defaultValue.getBigintData() != null) {
            return longSessionProperty(PREFIX + name, description, defaultValue.getBigintData().getSingleValue(), hidden);
        }
        else if (defaultValue.getDoubleData() != null) {
            return doubleSessionProperty(PREFIX + name, description, defaultValue.getDoubleData().getSingleValue(), hidden);
        }
        else if (defaultValue.getVarcharData() != null) {
            return stringSessionProperty(PREFIX + name, description, defaultValue.getVarcharData().getSingleValue(), hidden);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Session property %s has type not supported by presto thrift connector", name));
    }

    public static <T> PropertyMetadata serviceNamespacedProperty(PropertyMetadata<T> serviceProperty)
    {
        if (serviceProperty.getJavaType() == Boolean.class) {
            return booleanSessionProperty(PREFIX + serviceProperty.getName(), serviceProperty.getDescription(), (Boolean) serviceProperty.getDefaultValue(), serviceProperty.isHidden());
        }
        else if (serviceProperty.getJavaType() == Integer.class) {
            return integerSessionProperty(PREFIX + serviceProperty.getName(), serviceProperty.getDescription(), (Integer) serviceProperty.getDefaultValue(), serviceProperty.isHidden());
        }
        else if (serviceProperty.getJavaType() == Long.class) {
            return longSessionProperty(PREFIX + serviceProperty.getName(), serviceProperty.getDescription(), (Long) serviceProperty.getDefaultValue(), serviceProperty.isHidden());
        }
        else if (serviceProperty.getJavaType() == Double.class) {
            return doubleSessionProperty(PREFIX + serviceProperty.getName(), serviceProperty.getDescription(), (Double) serviceProperty.getDefaultValue(), serviceProperty.isHidden());
        }
        else if (serviceProperty.getJavaType() == String.class) {
            return stringSessionProperty(PREFIX + serviceProperty.getName(), serviceProperty.getDescription(), (String) serviceProperty.getDefaultValue(), serviceProperty.isHidden());
        }

        throw new PrestoException(NOT_SUPPORTED, format("Session property %s has type not supported by presto thrift connector", serviceProperty.getName()));
    }

    public static PrestoThriftSessionProperty fromProperty(PropertyMetadata property)
    {
        Class<?> javaType = property.getJavaType();
        if (javaType == Boolean.class) {
            return fromBooleanProperty(property);
        }
        else if (javaType == Integer.class) {
            return fromIntegerProperty(property);
        }
        else if (javaType == Long.class) {
            return fromLongProperty(property);
        }
        else if (javaType == Double.class) {
            return fromDoubleProperty(property);
        }
        else if (javaType == String.class) {
            return fromStringProperty(property);
        }
        throw new PrestoException(NOT_SUPPORTED, format("Session property %s has type not supported by presto thrift connector", property.getName()));
    }

    public static PrestoThriftSessionProperty fromBooleanProperty(PropertyMetadata<Boolean> property)
    {
        Boolean value = property.getDefaultValue();
        PrestoThriftBlock valueBlock;
        if (value != null) {
            valueBlock = PrestoThriftBlock.booleanData(new PrestoThriftBoolean(null, new boolean[] {value}));
        }
        else {
            valueBlock = PrestoThriftBlock.booleanData(new PrestoThriftBoolean(new boolean[] {true}, null));
        }

        return new PrestoThriftSessionProperty(property.getName(),
                property.getDescription(),
                valueBlock,
                property.isHidden());
    }

    public static PrestoThriftSessionProperty fromIntegerProperty(PropertyMetadata<Integer> property)
    {
        Integer value = property.getDefaultValue();
        PrestoThriftBlock valueBlock;
        if (value != null) {
            valueBlock = PrestoThriftBlock.integerData(new PrestoThriftInteger(null, new int[] {value}));
        }
        else {
            valueBlock = PrestoThriftBlock.integerData(new PrestoThriftInteger(new boolean[] {true}, null));
        }

        return new PrestoThriftSessionProperty(property.getName(),
                property.getDescription(),
                valueBlock,
                property.isHidden());
    }

    public static PrestoThriftSessionProperty fromLongProperty(PropertyMetadata<Long> property)
    {
        Long value = property.getDefaultValue();
        PrestoThriftBlock valueBlock;
        if (value != null) {
            valueBlock = PrestoThriftBlock.bigintData(new PrestoThriftBigint(null, new long[] {value}));
        }
        else {
            valueBlock = PrestoThriftBlock.bigintData(new PrestoThriftBigint(new boolean[] {true}, null));
        }

        return new PrestoThriftSessionProperty(property.getName(),
                property.getDescription(),
                valueBlock,
                property.isHidden());
    }

    public static PrestoThriftSessionProperty fromDoubleProperty(PropertyMetadata<Double> property)
    {
        Double value = property.getDefaultValue();
        PrestoThriftBlock valueBlock;
        if (value != null) {
            valueBlock = PrestoThriftBlock.doubleData(new PrestoThriftDouble(null, new double[] {value}));
        }
        else {
            valueBlock = PrestoThriftBlock.doubleData(new PrestoThriftDouble(new boolean[] {true}, null));
        }

        return new PrestoThriftSessionProperty(property.getName(),
                property.getDescription(),
                valueBlock,
                property.isHidden());
    }

    public static PrestoThriftSessionProperty fromStringProperty(PropertyMetadata<String> property)
    {
        String value = property.getDefaultValue();
        PrestoThriftBlock valueBlock;
        if (value != null) {
            valueBlock = PrestoThriftBlock.varcharData(new PrestoThriftVarchar(null, new int[] {value.length()}, value.getBytes(UTF_8)));
        }
        else {
            valueBlock = PrestoThriftBlock.varcharData(new PrestoThriftVarchar(new boolean[] {true}, null, null));
        }

        return new PrestoThriftSessionProperty(property.getName(),
                property.getDescription(),
                valueBlock,
                property.isHidden());
    }
}
