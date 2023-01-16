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
package com.facebook.presto.json.ir;

import com.facebook.presto.common.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TypedValue
{
    private final Type type;
    private final Object objectValue;
    private final long longValue;
    private final double doubleValue;
    private final boolean booleanValue;
    private final Object valueAsObject;

    public TypedValue(Type type, Object objectValue)
    {
        requireNonNull(type, "type is null");
        requireNonNull(objectValue, "value is null");
        checkArgument(type.getJavaType().isAssignableFrom(objectValue.getClass()), "%s value does not match the type %s", objectValue.getClass(), type);

        this.type = type;
        this.objectValue = objectValue;
        this.longValue = 0L;
        this.doubleValue = 0e0;
        this.booleanValue = false;
        this.valueAsObject = objectValue;
    }

    public TypedValue(Type type, long longValue)
    {
        requireNonNull(type, "type is null");
        checkArgument(long.class.equals(type.getJavaType()), "long value does not match the type %s", type);

        this.type = type;
        this.objectValue = null;
        this.longValue = longValue;
        this.doubleValue = 0e0;
        this.booleanValue = false;
        this.valueAsObject = longValue;
    }

    public TypedValue(Type type, double doubleValue)
    {
        requireNonNull(type, "type is null");
        checkArgument(double.class.equals(type.getJavaType()), "double value does not match the type %s", type);

        this.type = type;
        this.objectValue = null;
        this.longValue = 0L;
        this.doubleValue = doubleValue;
        this.booleanValue = false;
        this.valueAsObject = doubleValue;
    }

    public TypedValue(Type type, boolean booleanValue)
    {
        requireNonNull(type, "type is null");
        checkArgument(boolean.class.equals(type.getJavaType()), "boolean value does not match the type %s", type);

        this.type = type;
        this.objectValue = null;
        this.longValue = 0L;
        this.doubleValue = 0e0;
        this.booleanValue = booleanValue;
        this.valueAsObject = booleanValue;
    }

    public static TypedValue fromValueAsObject(Type type, Object valueAsObject)
    {
        if (long.class.equals(type.getJavaType())) {
            checkState(valueAsObject instanceof Long, "%s value does not match the type %s", valueAsObject.getClass(), type);
            return new TypedValue(type, (long) valueAsObject);
        }
        if (double.class.equals(type.getJavaType())) {
            checkState(valueAsObject instanceof Double, "%s value does not match the type %s", valueAsObject.getClass(), type);
            return new TypedValue(type, (double) valueAsObject);
        }
        if (boolean.class.equals(type.getJavaType())) {
            checkState(valueAsObject instanceof Boolean, "%s value does not match the type %s", valueAsObject.getClass(), type);
            return new TypedValue(type, (boolean) valueAsObject);
        }
        checkState(type.getJavaType().isAssignableFrom(valueAsObject.getClass()), "%s value does not match the type %s", valueAsObject.getClass(), type);
        return new TypedValue(type, valueAsObject);
    }

    public Type getType()
    {
        return type;
    }

    public Object getObjectValue()
    {
        checkArgument(objectValue != null, "the type %s is represented as %s. call another method to retrieve the value", type, type.getJavaType());
        checkArgument(type.getJavaType().isAssignableFrom(objectValue.getClass()), "%s value does not match the type %s", objectValue.getClass(), type);
        return objectValue;
    }

    public long getLongValue()
    {
        checkArgument(long.class.equals(type.getJavaType()), "long value does not match the type %s", type);
        return longValue;
    }

    public double getDoubleValue()
    {
        checkArgument(double.class.equals(type.getJavaType()), "double value does not match the type %s", type);
        return doubleValue;
    }

    public boolean getBooleanValue()
    {
        checkArgument(boolean.class.equals(type.getJavaType()), "boolean value does not match the type %s", type);
        return booleanValue;
    }

    public Object getValueAsObject()
    {
        return valueAsObject;
    }
}
