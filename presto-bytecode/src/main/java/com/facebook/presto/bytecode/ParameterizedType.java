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
package com.facebook.presto.bytecode;

import com.google.common.collect.ImmutableList;
import org.objectweb.asm.Type;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ParameterizedType
{
    public static ParameterizedType typeFromJavaClassName(String className)
    {
        requireNonNull(className, "type is null");
        return new ParameterizedType(className.replace('.', '/'));
    }

    public static ParameterizedType typeFromPathName(String className)
    {
        requireNonNull(className, "type is null");
        return new ParameterizedType(className);
    }

    public static ParameterizedType type(Type type)
    {
        requireNonNull(type, "type is null");
        return new ParameterizedType(type.getInternalName());
    }

    public static ParameterizedType type(Class<?> type)
    {
        requireNonNull(type, "type is null");
        return new ParameterizedType(type);
    }

    public static ParameterizedType type(Class<?> type, Class<?>... parameters)
    {
        requireNonNull(type, "type is null");
        return new ParameterizedType(type, parameters);
    }

    public static ParameterizedType type(Class<?> type, ParameterizedType... parameters)
    {
        requireNonNull(type, "type is null");
        return new ParameterizedType(type, parameters);
    }

    private final String type;
    private final String className;
    private final String simpleName;
    private final List<String> parameters;

    private final boolean isInterface;
    @Nullable
    private final Class<?> primitiveType;
    @Nullable
    private final ParameterizedType arrayComponentType;

    public ParameterizedType(String className)
    {
        requireNonNull(className, "className is null");
        checkArgument(!className.contains("."), "Invalid class name %s", className);
        checkArgument(!className.endsWith(";"), "Invalid class name %s", className);

        this.className = className;
        this.simpleName = className.substring(className.lastIndexOf("/") + 1);
        this.type = "L" + className + ";";
        this.parameters = ImmutableList.of();

        this.isInterface = false;
        this.primitiveType = null;
        this.arrayComponentType = null;
    }

    private ParameterizedType(Class<?> type)
    {
        requireNonNull(type, "type is null");
        this.type = toInternalIdentifier(type);
        this.className = getPathName(type);
        this.simpleName = type.getSimpleName();
        this.parameters = ImmutableList.of();

        this.isInterface = type.isInterface();
        this.primitiveType = type.isPrimitive() ? type : null;
        this.arrayComponentType = type.isArray() ? type(type.getComponentType()) : null;
    }

    private ParameterizedType(Class<?> type, Class<?>... parameters)
    {
        requireNonNull(type, "type is null");
        this.type = toInternalIdentifier(type);
        this.className = getPathName(type);
        this.simpleName = type.getSimpleName();

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Class<?> parameter : parameters) {
            builder.add(toInternalIdentifier(parameter));
        }
        this.parameters = builder.build();

        this.isInterface = type.isInterface();
        this.primitiveType = type.isPrimitive() ? type : null;
        this.arrayComponentType = type.isArray() ? type(type.getComponentType()) : null;
    }

    private ParameterizedType(Class<?> type, ParameterizedType... parameters)
    {
        requireNonNull(type, "type is null");
        this.type = toInternalIdentifier(type);
        this.className = getPathName(type);
        this.simpleName = type.getSimpleName();

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (ParameterizedType parameter : parameters) {
            builder.add(parameter.toString());
        }
        this.parameters = builder.build();

        this.isInterface = type.isInterface();
        this.primitiveType = type.isPrimitive() ? type : null;
        this.arrayComponentType = type.isArray() ? type(type.getComponentType()) : null;
    }

    public String getClassName()
    {
        return className;
    }

    public String getJavaClassName()
    {
        return className.replace('/', '.');
    }

    public String getSimpleName()
    {
        return simpleName;
    }

    public String getType()
    {
        return type;
    }

    public Type getAsmType()
    {
        return Type.getObjectType(className);
    }

    public String getGenericSignature()
    {
        StringBuilder sb = new StringBuilder();
        if (primitiveType != null || arrayComponentType != null) {
            return type;
        }
        sb.append('L').append(className);
        if (!parameters.isEmpty()) {
            sb.append("<");
            for (String parameterType : parameters) {
                sb.append(parameterType);
            }
            sb.append(">");
        }
        sb.append(";");
        return sb.toString();
    }

    public boolean isGeneric()
    {
        return !parameters.isEmpty();
    }

    public boolean isInterface()
    {
        return isInterface;
    }

    @Nullable
    public Class<?> getPrimitiveType()
    {
        return primitiveType;
    }

    public boolean isPrimitive()
    {
        return primitiveType != null;
    }

    @Nullable
    public ParameterizedType getArrayComponentType()
    {
        return arrayComponentType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ParameterizedType that = (ParameterizedType) o;

        if (!type.equals(that.type)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode();
    }

    @Override
    public String toString()
    {
        return getGenericSignature();
    }

    public static String getPathName(Class<?> n)
    {
        return n.getName().replace('.', '/');
    }

    private static String toInternalIdentifier(Class<?> n)
    {
        if (n.isArray()) {
            n = n.getComponentType();
            if (n.isPrimitive()) {
                if (n == Byte.TYPE) {
                    return "[B";
                }
                else if (n == Boolean.TYPE) {
                    return "[Z";
                }
                else if (n == Short.TYPE) {
                    return "[S";
                }
                else if (n == Character.TYPE) {
                    return "[C";
                }
                else if (n == Integer.TYPE) {
                    return "[I";
                }
                else if (n == Float.TYPE) {
                    return "[F";
                }
                else if (n == Double.TYPE) {
                    return "[D";
                }
                else if (n == Long.TYPE) {
                    return "[J";
                }
                else {
                    throw new RuntimeException("Unrecognized type in compiler: " + n.getName());
                }
            }
            else {
                return "[" + toInternalIdentifier(n);
            }
        }
        else {
            if (n.isPrimitive()) {
                if (n == Byte.TYPE) {
                    return "B";
                }
                else if (n == Boolean.TYPE) {
                    return "Z";
                }
                else if (n == Short.TYPE) {
                    return "S";
                }
                else if (n == Character.TYPE) {
                    return "C";
                }
                else if (n == Integer.TYPE) {
                    return "I";
                }
                else if (n == Float.TYPE) {
                    return "F";
                }
                else if (n == Double.TYPE) {
                    return "D";
                }
                else if (n == Long.TYPE) {
                    return "J";
                }
                else if (n == Void.TYPE) {
                    return "V";
                }
                else {
                    throw new RuntimeException("Unrecognized type in compiler: " + n.getName());
                }
            }
            else {
                return "L" + getPathName(n) + ";";
            }
        }
    }
}
