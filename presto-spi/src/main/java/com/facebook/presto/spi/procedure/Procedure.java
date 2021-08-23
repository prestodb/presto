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
package com.facebook.presto.spi.procedure;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ConnectorSession;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class Procedure
{
    private final String schema;
    private final String name;
    private final List<Argument> arguments;
    private final MethodHandle methodHandle;

    public Procedure(String schema, String name, List<Argument> arguments, MethodHandle methodHandle)
    {
        this.schema = checkNotNullOrEmpty(schema, "schema").toLowerCase(ENGLISH);
        this.name = checkNotNullOrEmpty(name, "name").toLowerCase(ENGLISH);
        this.arguments = unmodifiableList(new ArrayList<>(arguments));
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");

        Set<String> names = new HashSet<>();
        for (Argument argument : arguments) {
            checkArgument(names.add(argument.getName()), format("Duplicate argument name: '%s'", argument.getName()));
        }

        for (int index = 1; index < arguments.size(); index++) {
            if (arguments.get(index - 1).isOptional() && arguments.get(index).isRequired()) {
                throw new IllegalArgumentException("Optional arguments should follow required ones");
            }
        }

        checkArgument(!methodHandle.isVarargsCollector(), "Method must have fixed arity");
        checkArgument(methodHandle.type().returnType() == void.class, "Method must return void");

        long parameterCount = methodHandle.type().parameterList().stream()
                .filter(type -> !ConnectorSession.class.isAssignableFrom(type))
                .count();
        checkArgument(parameterCount == arguments.size(), "Method parameter count must match arguments");
    }

    public String getSchema()
    {
        return schema;
    }

    public String getName()
    {
        return name;
    }

    public List<Argument> getArguments()
    {
        return arguments;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append(schema).append('.').append(name)
                .append('(')
                .append(arguments.stream()
                        .map(Object::toString)
                        .collect(joining(", ")))
                .append(')')
                .toString();
    }

    public static class Argument
    {
        private final String name;
        private final TypeSignature type;
        private final boolean required;
        private final Object defaultValue;

        public Argument(String name, String type)
        {
            this(name, parseTypeSignature(type), true, null);
        }

        public Argument(String name, String type, boolean required, @Nullable Object defaultValue)
        {
            this(name, parseTypeSignature(type), required, defaultValue);
        }

        public Argument(String name, TypeSignature type, boolean required, @Nullable Object defaultValue)
        {
            this.name = checkNotNullOrEmpty(name, "name");
            this.type = requireNonNull(type, "type is null");
            this.required = required;
            this.defaultValue = defaultValue;
        }

        public String getName()
        {
            return name;
        }

        public TypeSignature getType()
        {
            return type;
        }

        public boolean isRequired()
        {
            return required;
        }

        public boolean isOptional()
        {
            return !required;
        }

        /**
         * Argument default value in type's stack representation.
         */
        public Object getDefaultValue()
        {
            return defaultValue;
        }

        @Override
        public String toString()
        {
            return name + " " + type;
        }
    }

    private static String checkNotNullOrEmpty(String value, String name)
    {
        requireNonNull(value, name + " is null");
        checkArgument(!value.isEmpty(), name + " is empty");
        return value;
    }

    private static void checkArgument(boolean assertion, String message)
    {
        if (!assertion) {
            throw new IllegalArgumentException(message);
        }
    }
}
