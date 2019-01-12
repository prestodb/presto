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
package io.prestosql.spi.procedure;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
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
            checkArgument(names.add(argument.getName()), "Duplicate argument name: " + argument.getName());
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

        public Argument(String name, String type)
        {
            this(name, parseTypeSignature(type));
        }

        public Argument(String name, TypeSignature type)
        {
            this.name = checkNotNullOrEmpty(name, "name");
            this.type = requireNonNull(type, "type is null");
        }

        public String getName()
        {
            return name;
        }

        public TypeSignature getType()
        {
            return type;
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
