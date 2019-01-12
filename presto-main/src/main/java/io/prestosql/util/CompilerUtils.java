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
package io.prestosql.util;

import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.log.Logger;

import java.lang.invoke.MethodHandle;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.bytecode.BytecodeUtils.toJavaIdentifierString;
import static io.airlift.bytecode.ClassGenerator.classGenerator;
import static io.airlift.bytecode.ParameterizedType.typeFromJavaClassName;
import static java.time.ZoneOffset.UTC;

public final class CompilerUtils
{
    private static final Logger log = Logger.get(CompilerUtils.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss");

    private CompilerUtils() {}

    public static ParameterizedType makeClassName(String baseName, Optional<String> suffix)
    {
        String className = baseName
                + "_" + suffix.orElseGet(() -> Instant.now().atZone(UTC).format(TIMESTAMP_FORMAT))
                + "_" + CLASS_ID.incrementAndGet();
        return typeFromJavaClassName("io.prestosql.$gen." + toJavaIdentifierString(className));
    }

    public static ParameterizedType makeClassName(String baseName)
    {
        return makeClassName(baseName, Optional.empty());
    }

    public static <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, Map<Long, MethodHandle> callSiteBindings, ClassLoader parentClassLoader)
    {
        return defineClass(classDefinition, superType, new DynamicClassLoader(parentClassLoader, callSiteBindings));
    }

    public static <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, DynamicClassLoader classLoader)
    {
        log.debug("Defining class: %s", classDefinition.getName());
        return classGenerator(classLoader).defineClass(classDefinition, superType);
    }
}
