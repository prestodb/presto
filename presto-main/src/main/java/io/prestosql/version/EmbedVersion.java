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
package io.prestosql.version;

import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.prestosql.server.ServerConfig;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.Objects;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Reflection.constructorMethodHandle;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class EmbedVersion
{
    private final MethodHandle runnableConstructor;

    @Inject
    public EmbedVersion(ServerConfig serverConfig)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(baseClassName(serverConfig)),
                type(Object.class),
                type(Runnable.class));

        FieldDefinition field = classDefinition.declareField(a(PRIVATE), "runnable", Runnable.class);

        Parameter parameter = arg("runnable", type(Runnable.class));
        MethodDefinition constructor = classDefinition.declareConstructor(a(PUBLIC), parameter);
        constructor.getBody()
                .comment("super(runnable);")
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .append(constructor.getThis())
                .append(parameter)
                .putField(field)
                .ret();

        MethodDefinition run = classDefinition.declareMethod(a(PUBLIC), "run", type(void.class));
        run.getBody()
                .comment("runnable.run();")
                .append(run.getThis())
                .getField(field)
                .invokeInterface(Runnable.class, "run", void.class)
                .ret();

        Class<? extends Runnable> generatedClass = defineClass(classDefinition, Runnable.class, ImmutableMap.of(), getClass().getClassLoader());
        this.runnableConstructor = constructorMethodHandle(generatedClass, Runnable.class);
    }

    private static String baseClassName(ServerConfig serverConfig)
    {
        String builtInVersion = new ServerConfig().getPrestoVersion();
        String configuredVersion = serverConfig.getPrestoVersion();

        String version = configuredVersion;
        if (!Objects.equals(builtInVersion, configuredVersion)) {
            version = format("%s__%s", builtInVersion, configuredVersion);
        }
        return format("Presto_%s___", version);
    }

    public Runnable embedVersion(Runnable runnable)
    {
        requireNonNull(runnable, "runnable is null");
        try {
            return (Runnable) runnableConstructor.invoke(runnable);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }
}
