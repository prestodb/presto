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
package io.prestosql.tests;

import com.google.common.collect.ImmutableList;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.procedure.Procedure.Argument;
import io.prestosql.testing.ProcedureTester;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class TestingProcedures
{
    private final ProcedureTester tester;

    public TestingProcedures(ProcedureTester tester)
    {
        this.tester = requireNonNull(tester, "tester is null");
    }

    @UsedByGeneratedCode
    public void simple()
    {
        tester.recordCalled("simple");
    }

    @UsedByGeneratedCode
    public void args(long x, double y, String z, boolean q)
    {
        tester.recordCalled("args", x, y, z, q);
    }

    @UsedByGeneratedCode
    public void nulls(Long x, String y)
    {
        tester.recordCalled("nulls", x, y);
    }

    @UsedByGeneratedCode
    public void arrays(List<Long> x, List<String> y)
    {
        tester.recordCalled("arrays", x, y);
    }

    @UsedByGeneratedCode
    public void nested(List<List<Long>> x)
    {
        tester.recordCalled("nested", x);
    }

    @UsedByGeneratedCode
    public void sessionFirst(ConnectorSession session, long x)
    {
        requireNonNull(session, "session is null");
        tester.recordCalled("session_first", x);
    }

    @UsedByGeneratedCode
    public void sessionLast(String x, ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        tester.recordCalled("session_last", x);
    }

    @UsedByGeneratedCode
    public void exception()
    {
        tester.recordCalled("exception");
        throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "test exception from procedure");
    }

    @UsedByGeneratedCode
    public void error()
    {
        tester.recordCalled("error");
        throw new RuntimeException("test error from procedure");
    }

    public List<Procedure> getProcedures(String schema)
    {
        return ImmutableList.<Procedure>builder()
                .add(procedure(schema, "test_simple", "simple", ImmutableList.of()))
                .add(procedure(schema, "test_args", "args", ImmutableList.of(
                        new Argument("x", BIGINT),
                        new Argument("y", DOUBLE),
                        new Argument("z", VARCHAR),
                        new Argument("q", BOOLEAN))))
                .add(procedure(schema, "test_nulls", "nulls", ImmutableList.of(
                        new Argument("x", BIGINT),
                        new Argument("y", VARCHAR))))
                .add(procedure(schema, "test_arrays", "arrays", ImmutableList.of(
                        new Argument("x", "array(bigint)"),
                        new Argument("y", "array(varchar)"))))
                .add(procedure(schema, "test_nested", "nested", ImmutableList.of(
                        new Argument("x", "array(array(bigint))"))))
                .add(procedure(schema, "test_session_first", "sessionFirst", ImmutableList.of(
                        new Argument("x", BIGINT))))
                .add(procedure(schema, "test_session_last", "sessionLast", ImmutableList.of(
                        new Argument("x", VARCHAR))))
                .add(procedure(schema, "test_exception", "exception", ImmutableList.of()))
                .add(procedure(schema, "test_error", "error", ImmutableList.of()))
                .build();
    }

    private Procedure procedure(String schema, String name, String methodName, List<Argument> arguments)
    {
        return new Procedure(schema, name, arguments, handle(methodName));
    }

    private MethodHandle handle(String name)
    {
        List<Method> methods = Arrays.stream(getClass().getMethods())
                .filter(method -> method.getName().equals(name))
                .collect(toList());
        checkArgument(!methods.isEmpty(), "no matching methods: %s", name);
        checkArgument(methods.size() == 1, "multiple matching methods: %s", methods);
        return methodHandle(methods.get(0)).bindTo(this);
    }
}
