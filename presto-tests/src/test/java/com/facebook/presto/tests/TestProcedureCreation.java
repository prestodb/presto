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

package com.facebook.presto.tests;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.facebook.presto.testing.TestProcedureRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestProcedureCreation
{
    @Test
    public void shouldThrowExceptionWhenOptionalArgumentIsNotLast()
    {
        assertThatThrownBy(() -> createTestProcedure(ImmutableList.of(
                new Argument("name", VARCHAR, false, null),
                new Argument("name2", VARCHAR, true, null))))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Optional arguments should follow required ones");

        assertThatThrownBy(() -> createTestProcedure(ImmutableList.of(
                new Argument("name", VARCHAR, true, null),
                new Argument("name2", VARCHAR, true, null),
                new Argument("name3", VARCHAR, true, null),
                new Argument("name4", VARCHAR, false, null),
                new Argument("name5", VARCHAR, true, null))))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Optional arguments should follow required ones");
    }

    @Test
    public void shouldThrowExceptionWhenArgumentNameRepeats()
    {
        assertThatThrownBy(() -> createTestProcedure(ImmutableList.of(
                new Argument("name", VARCHAR, false, null),
                new Argument("name", VARCHAR, true, null))))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Duplicate argument name: 'name'");
    }

    @Test
    public void shouldThrowExceptionWhenProcedureIsNonVoid()
    {
        assertThatThrownBy(() -> new Procedure(
                "schema",
                "name",
                ImmutableList.of(),
                methodHandle(Procedures.class, "funWithoutArguments")))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Method must return void");
    }

    @Test
    public void shouldThrowExceptionWhenMethodHandleIsNull()
    {
        assertThatThrownBy(() -> new Procedure(
                "schema",
                "name",
                ImmutableList.of(),
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("methodHandle is null");
    }

    @Test
    public void shouldThrowExceptionWhenMethodHandleHasVarargs()
    {
        assertThatThrownBy(() -> new Procedure(
                "schema",
                "name",
                ImmutableList.of(),
                methodHandle(Procedures.class, "funWithVarargs", String[].class)))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Method must have fixed arity");
    }

    @Test
    public void shouldThrowExceptionWhenArgumentCountDoesntMatch()
    {
        assertThatThrownBy(() -> new Procedure(
                "schema",
                "name",
                ImmutableList.of(
                        new Argument("name", VARCHAR, true, null),
                        new Argument("name2", VARCHAR, true, null),
                        new Argument("name3", VARCHAR, true, null)),
                methodHandle(Procedures.class, "fun1", ConnectorSession.class, Object.class)))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Method parameter count must match arguments");
    }

    @Test
    public void showCreateDistributedProcedure()
    {
        assertThat(new TableDataRewriteDistributedProcedure(
                "schema",
                "name",
                ImmutableList.of(
                        new DistributedProcedure.Argument("name", VARCHAR),
                        new DistributedProcedure.Argument("table_name", VARCHAR),
                        new DistributedProcedure.Argument("schema", VARCHAR, false, null)),
                (session, transactionContext, tableLayoutHandle, arguments) -> null,
                (session, transactionContext, procedureHandle, fragments) -> {},
                ignored -> new TestProcedureRegistry.TestProcedureContext())).isNotNull();
    }

    @Test
    public void shouldThrowExceptionForDistributedProcedureWithWrongArgument()
    {
        assertThatThrownBy(() -> new TableDataRewriteDistributedProcedure(
                "schema",
                "name",
                ImmutableList.of(
                        new DistributedProcedure.Argument("name", VARCHAR),
                        new DistributedProcedure.Argument("table_name", VARCHAR),
                        new DistributedProcedure.Argument("name3", VARCHAR, false, null)),
                (session, transactionContext, tableLayoutHandle, arguments) -> null,
                (session, transactionContext, procedureHandle, fragments) -> {},
                ignored -> new TestProcedureRegistry.TestProcedureContext()))
                .isInstanceOf(PrestoException.class)
                .hasMessage("A distributed procedure need at least 2 arguments: `schema` and `table_name` for the target table");

        assertThatThrownBy(() -> new TableDataRewriteDistributedProcedure(
                "schema",
                "name",
                ImmutableList.of(
                        new DistributedProcedure.Argument("name", VARCHAR),
                        new DistributedProcedure.Argument("name2", VARCHAR),
                        new DistributedProcedure.Argument("schema", VARCHAR, false, null)),
                (session, transactionContext, tableLayoutHandle, arguments) -> null,
                (session, transactionContext, procedureHandle, fragments) -> {},
                ignored -> new TestProcedureRegistry.TestProcedureContext()))
                .isInstanceOf(PrestoException.class)
                .hasMessage("A distributed procedure need at least 2 arguments: `schema` and `table_name` for the target table");

        assertThatThrownBy(() -> new TableDataRewriteDistributedProcedure(
                "schema",
                "name",
                ImmutableList.of(
                        new DistributedProcedure.Argument("name", VARCHAR),
                        new DistributedProcedure.Argument("table_name", VARCHAR),
                        new DistributedProcedure.Argument("schema", INTEGER, false, 123)),
                (session, transactionContext, tableLayoutHandle, arguments) -> null,
                (session, transactionContext, procedureHandle, fragments) -> {},
                ignored -> new TestProcedureRegistry.TestProcedureContext()))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Argument `schema` must be string type");

        assertThatThrownBy(() -> new TableDataRewriteDistributedProcedure(
                "schema",
                "name",
                ImmutableList.of(
                        new DistributedProcedure.Argument("name", VARCHAR),
                        new DistributedProcedure.Argument("table_name", TIMESTAMP),
                        new DistributedProcedure.Argument("schema", VARCHAR, false, null)),
                (session, transactionContext, tableLayoutHandle, arguments) -> null,
                (session, transactionContext, procedureHandle, fragments) -> {},
                ignored -> new TestProcedureRegistry.TestProcedureContext()))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Argument `table_name` must be string type");
    }

    private static Procedure createTestProcedure(List<Argument> arguments)
    {
        int argumentsCount = arguments.size();
        String functionName = "fun" + argumentsCount;

        Class<?>[] clazzes = new Class<?>[argumentsCount + 1];
        clazzes[0] = ConnectorSession.class;

        for (int i = 0; i < argumentsCount; i++) {
            clazzes[i + 1] = Object.class;
        }

        return new Procedure(
                "schema",
                "name",
                arguments,
                methodHandle(Procedures.class, functionName, clazzes));
    }

    public static class Procedures
    {
        public void fun0(ConnectorSession session) {}

        public void fun1(ConnectorSession session, Object arg1) {}

        public void fun2(ConnectorSession session, Object arg1, Object arg2) {}

        public void fun2(Object arg1, Object arg2) {}

        public void fun3(ConnectorSession session, Object arg1, Object arg2, Object arg3) {}

        public void fun4(ConnectorSession session, Object arg1, Object arg2, Object arg3, Object arg4) {}

        public void fun5(ConnectorSession session, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {}

        public String funWithoutArguments()
        {
            return "";
        }

        public void funWithVarargs(String... arguments) {}
    }
}
