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
package com.facebook.presto.operator;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.operator.scalar.annotations.SqlInvokedScalarFromAnnotationsParser;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAnnotationEngineForSqlInvokedScalars
        extends TestAnnotationEngine
{
    @Test
    public void testParseFunctionDefinition()
    {
        Signature expectedSignature = new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "sample_sql_invoked_scalar_function"),
                FunctionKind.SCALAR,
                new ArrayType(BIGINT).getTypeSignature(),
                ImmutableList.of(INTEGER.getTypeSignature()));

        List<SqlInvokedFunction> functions = SqlInvokedScalarFromAnnotationsParser.parseFunctionDefinitions(SingleImplementationSQLInvokedScalarFunction.class);
        assertEquals(functions.size(), 1);
        SqlInvokedFunction f = functions.get(0);

        assertEquals(f.getSignature(), expectedSignature);
        assertTrue(f.isDeterministic());
        assertEquals(f.getVisibility(), PUBLIC);
        assertEquals(f.getDescription(), "Simple SQL invoked scalar function");

        assertEquals(f.getBody(), "RETURN SEQUENCE(1, input)");
    }

    @Test
    public void testParseFunctionDefinitionWithTypeParameter()
    {
        Signature expectedSignature = new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "sample_sql_invoked_scalar_function_with_type_parameter"),
                FunctionKind.SCALAR,
                ImmutableList.of(new TypeVariableConstraint("T", false, false, null, false)),
                Collections.emptyList(),
                TypeSignature.parseTypeSignature("array(T)"),
                ImmutableList.of(new TypeSignature("T")),
                false);

        List<SqlInvokedFunction> functions = SqlInvokedScalarFromAnnotationsParser.parseFunctionDefinitions(SingleImplementationSQLInvokedScalarFunctionWithTypeParameter.class);
        assertEquals(functions.size(), 1);
        SqlInvokedFunction f = functions.get(0);

        assertEquals(f.getSignature(), expectedSignature);
        assertTrue(f.isDeterministic());
        assertEquals(f.getVisibility(), PUBLIC);
        assertEquals(f.getDescription(), "Simple SQL invoked scalar function with type parameter");

        assertEquals(f.getBody(), "RETURN ARRAY[input]");
    }

    public static class SingleImplementationSQLInvokedScalarFunction
    {
        @SqlInvokedScalarFunction(value = "sample_sql_invoked_scalar_function", deterministic = true, calledOnNullInput = false)
        @Description("Simple SQL invoked scalar function")
        @SqlParameter(name = "input", type = "integer")
        @SqlType("array<bigint>")
        public static String fun()
        {
            return "RETURN SEQUENCE(1, input)";
        }
    }

    public static class SingleImplementationSQLInvokedScalarFunctionWithTypeParameter
    {
        @SqlInvokedScalarFunction(value = "sample_sql_invoked_scalar_function_with_type_parameter", deterministic = true, calledOnNullInput = false)
        @Description("Simple SQL invoked scalar function with type parameter")
        @TypeParameter("T")
        @SqlParameter(name = "input", type = "T")
        @SqlType("array<T>")
        public static String fun()
        {
            return "RETURN ARRAY[input]";
        }
    }
}
