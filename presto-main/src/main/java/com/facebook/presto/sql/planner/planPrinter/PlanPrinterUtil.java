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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class PlanPrinterUtil
{
    private PlanPrinterUtil()
    {
    }

    static String castToVarchar(Type type, Object value, FunctionRegistry functionRegistry, Session session)
    {
        if (value == null) {
            return "NULL";
        }

        try {
            Signature coercion = functionRegistry.getCoercion(type, VARCHAR);
            Slice coerced = (Slice) new InterpretedFunctionInvoker(functionRegistry).invoke(coercion, session.toConnectorSession(), value);
            return coerced.toStringUtf8();
        }
        catch (OperatorNotFoundException e) {
            return "<UNREPRESENTABLE VALUE>";
        }
    }
}
