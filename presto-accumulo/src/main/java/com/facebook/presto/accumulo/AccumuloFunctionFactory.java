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
package com.facebook.presto.accumulo;

import com.facebook.presto.accumulo.udf.AccumuloStringFunctions;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

/**
 * An implementation of a FunctionFactory to provide additional UDF functionality for Presto
 *
 * @see AccumuloStringFunctions
 */
public class AccumuloFunctionFactory
        implements FunctionFactory
{
    private final TypeManager typeManager;

    /**
     * Creates a new instance of {@link AccumuloFunctionFactory}
     *
     * @param typeManager Type manager
     */
    public AccumuloFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    /**
     * Lists all SqlFunctions provided by this connector
     *
     * @return A list of SQL functions
     * @see AccumuloStringFunctions
     */
    @Override
    public List<SqlFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager).scalar(AccumuloStringFunctions.class)
                .getFunctions();
    }
}
