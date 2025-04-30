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
package com.facebook.presto.spi.function.table;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public abstract class ConnectorTableFunction
{
    private final String schema;
    private final String name;
    private final List<ArgumentSpecification> arguments;
    private final ReturnTypeSpecification returnTypeSpecification;

    public ConnectorTableFunction(String schema, String name, List<ArgumentSpecification> arguments, ReturnTypeSpecification returnTypeSpecification)
    {
        this.schema = checkNotNullOrEmpty(schema, "schema");
        this.name = checkNotNullOrEmpty(name, "name");
        requireNonNull(arguments, "arguments is null");
        Set<String> argumentNames = new HashSet<>();
        for (ArgumentSpecification specification : arguments) {
            if (!argumentNames.add(specification.getName())) {
                throw new IllegalArgumentException("duplicate argument name: " + specification.getName());
            }
        }
        long tableArgumentsWithRowSemantics = arguments.stream()
                .filter(specification -> specification instanceof TableArgumentSpecification)
                .map(TableArgumentSpecification.class::cast)
                .filter(TableArgumentSpecification::isRowSemantics)
                .count();
        checkArgument(tableArgumentsWithRowSemantics <= 1, "more than one table argument with row semantics");
        this.arguments = unmodifiableList(arguments);
        this.returnTypeSpecification = requireNonNull(returnTypeSpecification, "returnTypeSpecification is null");
    }

    public String getSchema()
    {
        return schema;
    }

    public String getName()
    {
        return name;
    }

    public List<ArgumentSpecification> getArguments()
    {
        return arguments;
    }

    public ReturnTypeSpecification getReturnTypeSpecification()
    {
        return returnTypeSpecification;
    }

    /**
     * This method is called by the Analyzer. Its main purposes are to:
     * 1. Determine the resulting relation type of the Table Function in case when the declared return type is GENERIC_TABLE.
     * 2. Declare the dependencies between the input descriptors and the input tables.
     * 3. Perform function-specific validation and pre-processing of the input arguments.
     * As part of function-specific validation, the Table Function's author might want to:
     * - check if the descriptors which reference input tables contain a correct number of column references
     * - check if the referenced input columns have appropriate types to fit the function's logic // TODO return request for coercions to the Analyzer in the Analysis object
     * - if there is a descriptor which describes the function's output, check if it matches the shape of the actual function's output
     * - for table arguments, check the number and types of ordering columns
     * <p>
     * The actual argument values, and the pre-processing results can be stored in an ConnectorTableFunctionHandle
     * object, which will be passed along with the Table Function invocation through subsequent phases of planning.
     *
     * @param arguments actual invocation arguments, mapped by argument names
     */
    public abstract TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments);

    static String checkNotNullOrEmpty(String value, String name)
    {
        requireNonNull(value, name + " is null");
        checkArgument(!value.isEmpty(), name + " is empty");
        return value;
    }

    static void checkArgument(boolean assertion, String message)
    {
        if (!assertion) {
            throw new IllegalArgumentException(message);
        }
    }
}
