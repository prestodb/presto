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
import java.util.Optional;
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
    public Analysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        throw new UnsupportedOperationException("analyze method not implemented for Table Function " + name);
    }

    /**
     * The `analyze()` method should produce an object of this class, containing all the analysis results:
     * <p>
     * The `returnedType` field is used to inform the Analyzer of the proper columns returned by the Table
     * Function, that is, the columns produced by the function, as opposed to the columns passed from the
     * input tables. The `returnedType` should only be set if the declared returned type is GENERIC_TABLE.
     * <p>
     * The `descriptorsToTables` field is used to inform the Analyzer of the semantics of descriptor arguments.
     * Some descriptor arguments (or some of their fields) might be references to columns of the input tables.
     * In such case, the Analyzer must be informed of those dependencies. It allows to pass the right values
     * (input channels) to the Table Function during execution. It also allows to prune unused input columns
     * during the optimization phase.
     * <p>
     * The `handle` field can be used to carry all information necessary to execute the table function,
     * gathered at analysis time. Typically, these are the values of the constant arguments, and results
     * of pre-processing arguments.
     */
    public static class Analysis
    {
        private final Optional<Descriptor> returnedType;
        private final DescriptorMapping descriptorsToTables;
        private final ConnectorTableFunctionHandle handle;

        public Analysis(Optional<Descriptor> returnedType, DescriptorMapping descriptorsToTables, ConnectorTableFunctionHandle handle)
        {
            this.returnedType = requireNonNull(returnedType, "returnedType is null");
            returnedType.ifPresent(descriptor -> checkArgument(descriptor.isTyped(), "field types not specified"));
            this.descriptorsToTables = requireNonNull(descriptorsToTables, "descriptorsToTables is null");
            this.handle = requireNonNull(handle, "handle is null");
        }

        public Optional<Descriptor> getReturnedType()
        {
            return returnedType;
        }

        public DescriptorMapping getDescriptorsToTables()
        {
            return descriptorsToTables;
        }

        public ConnectorTableFunctionHandle getHandle()
        {
            return handle;
        }
    }

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
