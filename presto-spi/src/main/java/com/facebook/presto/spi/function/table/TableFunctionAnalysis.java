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

import java.util.Optional;

import static com.facebook.presto.spi.function.table.DescriptorMapping.EMPTY_MAPPING;
import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * An object of this class is produced by the `analyze()` method of a `ConnectorTableFunction`
 * implementation. It contains all the analysis results:
 * <p>
 * The `returnedType` field is used to inform the Analyzer of the proper columns returned by the Table
 * Function, that is, the columns produced by the function, as opposed to the columns passed from the
 * input tables. The `returnedType` should only be set if the declared returned type is GENERIC_TABLE.
 * <p>
 * The `descriptorMapping` field is used to inform the Analyzer of the semantics of descriptor arguments.
 * Some descriptor arguments (or some of their fields) might be references to columns of the input tables.
 * In such case, the Analyzer must be informed of those dependencies. It allows to pass the right values
 * (input channels) to the Table Function during execution. It also allows to prune unused input columns
 * during the optimization phase.
 * <p>
 * The `handle` field can be used to carry all information necessary to execute the table function,
 * gathered at analysis time. Typically, these are the values of the constant arguments, and results
 * of pre-processing arguments.
 */
public final class TableFunctionAnalysis
{
    private final Optional<Descriptor> returnedType;
    private final DescriptorMapping descriptorMapping;
    private final ConnectorTableFunctionHandle handle;

    private TableFunctionAnalysis(Optional<Descriptor> returnedType, DescriptorMapping descriptorMapping, ConnectorTableFunctionHandle handle)
    {
        this.returnedType = requireNonNull(returnedType, "returnedType is null");
        returnedType.ifPresent(descriptor -> checkArgument(descriptor.isTyped(), "field types not specified"));
        this.descriptorMapping = requireNonNull(descriptorMapping, "descriptorMapping is null");
        this.handle = requireNonNull(handle, "handle is null");
    }

    public Optional<Descriptor> getReturnedType()
    {
        return returnedType;
    }

    public DescriptorMapping getDescriptorMapping()
    {
        return descriptorMapping;
    }

    public ConnectorTableFunctionHandle getHandle()
    {
        return handle;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Descriptor returnedType;
        private DescriptorMapping descriptorMapping = EMPTY_MAPPING;
        private ConnectorTableFunctionHandle handle = new ConnectorTableFunctionHandle() {};

        private Builder() {}

        public Builder returnedType(Descriptor returnedType)
        {
            this.returnedType = returnedType;
            return this;
        }

        public Builder descriptorMapping(DescriptorMapping descriptorMapping)
        {
            this.descriptorMapping = descriptorMapping;
            return this;
        }

        public Builder handle(ConnectorTableFunctionHandle handle)
        {
            this.handle = handle;
            return this;
        }

        public TableFunctionAnalysis build()
        {
            return new TableFunctionAnalysis(Optional.ofNullable(returnedType), descriptorMapping, handle);
        }
    }
}
