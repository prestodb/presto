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
package com.facebook.presto.thrift.api.udf;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class ThriftUdfRequest
{
    private final String source;
    private final ThriftFunctionHandle functionHandle;
    private final ThriftUdfPage inputs;

    @ThriftConstructor
    public ThriftUdfRequest(String source, ThriftFunctionHandle functionHandle, ThriftUdfPage inputs)
    {
        this.source = requireNonNull(source, "source is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.inputs = requireNonNull(inputs, "inputs is null");
    }

    @ThriftField(1)
    public String getSource()
    {
        return source;
    }

    @ThriftField(2)
    public ThriftFunctionHandle getFunctionHandle()
    {
        return functionHandle;
    }

    @ThriftField(3)
    public ThriftUdfPage getInputs()
    {
        return inputs;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ThriftUdfRequest other = (ThriftUdfRequest) obj;
        return Objects.equals(this.source, other.source) &&
                Objects.equals(this.functionHandle, other.functionHandle) &&
                Objects.equals(this.inputs, other.inputs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, functionHandle, inputs);
    }
}
