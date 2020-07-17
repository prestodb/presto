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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct("FunctionHandle")
public class ThriftFunctionHandle
{
    private final String functionName;
    private final List<String> argumentTypes;
    private final String returnType;
    private final String version;

    @ThriftConstructor
    public ThriftFunctionHandle(String functionName, List<String> argumentTypes, String returnType, String version)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.argumentTypes = requireNonNull(argumentTypes, "argumentTypes is null");
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.version = requireNonNull(version, "version is null");
    }

    @ThriftField(1)
    public String getFunctionName()
    {
        return functionName;
    }

    @ThriftField(2)
    public List<String> getArgumentTypes()
    {
        return argumentTypes;
    }

    @ThriftField(3)
    public String getReturnType()
    {
        return returnType;
    }

    @ThriftField(4)
    public String getVersion()
    {
        return version;
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
        ThriftFunctionHandle other = (ThriftFunctionHandle) obj;
        return Objects.equals(this.functionName, other.functionName) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.version, other.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, argumentTypes, returnType, version);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("functionName", functionName)
                .add("argumentTypes", argumentTypes)
                .add("returnType", returnType)
                .add("version", version)
                .toString();
    }
}
