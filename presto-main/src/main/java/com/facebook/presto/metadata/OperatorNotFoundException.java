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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class OperatorNotFoundException extends PrestoException
{
    private final OperatorType operatorType;
    private final Type returnType;
    private final List<Type> argumentTypes;

    public OperatorNotFoundException(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        super(StandardErrorCode.OPERATOR_NOT_FOUND.toErrorCode(), format("Operator %s(%s) not registered", operatorType, Joiner.on(", ").join(argumentTypes)));
        this.operatorType = checkNotNull(operatorType, "operatorType is null");
        this.returnType = null;
        this.argumentTypes = ImmutableList.copyOf(checkNotNull(argumentTypes, "argumentTypes is null"));
    }

    public OperatorNotFoundException(OperatorType operatorType, List<? extends Type> argumentTypes, Type returnType)
    {
        super(StandardErrorCode.OPERATOR_NOT_FOUND.toErrorCode(), format("Operator %s(%s):%s not registered", operatorType, Joiner.on(", ").join(argumentTypes), returnType));
        this.operatorType = checkNotNull(operatorType, "operatorType is null");
        this.argumentTypes = ImmutableList.copyOf(checkNotNull(argumentTypes, "argumentTypes is null"));
        this.returnType = checkNotNull(returnType, "returnType is null");
    }

    public OperatorType getOperatorType()
    {
        return operatorType;
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }
}
