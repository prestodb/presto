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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.DistinctType;
import com.facebook.presto.common.type.DistinctTypeInfo;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeWithName;
import com.facebook.presto.common.type.UserDefinedType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class QueryAwareFunctionAndTypeManager
        extends FunctionAndTypeManager
{
    // Cache user defined types queried for a particular query. So a query sees a consistent view of them.
    private final ConcurrentHashMap<QualifiedObjectName, UserDefinedType> userDefinedTypes = new ConcurrentHashMap<>();

    public QueryAwareFunctionAndTypeManager(FunctionAndTypeManager functionAndTypeManager)
    {
        super(functionAndTypeManager);
    }

    private void cacheType(Type type)
    {
        if (type instanceof DistinctType) {
            DistinctType distinctType = (DistinctType) type;
            while (distinctType != null) {
                userDefinedTypes.putIfAbsent(
                        distinctType.getName(),
                        new UserDefinedType(
                                distinctType.getName(),
                                new TypeSignature(
                                        new DistinctTypeInfo(
                                                distinctType.getName(),
                                                distinctType.getBaseType().getTypeSignature(),
                                                distinctType.getParentTypeName(),
                                                distinctType.isOrderable()))));
                distinctType = distinctType.getParentTypeIfAvailable().orElse(null);
            }
        }
        else if (type instanceof TypeWithName) {
            TypeWithName typeWithName = (TypeWithName) type;
            userDefinedTypes.putIfAbsent(typeWithName.getName(), new UserDefinedType(typeWithName.getName(), typeWithName.getType().getTypeSignature()));
        }
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        if (signature.getTypeSignatureBase().hasStandardType()) {
            Type returnType = super.getType(signature);
            cacheType(returnType);
            return returnType;
        }

        UserDefinedType cachedUserDefinedType = userDefinedTypes.get(signature.getTypeSignatureBase().getTypeName());

        if (cachedUserDefinedType != null) {
            return super.getType(cachedUserDefinedType);
        }

        Type returnType = super.getType(signature);
        cacheType(returnType);
        return returnType;
    }
}
