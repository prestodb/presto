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
package com.facebook.presto.common.type;

import static java.util.Collections.singletonList;

public final class VarcharType
        extends AbstractVarcharType
{
    public static final VarcharType VARCHAR = new VarcharType(UNBOUNDED_LENGTH);

    public static VarcharType createUnboundedVarcharType()
    {
        return VARCHAR;
    }

    public static VarcharType createVarcharType(int length)
    {
        if (length > MAX_LENGTH || length < 0) {
            // Use createUnboundedVarcharType for unbounded VARCHAR.
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        return new VarcharType(length);
    }

    public static TypeSignature getParametrizedVarcharSignature(String param)
    {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of(param));
    }

    private VarcharType(int length)
    {
        super(
                length,
                new TypeSignature(
                        StandardTypes.VARCHAR,
                        singletonList(TypeSignatureParameter.of((long) length))));
    }
}
