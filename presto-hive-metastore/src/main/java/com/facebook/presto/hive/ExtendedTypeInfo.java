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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.TypeSignature;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class ExtendedTypeInfo
        extends TypeInfo
{
    private final TypeSignature signature;
    private final TypeInfo typeInfo;

    public ExtendedTypeInfo(TypeSignature signature, TypeInfo typeInfo)
    {
        checkArgument(!(typeInfo instanceof ExtendedTypeInfo), "typeInfo cannot be an ExtendedTypeInfo");
        this.typeInfo = typeInfo;
        this.signature = signature;
    }

    public TypeSignature getSignature()
    {
        return signature;
    }

    public TypeInfo getTypeInfo()
    {
        return typeInfo;
    }

    @Override
    public ObjectInspector.Category getCategory()
    {
        return typeInfo.getCategory();
    }

    @Override
    public String getTypeName()
    {
        return typeInfo.getTypeName();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ExtendedTypeInfo)) {
            return false;
        }
        ExtendedTypeInfo other = (ExtendedTypeInfo) o;
        return Objects.equals(other.getTypeInfo(), typeInfo)
                && Objects.equals(other.getSignature(), signature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeInfo, signature);
    }
}
