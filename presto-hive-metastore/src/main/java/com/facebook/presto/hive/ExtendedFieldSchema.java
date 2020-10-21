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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/*
 * A regular Hive FieldSchema object only stores the column type as a string, which must
 * be a valid built-in Hive type signature, that can be parsed by Hive's TypeInfoUtils.getTypeInfoFromTypeString.
 * This is why use this class to pass non-builtin Hive types to Presto (eg enums, distinct types).
 */
public class ExtendedFieldSchema
        extends FieldSchema
{
    private final TypeInfo typeInfo;

    public ExtendedFieldSchema(String name, String type, String comment, TypeInfo typeInfo)
    {
        super(name, type, comment);
        this.typeInfo = typeInfo;
    }

    public TypeInfo getTypeInfo()
    {
        return typeInfo;
    }

    @Override
    public FieldSchema deepCopy()
    {
        return new ExtendedFieldSchema(getName(), getType(), getComment(), getTypeInfo());
    }
}
