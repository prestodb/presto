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
package com.facebook.plugin.arrow;

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import org.apache.arrow.vector.types.pojo.Field;

public class TestingArrowBlockBuilder
        extends ArrowBlockBuilder
{
    @Override
    protected Type getPrestoTypeFromArrowField(Field field)
    {
        String columnLength = field.getMetadata().get("columnLength");
        int length = columnLength != null ? Integer.parseInt(columnLength) : 0;

        String nativeType = field.getMetadata().get("columnNativeType");

        if ("CHAR".equals(nativeType) || "CHARACTER".equals(nativeType)) {
            return CharType.createCharType(length);
        }
        else if ("VARCHAR".equals(nativeType)) {
            return VarcharType.createVarcharType(length);
        }
        else if ("TIME".equals(nativeType)) {
            return TimeType.TIME;
        }
        else {
            return super.getPrestoTypeFromArrowField(field);
        }
    }
}
