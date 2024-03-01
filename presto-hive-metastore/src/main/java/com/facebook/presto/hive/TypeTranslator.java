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

import com.facebook.presto.common.type.Type;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.Optional;

public interface TypeTranslator
{
    default TypeInfo translate(Type type)
    {
        return translate(type, Optional.empty());
    }

    /**
     * @param type            source type to be translated
     * @param defaultHiveType When there is no mapping type to translate a type to,
     *                        use provided default hive type when necessary. For example,
     *                        explicit type is not needed for PageFile format.
     */
    TypeInfo translate(Type type, Optional<HiveType> defaultHiveType);
}
