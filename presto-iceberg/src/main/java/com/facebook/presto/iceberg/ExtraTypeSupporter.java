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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

import java.util.Optional;
import java.util.Set;

import static org.apache.iceberg.types.Type.TypeID.STRING;

public class ExtraTypeSupporter
{
    private static Set<ExtraTypeSupportRule> extraTypeSupportRules = ImmutableSet.of(
            new CharTypeSupportRule());

    private ExtraTypeSupporter()
    {}

    static Optional<Type> getPrestoTypeWithExtraInfo(org.apache.iceberg.types.Type type, String typeExtraInfo)
    {
        for (ExtraTypeSupportRule extraTypeSupportRule : extraTypeSupportRules) {
            Optional<Type> prestoTypeWithExtraInfo = extraTypeSupportRule.getPrestoTypeWithExtraInfo(type, typeExtraInfo);
            if (prestoTypeWithExtraInfo.isPresent()) {
                return prestoTypeWithExtraInfo;
            }
        }
        return Optional.empty();
    }

    static Optional<Pair<org.apache.iceberg.types.Type, String>> getIcebergTypeWithExtraInfo(Type type)
    {
        for (ExtraTypeSupportRule extraTypeSupportRule : extraTypeSupportRules) {
            Optional<Pair<org.apache.iceberg.types.Type, String>> icebergTypeWithExtraInfo = extraTypeSupportRule.getIcebergTypeWithExtraInfo(type);
            if (icebergTypeWithExtraInfo.isPresent()) {
                return icebergTypeWithExtraInfo;
            }
        }
        return Optional.empty();
    }

    interface ExtraTypeSupportRule
    {
        Optional<Type> getPrestoTypeWithExtraInfo(org.apache.iceberg.types.Type type, String typeExtraInfo);

        Optional<Pair<org.apache.iceberg.types.Type, String>> getIcebergTypeWithExtraInfo(Type type);
    }

    static class CharTypeSupportRule
            implements ExtraTypeSupportRule
    {
        @Override
        public Optional<Type> getPrestoTypeWithExtraInfo(org.apache.iceberg.types.Type type, String typeExtraInfo)
        {
            if (type.typeId() == STRING) {
                try {
                    int length = Integer.valueOf(typeExtraInfo);
                    if (length > 0) {
                        return Optional.of(CharType.createCharType(length));
                    }
                }
                catch (NumberFormatException e) {
                    return Optional.empty();
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<Pair<org.apache.iceberg.types.Type, String>> getIcebergTypeWithExtraInfo(Type type)
        {
            if (type instanceof CharType) {
                int length = ((CharType) type).getLength();
                return Optional.of(Pair.of(Types.StringType.get(), String.valueOf(length)));
            }
            return Optional.empty();
        }
    }
}
