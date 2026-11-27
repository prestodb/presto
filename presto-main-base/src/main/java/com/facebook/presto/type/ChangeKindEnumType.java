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
package com.facebook.presto.type;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.common.type.VarcharEnumType;
import com.facebook.presto.common.type.VarcharEnumType.VarcharEnumMap;
import com.google.common.collect.ImmutableMap;

public final class ChangeKindEnumType
        extends VarcharEnumType
{
    public static final ChangeKindEnumType CHANGE_KIND = new ChangeKindEnumType();

    public static final UserDefinedType USER_DEFINED_TYPE = new UserDefinedType(
            QualifiedObjectName.valueOf("presto.default.change_kind"),
            CHANGE_KIND.getTypeSignature());

    private ChangeKindEnumType()
    {
        super(new VarcharEnumMap(
                "presto.default.change_kind",
                ImmutableMap.of(
                        "INSERT", "INSERT",
                        "DELETE", "DELETE",
                        "UPDATE_BEFORE", "UPDATE_BEFORE",
                        "UPDATE_AFTER", "UPDATE_AFTER")));
    }
}
