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

package com.facebook.presto.util;

import com.facebook.presto.common.type.RowType;

import java.util.List;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LegacyRowFieldOrdinalAccessUtil
{
    private static final Pattern ORDINAL_ROW_FIELD_NAME = Pattern.compile("(?i)field([0-9]+)");

    private LegacyRowFieldOrdinalAccessUtil() {}

    public static OptionalInt parseAnonymousRowFieldOrdinalAccess(String fieldName, List<RowType.Field> rowFields)
    {
        Matcher matcher = ORDINAL_ROW_FIELD_NAME.matcher(fieldName);
        if (!matcher.matches()) {
            return OptionalInt.empty();
        }
        int rowIndex;
        try {
            rowIndex = Integer.parseInt(matcher.group(1));
        }
        catch (NumberFormatException e) {
            return OptionalInt.empty();
        }
        if (rowIndex >= rowFields.size()) {
            return OptionalInt.empty();
        }
        if (rowFields.get(rowIndex).getName().isPresent()) {
            // not an anonymous field
            return OptionalInt.empty();
        }
        return OptionalInt.of(rowIndex);
    }
}
