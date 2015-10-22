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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;

import static com.facebook.presto.operator.scalar.HtmlEntitiesSlice.getEscapedSliceForCodepoint;

public final class HtmlFunctions
{
    private HtmlFunctions() {}

    @ScalarFunction
    @Description("Escape/Encode HTML entities from a string. ")
    @SqlType(StandardTypes.VARCHAR)
    public static final Slice htmlEntityEscape(@SqlType(StandardTypes.VARCHAR) Slice html)
    {
        int length = html.length();
        DynamicSliceOutput result = new DynamicSliceOutput(length);
        int position = 0;
        while (position < length) {
            final int codePoint = SliceUtf8.tryGetCodePointAt(html, position);
            if (codePoint < 0) {
                break;
            }
            final int lengthOfCodePoint = SliceUtf8.lengthOfCodePoint(codePoint);
            Slice toAdd = getEscapedSliceForCodepoint(codePoint);
            // if escaped slice is null we replace it with the original
            if (toAdd == null) {
                toAdd = html.slice(position, lengthOfCodePoint);
            }
            result.appendBytes(toAdd);
            position += lengthOfCodePoint;
        }
        return result.slice();
    }
}
