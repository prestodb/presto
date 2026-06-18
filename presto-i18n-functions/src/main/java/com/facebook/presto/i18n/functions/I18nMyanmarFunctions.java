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
package com.facebook.presto.i18n.functions;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.myanmartools.TransliterateZ2U;
import com.google.myanmartools.ZawgyiDetector;
import io.airlift.slice.Slice;

import static io.airlift.slice.Slices.utf8Slice;

public final class I18nMyanmarFunctions
{
    /**
     * ZawgyiDetector returns a confidence score that a given string is Zawgyi encoded.
     * This constant defines the default confidence at which a string is treated as Zawgyi.
     */
    private static final double ZAWGYI_PROBABILITY_THRESHOLD = 0.9;

    private static final ZawgyiDetector detector = new ZawgyiDetector();
    private static final TransliterateZ2U z2uTransliterator = new TransliterateZ2U("Zawgyi to Unicode");

    private I18nMyanmarFunctions() {}

    @Description("labels whether input strings use Unicode or Zawgyi font encoding")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice myanmarFontEncoding(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        if (detector.getZawgyiProbability(slice.toStringUtf8()) > ZAWGYI_PROBABILITY_THRESHOLD) {
            return utf8Slice("zawgyi");
        }
        else {
            return utf8Slice("unicode");
        }
    }

    @Description("transforms strings using Myanmar characters to a normalized Unicode form")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice myanmarNormalizeUnicode(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        String[] rawInputPieces = slice.toStringUtf8().split("\n");
        String[] normalizedPieces = new String[rawInputPieces.length];
        for (int i = 0; i < rawInputPieces.length; i++) {
            if (detector.getZawgyiProbability(rawInputPieces[i]) > ZAWGYI_PROBABILITY_THRESHOLD) {
                normalizedPieces[i] = z2uTransliterator.convert(rawInputPieces[i]);
            }
            else {
                normalizedPieces[i] = rawInputPieces[i];
            }
        }
        return utf8Slice(String.join("\n", normalizedPieces));
    }
}
