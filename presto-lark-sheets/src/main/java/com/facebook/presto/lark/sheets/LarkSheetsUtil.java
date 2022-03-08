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

package com.facebook.presto.lark.sheets;

import com.facebook.airlift.json.JsonCodec;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class LarkSheetsUtil
{
    static final int RADIX = 26;
    private static final String[] ALPHABETS = buildAlphabetTable();
    private static final int MASK_REMAIN = 6;

    private LarkSheetsUtil() {}

    public static String loadAppSecret(String secretFile)
    {
        JsonCodec<Map<String, String>> codec = JsonCodec.mapJsonCodec(String.class, String.class);

        final Map<String, String> content;
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(secretFile));
            content = codec.fromBytes(bytes);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Could not read secret file " + secretFile, e);
        }

        String secret = content.get("app-secret");
        if (emptyToNull(secret) == null) {
            throw new IllegalArgumentException("app-secret not provided in " + secretFile);
        }
        return secret;
    }

    public static String mask(String str)
    {
        if (str != null && str.length() > MASK_REMAIN) {
            char[] chars = str.toCharArray();
            Arrays.fill(chars, 0, chars.length - MASK_REMAIN, '*');
            return new String(chars);
        }
        return str;
    }

    public static int columnLabelToColumnIndex(String label)
    {
        requireNonNull(emptyToNull(label), "label is null or empty");

        int length = label.length();

        // Fast path
        if (length == 1) {
            char c = label.charAt(0);
            return charToIndex(c);
        }
        else if (length == 2) {
            char a = label.charAt(0);
            char b = label.charAt(1);
            return charToIndex(a) * RADIX + charToIndex(b) + RADIX;
        }

        // Slow path
        int value = 0;
        int power = 1;
        int offset = -1;
        for (int i = length - 1; i >= 0; i--) {
            value = addExact(value, multiplyExact(charToIndex(label.charAt(i)), power));
            offset = addExact(offset, power);
            power = multiplyExact(power, RADIX);
        }
        return value + offset;
    }

    public static String columnIndexToColumnLabel(int index)
    {
        checkArgument(index >= 0, "index must be non-negative");

        // Fast path
        if (index < RADIX) {
            // A to Z
            return ALPHABETS[index];
        }
        else if (index < RADIX * RADIX + RADIX) {
            // AA to ZZ
            return ALPHABETS[index / RADIX - 1] + ALPHABETS[index % RADIX];
        }

        // Slow path
        int power = RADIX * RADIX;
        int sum = RADIX * RADIX + RADIX;
        int width = 3;
        int position = -1;
        while (index >= sum) {
            power = multiplyExact(power, RADIX);
            int max = addExact(power, sum);
            if (index < max) {
                position = index - sum;
                break;
            }
            sum = max;
            width += 1;
        }

        if (position == -1) {
            throw new IllegalStateException("Unexpected position value");
        }
        String[] chars = new String[width];
        for (int i = width - 1; i >= 0; i--) {
            chars[i] = ALPHABETS[position % RADIX];
            position /= RADIX;
        }
        return String.join("", chars);
    }

    private static String[] buildAlphabetTable()
    {
        String[] table = new String[RADIX];
        for (int i = 0; i < RADIX; i++) {
            table[i] = Character.toString((char) ('A' + i));
        }
        return table;
    }

    private static int charToIndex(char c)
    {
        if (c >= 'A' && c <= 'Z') {
            return c - 'A';
        }
        throw new IllegalArgumentException(format("Illegal char '%c' in label", c));
    }
}
