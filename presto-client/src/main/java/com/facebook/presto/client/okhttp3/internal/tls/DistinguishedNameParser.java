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
package com.facebook.presto.client.okhttp3.internal.tls;

import javax.security.auth.x500.X500Principal;

/**
 * A distinguished name (DN) parser. This parser only supports extracting a string value from a DN.
 * It doesn't support values in the hex-string style.
 */
final class DistinguishedNameParser
{
    private final String dn;
    private final int length;
    private int position;
    private int start;
    private int end;
    private int cur;
    private char[] chars;

    /**
     * Instantiates a new Distinguished name parser.
     *
     * @param principal the principal
     */
    DistinguishedNameParser(X500Principal principal)
    {
        this.dn = principal.getName(X500Principal.RFC2253);
        this.length = this.dn.length();
    }

    private String nextAttributeType()
    {
        while (position < length && chars[position] == ' ') {
            position++;
        }
        if (position == length) {
            return null;
        }

        start = position;

        position++;
        while (position < length && chars[position] != '=' && chars[position] != ' ') {
            position++;
        }
        if (position >= length) {
            throw new IllegalStateException("Unexpected end of DN: " + dn);
        }

        end = position;

        if (chars[position] == ' ') {
            while (position < length && chars[position] != '=' && chars[position] == ' ') {
                position++;
            }

            if (chars[position] != '=' || position == length) {
                throw new IllegalStateException("Unexpected end of DN: " + dn);
            }
        }

        position++;

        while (position < length && chars[position] == ' ') {
            position++;
        }

        if ((end - start > 4) && (chars[start + 3] == '.')
                && (chars[start] == 'O' || chars[start] == 'o')
                && (chars[start + 1] == 'I' || chars[start + 1] == 'i')
                && (chars[start + 2] == 'D' || chars[start + 2] == 'd')) {
            start += 4;
        }

        return new String(chars, start, end - start);
    }

    private String quotedAttributeValue()
    {
        position++;
        start = position;
        end = start;
        while (true) {
            if (position == length) {
                throw new IllegalStateException("Unexpected end of DN: " + dn);
            }

            if (chars[position] == '"') {
                position++;
                break;
            }
            else if (chars[position] == '\\') {
                chars[end] = getEscaped();
            }
            else {
                chars[end] = chars[position];
            }
            position++;
            end++;
        }

        while (position < length && chars[position] == ' ') {
            position++;
        }

        return new String(chars, start, end - start);
    }

    private String hexAttributeValue()
    {
        if (position + 4 >= length) {
            throw new IllegalStateException("Unexpected end of DN: " + dn);
        }

        start = position;
        position++;
        while (true) {
            if (position == length || chars[position] == '+' || chars[position] == ','
                    || chars[position] == ';') {
                end = position;
                break;
            }

            if (chars[position] == ' ') {
                end = position;
                position++;
                while (position < length && chars[position] == ' ') {
                    position++;
                }
                break;
            }
            else if (chars[position] >= 'A' && chars[position] <= 'F') {
                chars[position] += 32;
            }

            position++;
        }

        int hexLen = end - start;
        if (hexLen < 5 || (hexLen & 1) == 0) {
            throw new IllegalStateException("Unexpected end of DN: " + dn);
        }

        byte[] encoded = new byte[hexLen / 2];
        for (int i = 0, p = start + 1; i < encoded.length; p += 2, i++) {
            encoded[i] = (byte) getByte(p);
        }
        return new String(chars, start, hexLen);
    }

    private String escapedAttributeValue()
    {
        start = position;
        end = position;
        while (true) {
            if (position >= length) {
                return new String(chars, start, end - start);
            }

            switch (chars[position]) {
                case '+':
                case ',':
                case ';':
                    return new String(chars, start, end - start);
                case '\\':
                    chars[end++] = getEscaped();
                    position++;
                    break;
                case ' ':
                    cur = end;

                    position++;
                    chars[end++] = ' ';

                    while (position < length && chars[position] == ' ') {
                        chars[end++] = ' ';
                        position++;
                    }
                    if (position == length || chars[position] == ',' || chars[position] == '+'
                            || chars[position] == ';') {
                        return new String(chars, start, cur - start);
                    }
                    break;
                default:
                    chars[end++] = chars[position];
                    position++;
            }
        }
    }

    private char getEscaped()
    {
        position++;
        if (position == length) {
            throw new IllegalStateException("Unexpected end of DN: " + dn);
        }

        switch (chars[position]) {
            case '"':
            case '\\':
            case ',':
            case '=':
            case '+':
            case '<':
            case '>':
            case '#':
            case ';':
            case ' ':
            case '*':
            case '%':
            case '_':
                return chars[position];
            default:
                return getUTF8();
        }
    }

    private char getUTF8()
    {
        int res = getByte(position);
        position++;

        if (res < 128) {
            return (char) res;
        }
        else if (res >= 192 && res <= 247) {
            int count;
            if (res <= 223) {
                count = 1;
                res = res & 0x1F;
            }
            else if (res <= 239) {
                count = 2;
                res = res & 0x0F;
            }
            else {
                count = 3;
                res = res & 0x07;
            }
            int b;
            for (int i = 0; i < count; i++) {
                position++;
                if (position == length || chars[position] != '\\') {
                    return 0x3F;
                }
                position++;

                b = getByte(position);
                position++;
                if ((b & 0xC0) != 0x80) {
                    return 0x3F;
                }

                res = (res << 6) + (b & 0x3F);
            }
            return (char) res;
        }
        else {
            return 0x3F;
        }
    }

    private int getByte(int position)
    {
        if (position + 1 >= length) {
            throw new IllegalStateException("Malformed DN: " + dn);
        }

        int b1;
        int b2;

        b1 = chars[position];
        if (b1 >= '0' && b1 <= '9') {
            b1 = b1 - '0';
        }
        else if (b1 >= 'a' && b1 <= 'f') {
            b1 = b1 - 87; // 87 = 'a' - 10
        }
        else if (b1 >= 'A' && b1 <= 'F') {
            b1 = b1 - 55; // 55 = 'A' - 10
        }
        else {
            throw new IllegalStateException("Malformed DN: " + dn);
        }

        b2 = chars[position + 1];
        if (b2 >= '0' && b2 <= '9') {
            b2 = b2 - '0';
        }
        else if (b2 >= 'a' && b2 <= 'f') {
            b2 = b2 - 87; // 87 = 'a' - 10
        }
        else if (b2 >= 'A' && b2 <= 'F') {
            b2 = b2 - 55; // 55 = 'A' - 10
        }
        else {
            throw new IllegalStateException("Malformed DN: " + dn);
        }
        return (b1 << 4) + b2;
    }

    /**
     * Parses the DN and returns the most significant attribute value for an attribute type, or null
     * if none found.
     *
     * @param attributeType attribute type to look for (e.g. "ca")
     * @return the string
     */
    public String findMostSpecific(String attributeType)
    {
        position = 0;
        start = 0;
        end = 0;
        cur = 0;
        chars = dn.toCharArray();

        String attType = nextAttributeType();
        if (attType == null) {
            return null;
        }
        while (true) {
            String attValue = "";

            if (position == length) {
                return null;
            }
            switch (chars[position]) {
                case '"':
                    attValue = quotedAttributeValue();
                    break;
                case '#':
                    attValue = hexAttributeValue();
                    break;
                case '+':
                case ',':
                case ';':
                    break;
                default:
                    attValue = escapedAttributeValue();
            }

            if (attributeType.equalsIgnoreCase(attType)) {
                return attValue;
            }

            if (position >= length) {
                return null;
            }
            if (chars[position] == ',' || chars[position] == ';') {
                //Do nothing
            } else if (chars[position] != '+') {
                throw new IllegalStateException("Malformed DN: " + dn);
            }
            position++;
            attType = nextAttributeType();
            if (attType == null) {
                throw new IllegalStateException("Malformed DN: " + dn);
            }
        }
    }
}
