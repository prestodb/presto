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

public class SanitizeMessage
{
    private SanitizeMessage() {}

    public static String getSanitizeMessage(String message)
    {
        if (message == null) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder(message.length());

        // Traverse each character in the string and replace only when necessary
        for (int i = 0; i < message.length(); i++) {
            char ch = message.charAt(i);
            switch (ch) {
                case '&':
                    stringBuilder.append("&amp;");
                    break;
                case '<':
                    stringBuilder.append("&lt;");
                    break;
                case '>':
                    stringBuilder.append("&gt;");
                    break;
                case '"':
                    stringBuilder.append("&quot;");
                    break;
                case '\'':
                    stringBuilder.append("&#39;");
                    break;
                case '/':
                    stringBuilder.append("&#47;");
                    break;
                default:
                    stringBuilder.append(ch);
                    break;
            }
        }
        return stringBuilder.toString();
    }
}
