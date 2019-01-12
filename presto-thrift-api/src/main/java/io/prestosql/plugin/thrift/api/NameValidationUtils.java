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
package io.prestosql.plugin.thrift.api;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

final class NameValidationUtils
{
    private NameValidationUtils() {}

    public static String checkValidName(String name)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        checkArgument('a' <= name.charAt(0) && name.charAt(0) <= 'z', "name must start with a lowercase latin letter: '%s'", name);
        for (int i = 1; i < name.length(); i++) {
            char ch = name.charAt(i);
            checkArgument('a' <= ch && ch <= 'z' || '0' <= ch && ch <= '9' || ch == '_',
                    "name must contain only lowercase latin letters, digits or underscores: '%s'", name);
        }
        return name;
    }
}
