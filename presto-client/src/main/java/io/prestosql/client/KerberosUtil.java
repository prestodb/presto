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
package io.prestosql.client;

import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;

public final class KerberosUtil
{
    private static final String FILE_PREFIX = "FILE:";

    private KerberosUtil() {}

    public static Optional<String> defaultCredentialCachePath()
    {
        String value = nullToEmpty(System.getenv("KRB5CCNAME"));
        if (value.startsWith(FILE_PREFIX)) {
            value = value.substring(FILE_PREFIX.length());
        }
        return Optional.ofNullable(emptyToNull(value));
    }
}
