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
package io.prestosql.plugin.mongodb;

import com.mongodb.ReadPreference;

import static java.util.Objects.requireNonNull;

public enum ReadPreferenceType
{
    PRIMARY(ReadPreference.primary()),
    PRIMARY_PREFERRED(ReadPreference.primaryPreferred()),
    SECONDARY(ReadPreference.secondary()),
    SECONDARY_PREFERRED(ReadPreference.secondaryPreferred()),
    NEAREST(ReadPreference.nearest());

    private final ReadPreference readPreference;

    ReadPreferenceType(ReadPreference readPreference)
    {
        this.readPreference = requireNonNull(readPreference, "readPreference is null");
    }

    public ReadPreference getReadPreference()
    {
        return readPreference;
    }
}
