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
package com.facebook.presto.verifier.framework;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class ChecksumQueryContext
{
    private Optional<String> checksumQueryId = Optional.empty();
    private Optional<String> checksumQuery = Optional.empty();

    public Optional<String> getChecksumQueryId()
    {
        return checksumQueryId;
    }

    public void setChecksumQueryId(String checksumQueryId)
    {
        checkState(!this.checksumQueryId.isPresent(), "controlChecksumQueryId is already set");
        this.checksumQueryId = Optional.of(checksumQueryId);
    }

    public Optional<String> getChecksumQuery()
    {
        return checksumQuery;
    }

    public void setChecksumQuery(String checksumQuery)
    {
        checkState(!this.checksumQuery.isPresent(), "controlChecksumQuery is already set");
        this.checksumQuery = Optional.of(checksumQuery);
    }
}
