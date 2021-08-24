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
package com.facebook.presto.tests;

import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.spi.PrestoWarning;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ResultsSession<T>
{
    default void setUpdateType(String type)
    {
        throw new UnsupportedOperationException();
    }

    default void setUpdateCount(long count)
    {
        throw new UnsupportedOperationException();
    }

    default void setWarnings(List<PrestoWarning> warnings)
    {
        throw new UnsupportedOperationException();
    }

    void addResults(QueryStatusInfo statusInfo, QueryData data);

    T build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties);
}
