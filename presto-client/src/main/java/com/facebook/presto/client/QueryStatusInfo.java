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
package com.facebook.presto.client;

import com.facebook.presto.spi.PrestoWarning;

import java.net.URI;
import java.util.List;

public interface QueryStatusInfo
{
    String getId();

    URI getInfoUri();

    URI getPartialCancelUri();

    URI getNextUri();

    List<Column> getColumns();

    StatementStats getStats();

    QueryError getError();

    List<PrestoWarning> getWarnings();

    String getUpdateType();

    Long getUpdateCount();
}
