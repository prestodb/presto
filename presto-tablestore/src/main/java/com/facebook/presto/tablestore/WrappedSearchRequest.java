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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.search.SearchRequest;

public class WrappedSearchRequest
{
    /**
     * whether we only fetch the row count
     */
    private final boolean onlyFetchRowCount;
    /**
     * the corresponding SearchRequest
     */
    private final SearchRequest searchRequest;

    public WrappedSearchRequest(boolean onlyFetchRowCount, SearchRequest searchRequest)
    {
        this.onlyFetchRowCount = onlyFetchRowCount;
        this.searchRequest = searchRequest;
    }

    public boolean isOnlyFetchRowCount()
    {
        return onlyFetchRowCount;
    }

    public SearchRequest getSearchRequest()
    {
        return searchRequest;
    }
}
