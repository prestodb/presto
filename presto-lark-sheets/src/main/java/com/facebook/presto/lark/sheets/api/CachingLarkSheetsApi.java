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

package com.facebook.presto.lark.sheets.api;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class CachingLarkSheetsApi
        implements LarkSheetsApi
{
    private final LarkSheetsApi delegate;
    private final LoadingCache<String, Boolean> readableCache;
    private final LoadingCache<String, SpreadsheetInfo> metaInfoCache;
    private final LoadingCache<HeaderRowKey, List<String>> headerRowCache;

    public CachingLarkSheetsApi(LarkSheetsApi delegate)
    {
        this.delegate = delegate;
        this.readableCache = buildCache(this::loadReadable);
        this.metaInfoCache = buildCache(this::loadMetaInfo);
        this.headerRowCache = buildCache(this::loadHeaderRow);
    }

    @Override
    public boolean isReadable(String token)
    {
        return readableCache.getUnchecked(token);
    }

    private boolean loadReadable(String token)
    {
        return delegate.isReadable(token);
    }

    @Override
    public SpreadsheetInfo getMetaInfo(String token)
    {
        return metaInfoCache.getUnchecked(token);
    }

    private SpreadsheetInfo loadMetaInfo(String token)
    {
        return delegate.getMetaInfo(token);
    }

    @Override
    public List<String> getHeaderRow(String token, String sheetId, int columnCount)
    {
        return headerRowCache.getUnchecked(new HeaderRowKey(token, sheetId, columnCount));
    }

    private List<String> loadHeaderRow(HeaderRowKey key)
    {
        return delegate.getHeaderRow(key.token, key.sheetId, key.columnCount);
    }

    @Override
    public SheetValues getValues(String token, String range)
    {
        return delegate.getValues(token, range);
    }

    private static <K, V> LoadingCache<K, V> buildCache(Function<K, V> loader)
    {
        return CacheBuilder.newBuilder().build(CacheLoader.from(loader::apply));
    }

    private static class HeaderRowKey
    {
        private final String token;
        private final String sheetId;
        private final int columnCount;

        private HeaderRowKey(String token, String sheetId, int columnCount)
        {
            this.token = requireNonNull(token, "token is null");
            this.sheetId = requireNonNull(sheetId, "sheetId is null");
            this.columnCount = columnCount;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HeaderRowKey that = (HeaderRowKey) o;
            return columnCount == that.columnCount && token.equals(that.token) && sheetId.equals(that.sheetId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(token, sheetId, columnCount);
        }
    }
}
