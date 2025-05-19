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
package io.ahana.eventplugin;

import java.util.ArrayList;
import java.util.List;

public class FilterConfig
{
    private final boolean isFilteringEnabled;
    private final boolean isFilterCatalog;
    private final List<String> filterCatalogs;
    private final boolean isFilterSchemas;
    private final List<String> filterSchemas;
    private final String filterKeyword;
    private final String queryActionType;
    private final boolean queryActionTypeEnabled;
    private final String queryContextSource;
    private final boolean queryContextSourceEnabled;
    private final String queryContextEnv;
    private final boolean queryContextEnvEnabled;

    private FilterConfig(Builder builder)
    {
        this.isFilteringEnabled = builder.isFilteringEnabled;
        this.isFilterCatalog = builder.isFilterCatalog;
        this.filterCatalogs = builder.filterCatalogs;
        this.isFilterSchemas = builder.isFilterSchemas;
        this.filterSchemas = builder.filterSchemas;
        this.filterKeyword = builder.filterKeyword;
        this.queryActionType = builder.queryActionType;
        this.queryActionTypeEnabled = builder.queryActionTypeEnabled;
        this.queryContextSource = builder.queryContextSource;
        this.queryContextSourceEnabled = builder.queryContextSourceEnabled;
        this.queryContextEnv = builder.queryContextEnv;
        this.queryContextEnvEnabled = builder.queryContextEnvEnabled;
    }

    public boolean isFilteringEnabled()
    {
        return isFilteringEnabled;
    }

    public boolean isFilterCatalog()
    {
        return isFilterCatalog;
    }

    public List<String> getFilterCatalogs()
    {
        return filterCatalogs;
    }

    public boolean isFilterSchemas()
    {
        return isFilterSchemas;
    }

    public List<String> getFilterSchemas()
    {
        return filterSchemas;
    }

    public String getFilterKeyword()
    {
        return filterKeyword;
    }

    public String getQueryActionType()
    {
        return queryActionType;
    }

    public boolean isQueryActionTypeEnabled()
    {
        return queryActionTypeEnabled;
    }

    public String getQueryContextSource()
    {
        return queryContextSource;
    }

    public boolean isQueryContextSourceEnabled()
    {
        return queryContextSourceEnabled;
    }

    public String getQueryContextEnv()
    {
        return queryContextEnv;
    }

    public boolean isQueryContextEnvEnabled()
    {
        return queryContextEnvEnabled;
    }

    public static class Builder
    {
        private boolean isFilteringEnabled;
        private boolean isFilterCatalog;
        private List<String> filterCatalogs;
        private boolean isFilterSchemas;
        private List<String> filterSchemas;
        private String filterKeyword;
        private String queryActionType;
        private boolean queryActionTypeEnabled;
        private String queryContextSource;
        private boolean queryContextSourceEnabled;
        private String queryContextEnv;
        private boolean queryContextEnvEnabled;

        public Builder()
        {
            this.filterCatalogs = new ArrayList<>();
            this.filterSchemas = new ArrayList<>();
        }

        public Builder filteringEnabled(boolean isFilteringEnabled)
        {
            this.isFilteringEnabled = isFilteringEnabled;
            return this;
        }

        public Builder isFilterCatalog(boolean isFilterCatalog)
        {
            this.isFilterCatalog = isFilterCatalog;
            return this;
        }

        public Builder addFilterCatalog(String filterCatalog)
        {
            this.filterCatalogs.add(filterCatalog);
            return this;
        }

        public Builder filterCatalogs(List<String> filterCatalogs)
        {
            this.filterCatalogs = filterCatalogs;
            return this;
        }

        public Builder isFilterSchemas(boolean isFilterSchemas)
        {
            this.isFilterSchemas = isFilterSchemas;
            return this;
        }

        public Builder addFilterSchema(String filterSchema)
        {
            this.filterSchemas.add(filterSchema);
            return this;
        }

        public Builder filterSchemas(List<String> filterSchemas)
        {
            this.filterSchemas = filterSchemas;
            return this;
        }

        public Builder filterKeyword(String filterKeyword)
        {
            this.filterKeyword = filterKeyword;
            return this;
        }

        public Builder queryActionType(String queryActionType)
        {
            this.queryActionType = queryActionType;
            return this;
        }

        public Builder queryActionTypeEnabled(boolean queryActionTypeEnabled)
        {
            this.queryActionTypeEnabled = queryActionTypeEnabled;
            return this;
        }

        public Builder queryContextSource(String queryContextSource)
        {
            this.queryContextSource = queryContextSource;
            return this;
        }

        public Builder queryContextSourceEnabled(boolean queryContextSourceEnabled)
        {
            this.queryContextSourceEnabled = queryContextSourceEnabled;
            return this;
        }

        public Builder queryContextEnv(String queryContextEnv)
        {
            this.queryContextEnv = queryContextEnv;
            return this;
        }

        public Builder queryContextEnvEnabled(boolean queryContextEnvEnabled)
        {
            this.queryContextEnvEnabled = queryContextEnvEnabled;
            return this;
        }

        public FilterConfig build()
        {
            return new FilterConfig(this);
        }
    }
}
