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

package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.OrcFileWriterFactory;
import com.facebook.presto.hive.SortingFileWriterConfig;
import com.facebook.presto.spi.PageSorter;
import jakarta.inject.Inject;

public class SortParameters
{
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final OrcFileWriterFactory orcFileWriterFactory;
    private final SortingFileWriterConfig sortingFileWriterConfig;

    @Inject
    public SortParameters(SortingFileWriterConfig sortingFileWriterConfig, TypeManager typeManager, PageSorter pageSorter, OrcFileWriterFactory orcFileWriterFactory)
    {
        this.sortingFileWriterConfig = sortingFileWriterConfig;
        this.typeManager = typeManager;
        this.pageSorter = pageSorter;
        this.orcFileWriterFactory = orcFileWriterFactory;
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    public PageSorter getPageSorter()
    {
        return pageSorter;
    }

    public OrcFileWriterFactory getOrcFileWriterFactory()
    {
        return orcFileWriterFactory;
    }

    public SortingFileWriterConfig getSortingFileWriterConfig()
    {
        return sortingFileWriterConfig;
    }
}
