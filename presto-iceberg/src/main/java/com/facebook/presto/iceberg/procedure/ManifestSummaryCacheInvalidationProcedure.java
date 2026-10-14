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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.iceberg.ManifestSummaryCache;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static java.util.Objects.requireNonNull;

public class ManifestSummaryCacheInvalidationProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle CACHE_DATA_INVALIDATION = methodHandle(
            ManifestSummaryCacheInvalidationProcedure.class,
            "manifestSummaryCacheInvalidation");

    private final ManifestSummaryCache manifestSummaryCache;

    @Inject
    public ManifestSummaryCacheInvalidationProcedure(ManifestSummaryCache manifestSummaryCache)
    {
        this.manifestSummaryCache = requireNonNull(manifestSummaryCache, "manifestSummaryCache is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "invalidate_manifest_summary_cache",
                ImmutableList.of(),
                CACHE_DATA_INVALIDATION.bindTo(this));
    }

    public void manifestSummaryCacheInvalidation()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            manifestSummaryCache.invalidateAll();
        }
    }
}
