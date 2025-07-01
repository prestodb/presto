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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.HiveSessionProperties.isUseListDirectoryCache;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.util.Objects.requireNonNull;

public class DirectoryListCacheInvalidationProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle CACHE_DATA_INVALIDATION = methodHandle(
            DirectoryListCacheInvalidationProcedure.class,
            "directoryListCacheInvalidation",
            ConnectorSession.class,
            String.class);

    private final DirectoryLister directoryLister;

    @Inject
    public DirectoryListCacheInvalidationProcedure(DirectoryLister directoryLister)
    {
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "invalidate_directory_list_cache",
                ImmutableList.of(
                        new Argument("directory_path", VARCHAR, false, null)),
                CACHE_DATA_INVALIDATION.bindTo(this));
    }

    public void directoryListCacheInvalidation(ConnectorSession session, String directoryPath)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doInvalidateDirectoryListCache(session, Optional.ofNullable(directoryPath));
        }
    }

    private void doInvalidateDirectoryListCache(ConnectorSession session, Optional<String> directoryPath)
    {
        if (isUseListDirectoryCache(session)) {
            CachingDirectoryLister cachingDirectoryLister = (CachingDirectoryLister) directoryLister;
            cachingDirectoryLister.invalidateDirectoryListCache(directoryPath);
        }
        else {
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "Directory list cache is not enabled on this catalog");
        }
    }
}
