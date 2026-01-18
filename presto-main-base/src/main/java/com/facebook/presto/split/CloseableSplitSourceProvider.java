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
package com.facebook.presto.split;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.metadata.TableFunctionHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CloseableSplitSourceProvider
        implements SplitSourceProvider, Closeable
{
    private static final Logger log = Logger.get(CloseableSplitSourceProvider.class);

    private final SplitManager delegate;

    @GuardedBy("this")
    private List<SplitSource> splitSources = new ArrayList<>();
    @GuardedBy("this")
    private boolean closed;

    public CloseableSplitSourceProvider(SplitManager delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public synchronized SplitSource getSplits(Session session, TableHandle tableHandle, SplitSchedulingStrategy splitSchedulingStrategy, WarningCollector warningCollector)
    {
        checkState(!closed, "split source provider is closed");
        SplitSource splitSource = delegate.getSplits(session, tableHandle, splitSchedulingStrategy, warningCollector);
        splitSources.add(splitSource);
        return splitSource;
    }

    @Override
    public synchronized SplitSource getSplits(Session session, TableFunctionHandle tableFunctionHandle)
    {
        checkState(!closed, "split source provider is closed");
        SplitSource splitSource = delegate.getSplitsForTableFunction(session, tableFunctionHandle);
        splitSources.add(splitSource);
        return splitSource;
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            closed = true;
            for (SplitSource source : splitSources) {
                try {
                    source.close();
                }
                catch (Throwable t) {
                    log.warn(t, "Error closing split source");
                }
            }
            splitSources = null;
        }
    }
}
