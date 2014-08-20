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

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class PrestoS3FileSystemStats
{
    public PrestoS3FileSystemStats() {}

    private final CounterStat activeConnections = new CounterStat();
    private final CounterStat startedUploads = new CounterStat();
    private final CounterStat failedUploads = new CounterStat();
    private final CounterStat successfulUploads = new CounterStat();
    private final CounterStat metadataCalls = new CounterStat();
    private final CounterStat listStatusCalls = new CounterStat();
    private final CounterStat listLocatedStatusCalls = new CounterStat();
    private final CounterStat listObjectsCalls = new CounterStat();

    @Managed
    @Nested
    public CounterStat getActiveConnections()
    {
        return activeConnections;
    }

    @Managed
    @Nested
    public CounterStat getStartedUploads()
    {
        return startedUploads;
    }

    @Managed
    @Nested
    public CounterStat getFailedUploads()
    {
        return failedUploads;
    }

    @Managed
    @Nested
    public CounterStat getSuccessfulUploads()
    {
        return successfulUploads;
    }

    @Managed
    @Nested
    public CounterStat getMetadataCalls()
    {
        return metadataCalls;
    }

    @Managed
    @Nested
    public CounterStat getListStatusCalls()
    {
        return listStatusCalls;
    }

    @Managed
    @Nested
    public CounterStat getListLocatedStatusCalls()
    {
        return listLocatedStatusCalls;
    }

    @Managed
    @Nested
    public CounterStat getListObjectsCalls()
    {
        return listObjectsCalls;
    }

    public void connectionOpened()
    {
        activeConnections.update(1);
    }

    public void connectionReleased()
    {
        activeConnections.update(-1);
    }

    public void uploadStarted()
    {
        startedUploads.update(1);
    }

    public void uploadFailed()
    {
        failedUploads.update(1);
    }

    public void uploadSuccessful()
    {
        successfulUploads.update(1);
    }

    public void newMetadataCall()
    {
        metadataCalls.update(1);
    }

    public void newListStatusCall()
    {
        listStatusCalls.update(1);
    }

    public void newListLocatedStatusCall()
    {
        listLocatedStatusCalls.update(1);
    }

    public void newListObjectsCall()
    {
        listObjectsCalls.update(1);
    }
}
