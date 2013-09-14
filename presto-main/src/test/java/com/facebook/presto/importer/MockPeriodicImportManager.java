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
package com.facebook.presto.importer;

import com.google.common.base.Predicate;

import java.util.List;

public class MockPeriodicImportManager
        implements PeriodicImportManager
{
    @Override
    public long insertJob(PeriodicImportJob job)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropJob(long jobId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropJobs(Predicate<PersistentPeriodicImportJob> jobPredicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJobCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PersistentPeriodicImportJob getJob(long jobId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PersistentPeriodicImportJob> getJobs()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long beginRun(long jobId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void endRun(long runId, boolean result)
    {
        throw new UnsupportedOperationException();
    }
}
