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
