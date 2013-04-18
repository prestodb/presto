package com.facebook.presto.importer;

import com.facebook.presto.importer.JobStateFactory.JobState;
import com.facebook.presto.metadata.Metadata;

import javax.inject.Inject;
import javax.inject.Singleton;

public class PeriodicImportRunnable
        extends AbstractPeriodicImportRunnable
{
    @SuppressWarnings("unused")
    private final Metadata metadata;

    @Inject
    public PeriodicImportRunnable(
            PeriodicImportManager periodicImportManager,
            Metadata metadata,
            JobState jobState)
    {
        super(jobState, periodicImportManager);

        this.metadata = metadata;
    }

    @Override
    public void doRun()
            throws Exception
    {
        throw new IllegalStateException("I NEED FIXING!");
    }

    @Singleton
    public static final class PeriodicImportRunnableFactory
    {
        private final PeriodicImportManager periodicImportManager;
        private final Metadata metadata;

        @Inject
        public PeriodicImportRunnableFactory(
                PeriodicImportManager periodicImportManager,
                Metadata metadata)
        {
            this.periodicImportManager = periodicImportManager;
            this.metadata = metadata;
        }

        public Runnable create(JobState jobState)
        {
            return new PeriodicImportRunnable(periodicImportManager, metadata, jobState);
        }
    }
}
