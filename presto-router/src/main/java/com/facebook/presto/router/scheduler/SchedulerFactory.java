package com.facebook.presto.router.scheduler;

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class SchedulerFactory
{
    private final SchedulerType schedulerType;

    public SchedulerFactory(SchedulerType schedulerType)
    {
        this.schedulerType = requireNonNull(schedulerType, "schedulerType is null");
    }

    public Scheduler create()
    {
        switch (schedulerType) {
            case RANDOM_CHOICE:
                return new RandomChoiceScheduler();
            case USER_HASH:
                return new UserHashScheduler();
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported router scheduler type " + schedulerType);
    }
}
