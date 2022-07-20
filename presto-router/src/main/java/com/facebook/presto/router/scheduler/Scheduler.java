package com.facebook.presto.router.scheduler;

import java.net.URI;
import java.util.List;
import java.util.Optional;

public interface Scheduler
{
    Optional<URI> getDestination(String user);

    void setCandidates(List<URI> candidates);
}
