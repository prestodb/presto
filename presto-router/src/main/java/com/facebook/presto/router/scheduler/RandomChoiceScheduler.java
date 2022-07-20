package com.facebook.presto.router.scheduler;

import com.facebook.airlift.log.Logger;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class RandomChoiceScheduler
        implements Scheduler
{
    private List<URI> candidates;

    private static final Random RANDOM = new Random();
    private static final Logger log = Logger.get(RandomChoiceScheduler.class);

    @Override
    public Optional<URI> getDestination(String user)
    {
        try {
            return Optional.ofNullable(candidates.get(RANDOM.nextInt(candidates.size())));
        }
        catch (Exception e) {
            log.warn(e, "Error getting destination for user ", user);
            return Optional.empty();
        }
    }

    public void setCandidates(List<URI> candidates)
    {
        this.candidates = candidates;
    }

    public List<URI> getCandidates()
    {
        return candidates;
    }
}
