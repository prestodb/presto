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
            return Optional.of(candidates.get(RANDOM.nextInt(candidates.size())));
        }
        catch (IllegalArgumentException e) {
            log.warn(e, "Error getting destination for user " + user);
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
