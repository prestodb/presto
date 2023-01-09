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

// As the round-robin scheduler keeps the selected index as a state for scheduling,
// the candidates shall not be modified after they are assigned.
// This design indicates that the round-robin scheduler can only be used when
// the candidates are always consistent.
public class RoundRobinScheduler
        implements Scheduler
{
    private List<URI> candidates;

    private static Integer candidateIndex = 0;
    private static final Logger log = Logger.get(RoundRobinScheduler.class);

    @Override
    public Optional<URI> getDestination(String user)
    {
        try {
            synchronized (candidateIndex) {
                if (candidateIndex >= candidates.size()) {
                    candidateIndex = 0;
                }
                return Optional.of(candidates.get(candidateIndex++));
            }
        }
        catch (IllegalArgumentException e) {
            log.warn(e, "Error getting destination for user " + user);
            return Optional.empty();
        }
    }

    public void setCandidates(List<URI> candidates)
    {
        // Only keeps the first given `candidates` due to maintaining the
        // selected index for round-robin.
        if (this.candidates == null) {
            this.candidates = candidates;
        }
    }

    public List<URI> getCandidates()
    {
        return candidates;
    }
}
