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

import java.net.URI;
import java.util.List;
import java.util.Optional;

public interface Scheduler
{
    /**
     * Schedules a request from a user to a concrete candidate. Returns the
     * URI of this candidate.
     */
    Optional<URI> getDestination(String user);

    /**
     * Sets the candidates with the list of URIs for scheduling.
     */
    void setCandidates(List<URI> candidates);
}
