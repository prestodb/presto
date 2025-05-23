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
import com.facebook.presto.router.SimplePrestoClient;
import com.facebook.presto.spi.router.Scheduler;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PreCheckClusterScheduler
        implements Scheduler
{
    private static Integer candidateIndex = 0;
    private static Integer validatorCandidateIndex = 0;
    private static final Logger log = Logger.get(PreCheckClusterScheduler.class);

    private List<URI> candidates;
    private final List<URI> validatorCandidates;
    private final String clientTagForNativeClusterRouting;

    @Inject
    public PreCheckClusterScheduler(PreCheckClusterConfig preCheckClusterConfig)
    {
        requireNonNull(preCheckClusterConfig, "preCheckClusterConfig is null");
        this.validatorCandidates =
                requireNonNull(preCheckClusterConfig.getValidatorURIs(), "validatorCandidates is null");
        this.clientTagForNativeClusterRouting =
                requireNonNull(preCheckClusterConfig.getClientTagForNativeClusterRouting(), "clientTagForNativeClusterRouting is null");
    }

    @Override
    public Optional<URI> getDestination(String user)
    {
        return Optional.empty();
    }

    @Override
    public Optional<URI> getDestination(String user, String statement)
    {
        try {
            synchronized (candidateIndex) {
                if (candidateIndex >= candidates.size()) {
                    candidateIndex = 0;
                }
                return Optional.ofNullable(candidates.get(candidateIndex++));
            }
        }
        catch (IllegalArgumentException e) {
            log.warn(e, "Error getting validator destination for user " + user);
            return Optional.empty();
        }
    }

    @Override
    public Optional<List<String>> getExtraClientTags(String statement, HttpServletRequest httpServletRequest)
    {
        SimplePrestoClient simplePrestoClient = new SimplePrestoClient(getValidatorDestination(statement), clientTagForNativeClusterRouting);
        return simplePrestoClient.getCompatibleClusterClientTags(httpServletRequest, statement);
    }

    private URI getValidatorDestination(String user)
    {
        try {
            synchronized (validatorCandidateIndex) {
                if (validatorCandidateIndex >= candidates.size()) {
                    validatorCandidateIndex = 0;
                }
                return validatorCandidates.get(validatorCandidateIndex++);
            }
        }
        catch (IllegalArgumentException e) {
            log.warn(e, "Error getting validator destination for user " + user);
            return null;
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
