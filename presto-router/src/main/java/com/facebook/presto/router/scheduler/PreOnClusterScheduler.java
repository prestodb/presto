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
import com.facebook.presto.spi.PrestoException;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;

public class PreOnClusterScheduler
        implements Scheduler
{
    private static Integer validatorCandidateIndex = 0;
    private static final Logger log = Logger.get(PreOnClusterScheduler.class);

    private List<URI> candidates;
    private final List<URI> validatorCandidates;

    @Inject
    public PreOnClusterScheduler(Optional<List<URI>> validatorUris)
    {
        this.validatorCandidates = validatorUris.orElseThrow(() -> new PrestoException(NOT_FOUND, "Validator URIs not found"));
    }

    @Override
    public Optional<URI> getDestination(String user, Map<String, String> headers, String sql)
    {
        SimplePrestoClient simplePrestoClient = new SimplePrestoClient(getValidatorDestination(user));
        return simplePrestoClient.executeQuery(sql, headers);
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
