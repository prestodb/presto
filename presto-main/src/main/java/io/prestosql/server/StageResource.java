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
package io.prestosql.server;

import io.prestosql.execution.QueryManager;
import io.prestosql.execution.StageId;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import static java.util.Objects.requireNonNull;

@Path("/v1/stage")
public class StageResource
{
    private final QueryManager queryManager;

    @Inject
    public StageResource(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @DELETE
    @Path("{stageId}")
    public void cancelStage(@PathParam("stageId") StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");
        queryManager.cancelStage(stageId);
    }
}
