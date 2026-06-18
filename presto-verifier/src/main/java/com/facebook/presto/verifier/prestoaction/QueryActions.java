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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.verifier.framework.ClusterType;

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class QueryActions
{
    private final PrestoAction helperAction;
    private final QueryAction controlAction;
    private final QueryAction testAction;

    public QueryActions(PrestoAction helperAction, QueryAction controlAction, QueryAction testAction)
    {
        this.helperAction = requireNonNull(helperAction, "helperAction is null");
        this.controlAction = requireNonNull(controlAction, "controlAction is null");
        this.testAction = requireNonNull(testAction, "testAction is null");
    }

    public PrestoAction getHelperAction()
    {
        return helperAction;
    }

    public QueryAction getControlAction()
    {
        return controlAction;
    }

    public QueryAction getTestAction()
    {
        return testAction;
    }

    public QueryAction getQueryAction(ClusterType clusterType)
    {
        checkArgument(clusterType == CONTROL || clusterType == TEST, "Invalid ClusterType: %s", clusterType);
        return clusterType == CONTROL ? getControlAction() : getTestAction();
    }
}
