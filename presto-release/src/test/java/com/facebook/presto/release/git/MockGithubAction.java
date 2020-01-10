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
package com.facebook.presto.release.git;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class MockGithubAction
        implements GithubAction
{
    private final List<Commit> commits;
    private PullRequest pullRequest;

    public MockGithubAction(List<Commit> commits)
    {
        this.commits = ImmutableList.copyOf(commits);
    }

    @Override
    public List<Commit> listCommits(String latest, String earliest)
    {
        return commits;
    }

    @Override
    public PullRequest createPullRequest(String branch, String title, String body)
    {
        checkState(pullRequest == null, "Can only create one pull request");
        pullRequest = new PullRequest(0, title, "", body, new Actor("test"), null);
        return pullRequest;
    }

    public PullRequest getCreatedPullRequest()
    {
        return pullRequest;
    }
}
