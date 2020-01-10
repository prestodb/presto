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

import java.util.List;

public interface GithubAction
{
    /**
     * List commits starting from {@code earliest}, inclusive, on {@code branch}.
     */
    List<Commit> listCommits(String branch, String earliest);

    /**
     * Create a pull request to merge from origin/branch to upstream/master.
     */
    PullRequest createPullRequest(String branch, String title, String body);
}
