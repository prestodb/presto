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

import { Timeline, DataSet } from "vis-timeline/standalone";
import { useRef, useEffect, useState } from 'react';
import { getFirstParameter } from "../utils";
import { QueryHeader } from "./QueryHeader";


export default function Split(): void {

    const containerRef = useRef(null);
    const timelineRef = useRef(null);
    const timerid = useRef(0);
    const [queryState, setQueryState] = useState({query: null, failed: false, ended: false});

    function calculateItemsGroups(query) {
        const getTasks = (stage) => {
            return [].concat.apply(
                    stage.latestAttemptExecutionInfo.tasks,
                    stage.subStages.map(getTasks));
        }
        let tasks = getTasks(query.outputStage);
        tasks = tasks.map((task) => {
            return {
                taskId: task.taskId.substring(task.taskId.indexOf('.') + 1),
                time: {
                    create: task.stats.createTime,
                    firstStart: task.stats.firstStartTime,
                    lastStart: task.stats.lastStartTime,
                    lastEnd: task.stats.lastEndTime,
                    end: task.stats.endTime,
                },
            };
        });

        const groups = new DataSet();
        const items = new DataSet();

        // Initializes or updates the timelineRef
        const updateTimeline = () => {
            if (!containerRef.current) return;
            if (timelineRef.current) {
                timelineRef.current.setData({groups, items});
                timelineRef.current.fit();
            } else {
                timelineRef.current = new Timeline(
                    containerRef.current,
                    items,
                    groups,
                    {
                        stack: false,
                        groupOrder: 'sort',
                        margin: 0,
                        clickToUse: true,
                    });
            }
        }

        for (const task of tasks) {
            const [stageId, _, taskNumberStr] = task.taskId.split('.');
            const taskNumber = parseInt(taskNumberStr);
            if (taskNumber === 0) {
                groups.add({
                    id: stageId,
                    content: stageId,
                    sort: stageId,
                    subgroupOrder: 'sort',
                });
            }
            if (task.time.create) {
                items.add({
                    group: stageId,
                    start: task.time.create,
                    end: task.time.firstStart,
                    className: 'red',
                    subgroup: taskNumber,
                    sort: -taskNumber,
                });
            }
            if (task.time.firstStart) {
                items.add({
                    group: stageId,
                    start: task.time.firstStart,
                    end: task.time.lastStart,
                    className: 'green',
                    subgroup: taskNumber,
                    sort: -taskNumber,
                });
            }
            if (task.time.lastStart) {
                items.add({
                    group: stageId,
                    start: task.time.lastStart,
                    end: task.time.lastEnd,
                    className: 'blue',
                    subgroup: taskNumber,
                    sort: -taskNumber,
                });
            }
            if (task.time.lastEnd) {
                items.add({
                    group: stageId,
                    start: task.time.lastEnd,
                    end: task.time.end,
                    className: 'orange',
                    subgroup: taskNumber,
                    sort: -taskNumber,
                });
            }
        }
        if (timerid.current !== 0) {
            clearTimeout(timerid.current);
            timerid.current = 0;
        }
        const newQueryState = {query, ended: query.finalQueryInfo, failed: false};
        if (newQueryState.ended === false && newQueryState.failed === false && timerid.current === 0) {
            timerid.current = setTimeout(queryResult, 3000);
        }

        updateTimeline();
        setQueryState(newQueryState);
    };

    function queryResult() {
        const queryId = getFirstParameter(window.location.search);
        fetch('/v1/query/' + queryId)
        .then( (response) => {
            if (!response.ok) {
                throw new Error('failed to get query details');
            }
            return response.json();
        })
        .then( (query) => {
            calculateItemsGroups(query);
        })
        .catch((err) => {
            console.log(`query failed with error: ${err}`);
            setQueryState({failed: true});
        });
    }

    useEffect(() => {
        queryResult();
    }, [containerRef]);

    return (
        <>
            { queryState.query && <QueryHeader query={queryState.query}/>}
            {(!queryState.query || queryState.ended === false) && <div className="row error-message">
                    <div className="col-12">
                    { queryState.failed && queryState.query === null ? (
                    <h4>Query not found</h4>
                    ) : (
                    <h4><div className="loader">Loading...</div></h4>
                    )}
                    </div>
            </div>}
            <div id="legend" className="row">
                <div>
                    <div className="red bar"></div>
                    <div className="text">Created</div>
                </div>
                <div>
                    <div className="green bar"></div>
                    <div className="text">First split started</div>
                </div>
                <div>
                    <div className="blue bar"></div>
                    <div className="text">Last split started</div>
                </div>
                <div>
                    <div className="orange bar"></div>
                    <div className="text">Last split ended</div>
                </div>
                <div>
                    <div className="bar empty"></div>
                    <div className="text">Ended</div>
                </div>
            </div>
            <div ref={containerRef} id="timeline"/>
        </>
    );
}
