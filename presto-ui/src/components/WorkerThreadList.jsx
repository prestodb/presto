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

import React, { useState, useEffect, useCallback } from "react";

import { clsx } from 'clsx';
import {getFirstParameter} from "../utils";

const ALL_THREADS = "All Threads";
const QUERY_THREADS = "Running Queries";

const ALL_THREAD_STATE = "ALL";
const THREAD_STATES = [ALL_THREAD_STATE, "RUNNABLE", "BLOCKED", "WAITING", "TIMED_WAITING", "NEW", "TERMINATED"];
const QUERY_THREAD_REGEX = new RegExp(/([0-9])*_([0-9])*_([0-9])*_.*?\.([0-9])*\.([0-9])*-([0-9])*-([0-9])*/);
const THREAD_GROUP_REGEXP = new RegExp(/(.*?)-[0-9]+/);

const getRequestQuery = (id) => {
    // Node ID does not have a common pattern
    if (id.length === 0) {
        return "/v1/worker/undefined/thread";
    }
    return `/v1/worker/${encodeURIComponent(id)}/thread`;
};

const processThreads = (threads) => {
    const result = {};

    result[ALL_THREADS] = threads;
    result[QUERY_THREADS] = [];

    for (let i = 0; i < threads.length; i++) {
        const thread = threads[i];
        if (thread.name.match(QUERY_THREAD_REGEX)) {
            result[QUERY_THREADS].push(thread)
        }

        const match = THREAD_GROUP_REGEXP.exec(thread.name);
        const threadGroup = match ? match[1] : thread.name;
        if (!result[threadGroup]) {
            result[threadGroup] = [];
        }
        result[threadGroup].push(thread);
    }

    return result
};

const WorkerThreadList = () => {
    const [initialized, setInitialized] = useState(false);
    const [threads, setThreads] = useState(null);
    const [snapshotTime, setSnapshotTime] = useState(null);
    const [selectedGroup, setSelectedGroup] = useState(ALL_THREADS);
    const [selectedThreadState, setSelectedThreadState] = useState(ALL_THREAD_STATE);

    const captureSnapshot = useCallback(() => {
        $.get(getRequestQuery(getFirstParameter(window.location.search)), function (thr) {
            setThreads(processThreads(thr));
            setSnapshotTime(new Date());
            setInitialized(true);
        }).fail(function () {
            setInitialized(true);
        });
    }, []);

    useEffect(() => {
        // eslint-disable-next-line no-undef
        new Clipboard('.copy-button');
    });

    const filterThreads = useCallback((group, state) => {
        return threads[group].filter(t => t.state === state || state === ALL_THREAD_STATE);
    }, [threads]);

    const handleGroupClick = useCallback((group, event) => {
        setSelectedGroup(group);
        event.preventDefault();
    }, []);

    const handleThreadStateClick = useCallback((state, event) => {
        setSelectedThreadState(state);
        event.preventDefault();
    }, []);

    const handleNewSnapshotClick = useCallback((event) => {
        setInitialized(false);
        captureSnapshot();
        event.preventDefault();
    }, [captureSnapshot]);

    const renderStackLine = useCallback((threadId) => (stackLine, index) => (
        <div key={threadId + index}>
            &nbsp;&nbsp;at {stackLine.className}.{stackLine.method}
            (<span className="font-light">{stackLine.file}:{stackLine.line}</span>)
        </div>
    ), []);

    const renderThread = useCallback((threadInfo) => (
        <div key={threadInfo.id}>
            <span className="font-white">{threadInfo.name} {threadInfo.state} #{threadInfo.id} {threadInfo.lockOwnerId}</span>
            <a className="copy-button" data-clipboard-target={"#stack-trace-" + threadInfo.id} data-bs-toggle="tooltip" data-placement="right"
               title="Copy to clipboard">
                <span className="bi bi-copy" alt="Copy to clipboard"/>
            </a>
            <br/>
            <span className="stack-traces" id={"stack-trace-" + threadInfo.id}>
                {threadInfo.stackTrace.map(renderStackLine(threadInfo.id))}
            </span>
            <div>
                &nbsp;
            </div>
        </div>
    ), [renderStackLine]);

    const renderGroupListItem = useCallback((group) => (
        <li key={group}>
            <a href="#"
               className={clsx('dropdown-item text-dark', selectedGroup === group ? "selected" : "")}
               onClick={(e) => handleGroupClick(group, e)}>
                {group} ({filterThreads(group, selectedThreadState).length})
            </a>
        </li>
    ), [filterThreads, handleGroupClick, selectedGroup, selectedThreadState]);

    const renderThreadStateListItem = useCallback((threadState) => (
        <li key={threadState}>
            <a href="#"
               className={clsx('dropdown-item text-dark', selectedThreadState === threadState ? "selected" : "")}
               onClick={(e) => handleThreadStateClick(threadState, e)}>
                {threadState} ({filterThreads(selectedGroup, threadState).length})
            </a>
        </li>
    ), [filterThreads, handleThreadStateClick, selectedGroup, selectedThreadState]);

    let display = null;
    let toolbar = null;
    if (threads === null) {
        if (initialized === false) {
            display = (
                <div className="row error-message">
                    <div className="col-12">
                        <button className="btn btn-info rounded-0 text-white dropdown-text"
                                onClick={handleNewSnapshotClick}>
                            Capture Snapshot
                        </button>
                    </div>
                </div>
            );
        }
        else {
            display = (
                <div className="row error-message">
                    <div className="col-12"><h4 className="therad-snapshot-text">Thread snapshot could not be loaded</h4></div>
                </div>
            );
        }
    }
    else {
        toolbar = (
            <div className="col-9">
                <table className="header-inline-links">
                    <tbody>
                    <tr>
                        <td>
                            <small>Snapshot at {snapshotTime ? snapshotTime.toTimeString() : ''}</small>
                            &nbsp;&nbsp;
                        </td>
                        <td>
                            <button
                                className="btn btn-info rounded-0 text-white dropdown-text"
                                onClick={handleNewSnapshotClick}>
                                New Snapshot
                            </button>
                            &nbsp;&nbsp;
                            &nbsp;&nbsp;
                        </td>
                        <td>
                            <div className="input-group-btn text-right">
                                <button type="button" className="btn btn-default dropdown-toggle bg-white text-dark rounded-0 dropdown-text" data-bs-toggle="dropdown" aria-haspopup="true"
                                        aria-expanded="false">
                                    <strong>Group:</strong> {selectedGroup} <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu bg-white rounded-0 dropdown-text">
                                    {Object.keys(threads).map(group => renderGroupListItem(group))}
                                </ul>
                            </div>
                        </td>
                        <td>
                            <div className="input-group-btn text-right">
                                <button type="button" className="btn btn-default dropdown-toggle bg-white text-dark rounded-0 dropdown-text" data-bs-toggle="dropdown" aria-haspopup="true"
                                        aria-expanded="false">
                                    <strong>State:</strong> {selectedThreadState} <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu bg-white rounded-0 dropdown-text">
                                    {THREAD_STATES.map(state => renderThreadStateListItem(state))}
                                </ul>
                            </div>
                        </td>
                    </tr>
                    </tbody>
                </table>
            </div>
        );

        const filteredThreads = filterThreads(selectedGroup, selectedThreadState);
        let displayedThreads;
        if (filteredThreads.length === 0 && selectedThreadState === ALL_THREAD_STATE) {
            displayedThreads = (
                <div className="row error-message">
                    <div className="col-12"><h4>No threads in group '{selectedGroup}'</h4></div>
                </div>);
        }
        else if (filteredThreads.length === 0 && selectedGroup === ALL_THREADS) {
            displayedThreads = (
                <div className="row error-message">
                    <div className="col-12"><h4>No threads with state {selectedThreadState}</h4></div>
                </div>);
        }
        else if (filteredThreads.length === 0) {
            displayedThreads = (
                <div className="row error-message">
                    <div className="col-12"><h4>No threads in group '{selectedGroup}' with state {selectedThreadState}</h4></div>
                </div>);
        }
        else {
            displayedThreads = (
                <pre>
                    {filteredThreads.map(t => renderThread(t))}
                </pre>);
        }

        display = (
            <div id="stack-traces">
                {displayedThreads}
            </div>
        );
    }

    return (
        <div>
            <div className="row">
                <div className="col-3">
                    <h3 style={{fontSize: '24px'}}>
                        Thread Snapshot
                        <a className="btn copy-button" data-clipboard-target="#stack-traces" data-bs-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                            <span className="bi bi-copy" alt="Copy to clipboard" style={{color:"#98E8FF", fontSize:"14px"}}/>
                        </a>
                        &nbsp;
                    </h3>
                </div>
                {toolbar}
            </div>
            <div className="row">
                <div className="col-12">
                    <hr className="h3-hr"/>
                    {display}
                </div>
            </div>
        </div>
    );
};

export default WorkerThreadList;