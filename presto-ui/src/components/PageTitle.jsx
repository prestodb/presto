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
//@flow
import React, { useState, useEffect, useRef } from "react";

type Props = {
    titles: string[],
    urls?: string[],
    current?: number,
    path?: string,
};

type State = {
    noConnection: boolean,
    lightShown: boolean,
    info: ?any,
    lastSuccess: number,
    modalShown: boolean,
    errorText: ?string,
};

const ClusterResourceGroupNavBar = ({ titles, urls, current = 0 }: Props) => {
    const classNames = ["navbar-brand inactive", "navbar-brand"];
    const navBarItems = titles.map((title, index) => {
        const classNameIdx = current === index || !urls?.length ? 0 : 1;
        return (
            <td key={index}>
                <span className={classNames[classNameIdx]}>
                    {classNameIdx ? <a href={urls?.[index]}>{title}</a> : title}
                </span>
            </td>
        );
    });

    return <>{navBarItems}</>;
};

const isOffline = () => {
    return window.location.protocol === "file:";
};

export const PageTitle = (props: Props): React.Node => {
    const [state, setState] = useState<State>({
        noConnection: false,
        lightShown: false,
        info: null,
        lastSuccess: Date.now(),
        modalShown: false,
        errorText: null,
    });
    const [clusterTag, setClusterTag] = useState(null);

    const timeoutId = useRef<TimeoutID | null>(null);

    const refreshLoop = () => {
        clearTimeout(timeoutId.current);
        fetch("/v1/info")
            .then((response) => response.json())
            .then((info) => {
                setState((prevState) => ({
                    ...prevState,
                    info: info,
                    noConnection: false,
                    lastSuccess: Date.now(),
                    modalShown: false,
                }));
                //$FlowFixMe$ Bootstrap 5 plugin
                $("#no-connection-modal").hide();
                timeoutId.current = setTimeout(refreshLoop, 1000);
            })
            .catch((error) => {
                setState((prevState) => {
                    const noConnection = true;
                    const lightShown = !prevState.lightShown;
                    const errorText = error;
                    const shouldShowModal =
                        !prevState.modalShown && (error || Date.now() - prevState.lastSuccess > 30 * 1000);

                    if (shouldShowModal) {
                        //$FlowFixMe$ Bootstrap 5 plugin
                        $("#no-connection-modal").modal("show");
                    }

                    timeoutId.current = setTimeout(refreshLoop, 1000);

                    return {
                        ...prevState,
                        noConnection,
                        lightShown,
                        errorText,
                        modalShown: shouldShowModal || prevState.modalShown,
                    };
                });
            });
    };

    useEffect(() => {
        if (isOffline()) {
            setState((prevState) => ({
                ...prevState,
                noConnection: true,
                lightShown: true,
            }));
        } else {
            refreshLoop();
        }

        return () => {
            clearTimeout(timeoutId.current);
        };
    }, []);

    useEffect(() => {
        fetch("/v1/cluster")
            .then((response) => response.json())
            .then((clusterResponse) => {
                setClusterTag(clusterResponse.clusterTag);
            })
            .catch((error) => {
                console.error("Could not fetch cluster response:", error);
            });
    }, []);

    const renderStatusLight = () => {
        if (state.noConnection) {
            if (state.lightShown) {
                return <span className="status-light status-light-red" id="status-indicator" />;
            } else {
                return <span className="status-light" id="status-indicator" />;
            }
        } else {
            return <span className="status-light status-light-green" id="status-indicator" />;
        }
    };

    const { info } = state;
    if (!isOffline() && !info) {
        return null;
    }

    return (
        <div>
            <nav className="navbar navbar-expand-lg navbar-dark bg-dark">
                <div className="container-fluid gap-4">
                    <div className="navbar-header">
                        <table>
                            <tbody>
                                <tr>
                                    <td>
                                        <a href="/ui/">
                                            <img src={`${props.path || "."}/assets/logo.png`} />
                                        </a>
                                    </td>
                                    <ClusterResourceGroupNavBar
                                        titles={props.titles}
                                        urls={props.urls}
                                        current={props.current}
                                    />
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <button
                        className="navbar-toggler"
                        type="button"
                        data-bs-toggle="collapse"
                        data-bs-target="#navbar"
                        aria-controls="navbar"
                        aria-expanded="false"
                        aria-label="Toggle navigation"
                    >
                        <span className="navbar-toggler-icon"></span>
                    </button>
                    <div id="navbar" className="navbar-collapse collapse min-width-0">
                        <ul className="nav navbar-nav navbar-right gap-3 flex-nowrap justify-content-end align-items-center min-width-0 flex-grow-1">
                            <li className="flex-basis-40 min-width-0 flex-grow-1">
                                <div className="navbar-cluster-info">
                                    <div className="uppercase">Version</div>
                                    <div title={info?.nodeVersion?.version} className="text text-truncate" id="version-number">
                                        {isOffline() ? "N/A" : info?.nodeVersion?.version}
                                    </div>
                                </div>
                            </li>
                            <li className="flex-basis-20 min-width-0 flex-shrink-0">
                                <div className="navbar-cluster-info">
                                    <div className="uppercase">Environment</div>
                                    <div className="text" id="environment">
                                        {isOffline() ? "N/A" : info?.environment}
                                    </div>
                                </div>
                            </li>
                            <li className="flex-basis-20 min-width-0 flex-shrink-0">
                                <div className="navbar-cluster-info">
                                    <div className="uppercase">Uptime</div>
                                    <div>
                                        <span data-bs-toggle="tooltip" data-bs-placement="bottom" title="Connection status">
                                            {renderStatusLight()}
                                        </span>
                                        &nbsp;
                                        <span className="text" id="uptime">
                                            {isOffline() ? "Offline" : info?.uptime}
                                        </span>
                                    </div>
                                </div>
                            </li>
                            {clusterTag && (
                                <li key="cluster-tag" className="min-width-0 flex-shrink-0">
                                    <div className="navbar-cluster-info">
                                        <div className="uppercase">Tag</div>
                                        <div className="text" title="Cluster Tag">
                                            <span
                                                title={clusterTag}
                                                className="badge bg-secondary truncated-badge d-inline-block"
                                            >
                                                {clusterTag}
                                            </span>
                                        </div>
                                    </div>
                                </li>
                            )}
                        </ul>
                    </div>
                </div>
            </nav>
            <div id="no-connection-modal" className="modal" tabIndex="-1" role="dialog">
                <div className="modal-dialog modal-sm" role="document">
                    <div className="modal-content">
                        <div className="row error-message">
                            <div className="col-12">
                                <br />
                                <h4>Unable to connect to server</h4>
                                <p>{state.errorText ? "Error: " + state.errorText : null}</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};
