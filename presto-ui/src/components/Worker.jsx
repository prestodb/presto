import React from "react";
import lazy from "../lazy";
import { PageTitle } from "./PageTitle";
const WorkerStatus = lazy("WorkerStatus");
const WorkerThreadList = lazy("WorkerThreadList");

export const Worker = () => {
    return (
        <>
            <div id="title">
                <PageTitle titles={["Worker Status"]} />
            </div>

            <div id="worker-status">
                <WorkerStatus />
            </div>
            <div id="worker-threads">
                <WorkerThreadList />
            </div>
        </>
    );
};
