import React from "react";
import {WorkerStatus} from "./components/WorkerStatus";
import {WorkerThreadList} from "./components/WorkerThreadList";
import {PageTitle} from "./components/PageTitle";
import { createRoot } from 'react-dom/client';

const title = createRoot(document.getElementById('title'));

title.render(
    <PageTitle titles={["Worker Status"]} />,
    document.getElementById('title')
);

const worker = createRoot(document.getElementById('worker-status'));
worker.render(
    <WorkerStatus />
);

const threads = createRoot(document.getElementById('worker-threads'));
threads.render(
    <WorkerThreadList />
);
