import React from "react";
import {LivePlan} from "./components/LivePlan";
import {PageTitle} from "./components/PageTitle";
import {getFirstParameter} from "./utils";
import { createRoot } from 'react-dom/client';
const container = document.getElementById('title');
const root = createRoot(container);

root.render(
    <PageTitle titles={["Query Details"]} />,
    document.getElementById('title')
);
const live = document.getElementById('live-plan-container');
const live_root = createRoot(live);
live_root.render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={false}/>
);
