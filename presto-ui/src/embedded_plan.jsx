import React from "react";
import {LivePlan} from "./components/LivePlan";
import {getFirstParameter} from "./utils";
import { createRoot } from 'react-dom/client';
const container = document.getElementById('live-plan-container');
const root = createRoot(container);

root.render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={true}/>
);
