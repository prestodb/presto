import React from "react";
import {StageDetail} from "./components/StageDetail";
import {PageTitle} from "./components/PageTitle";
import { createRoot } from 'react-dom/client';
const container = document.getElementById('title');
const root = createRoot(container);

root.render(
    <PageTitle titles={["Query Details"]} />
);
const stageroot = createRoot(document.getElementById('stage-performance-header'));
stageroot.render(
    <StageDetail />
);
