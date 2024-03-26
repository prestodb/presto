import { createRoot } from 'react-dom/client';
import { PlanViewer } from "./components/PlanViewer";
import { PageTitle } from "./components/PageTitle";

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={["Plan Viewer"]} path='..'/>);

const planView = createRoot(document.getElementById('plan-view-container'));
planView.render(<PlanViewer/>);

