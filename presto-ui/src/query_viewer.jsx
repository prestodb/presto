import { createRoot } from 'react-dom/client';
import QueryViewer from "./components/QueryViewer";
import { PageTitle } from "./components/PageTitle";

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={["Query Viewer"]} path='..'/>);

const queryView = createRoot(document.getElementById('query-view-container'));
queryView.render(<QueryViewer/>);

