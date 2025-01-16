import { createRoot } from 'react-dom/client';
import lazy from "./lazy";
import { PageTitle } from "./components/PageTitle";

const QueryViewer = lazy('QueryViewer');

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={["Query Viewer"]} path='..'/>);

const queryView = createRoot(document.getElementById('query-view-container'));
queryView.render(<QueryViewer/>);

