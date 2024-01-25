import { createRoot } from 'react-dom/client';
import SQLClientView from "./components/SQLClient";
import { PageTitle } from "./components/PageTitle";

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={['Cluster Overview', 'Resource Groups', 'SQL Client']} urls={['./index.html', './res_groups.html', './sql_client.html']} current={2} />);

const resourceGroups = createRoot(document.getElementById('sql-client'));
resourceGroups.render(<SQLClientView />);
