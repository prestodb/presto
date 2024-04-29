import { createRoot } from 'react-dom/client';
import ResourceGroupView from "./components/ResourceGroupView";
import { PageTitle } from "./components/PageTitle";

const title = createRoot(document.getElementById('title'));
title.render(<PageTitle titles={['Cluster Overview', 'Resource Groups', 'SQL Client']} urls={['./index.html', './res_groups.html', 'sql_client.html']} current={1} />);

const resourceGroups = createRoot(document.getElementById('resource-groups'));
resourceGroups.render(<ResourceGroupView />);
