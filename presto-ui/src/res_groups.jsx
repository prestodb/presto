import ReactDOM from "react-dom/client";
import lazy from "./lazy";

const ResourceGroupView = lazy("ResourceGroupView");

ReactDOM.createRoot(document.getElementById("resource-groups-root")).render(<ResourceGroupView />);
