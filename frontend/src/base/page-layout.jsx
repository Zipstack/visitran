import { Outlet } from "react-router-dom";
import "./layout.css";

function PageLayout() {
  return (
    <div className="home">
      <div className="contentContainer">{<Outlet />}</div>
    </div>
  );
}

export { PageLayout };
