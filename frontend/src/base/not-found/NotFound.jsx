import { Button, Typography } from "antd";
import { useNavigate } from "react-router-dom";

import { NotFound404 } from "../icons/index.js";
import "./notfound.css";
const NotFound = () => {
  const navigate = useNavigate();
  return (
    <div className="pagewrap">
      <NotFound404 />
      <div className="desc_wrap">
        <Typography className="notfoundheading">404 Page Not Found</Typography>
        <Typography className="desc">
          Sorry the page you visited does not exist
        </Typography>
        <Button
          style={{ background: "#0C3861", color: "white", marginTop: "30px" }}
          onClick={() => navigate("/")}
        >
          Go Back
        </Button>
      </div>
    </div>
  );
};

export default NotFound;
