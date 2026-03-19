import { useEffect, useState } from "react";
import { Switch, Col, Row } from "antd";

import { useUserStore } from "../../store/user-store.js";
import { THEME } from "../../common/constants.js";
import { MoonIcon, SunIcon, WhiteLogo } from "../icons";
import "../layout.css";

const PREFERS_DARK_THEME = window.matchMedia("(prefers-color-scheme: dark)");

function TopNav() {
  const [currentTheme, setCurrentTheme] = useState();
  const userDetails = useUserStore((state) => state.userDetails);
  const updateUserDetails = useUserStore((state) => state.updateUserDetails);

  useEffect(() => {
    if (userDetails.currentTheme) {
      if (userDetails.currentTheme === THEME.DARK) {
        changeTheme(true);
      }
    } else if (PREFERS_DARK_THEME.matches) {
      changeTheme(true);
    }
  }, []);

  function updateTheme(theme = THEME.LIGHT) {
    setCurrentTheme(theme);
    updateUserDetails({ currentTheme: theme });
  }

  function changeTheme(checked) {
    if (checked) {
      document.body.classList.add(THEME.DARK);
    } else {
      document.body.classList.remove(THEME.DARK);
    }
    updateTheme(checked ? THEME.DARK : THEME.LIGHT);
  }

  return (
    <Row align="middle" className="topNav">
      <Col span={4}>
        <Row align="middle">
          <WhiteLogo />
        </Row>
      </Col>
      <Col span={20}>
        <Row justify="end" align="middle">
          <Col>
            <Switch
              onClick={changeTheme}
              checked={currentTheme === THEME.DARK}
              checkedChildren={<MoonIcon />}
              unCheckedChildren={<SunIcon />}
            />
          </Col>
        </Row>
      </Col>
    </Row>
  );
}

export { TopNav };
