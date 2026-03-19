import { memo } from "react";
import { Switch } from "antd";
import PropTypes from "prop-types";

import { MoonIcon, SunIcon } from "../../../base/icons";
import { THEME } from "../../../common/constants";

const ThemeSwitcher = memo(({ currentTheme, onThemeChange }) => (
  <Switch
    onClick={onThemeChange}
    checked={currentTheme === THEME.DARK}
    checkedChildren={<MoonIcon />}
    unCheckedChildren={<SunIcon />}
  />
));

ThemeSwitcher.displayName = "ThemeSwitcher";

ThemeSwitcher.propTypes = {
  currentTheme: PropTypes.string,
  onThemeChange: PropTypes.func.isRequired,
};

export { ThemeSwitcher };
