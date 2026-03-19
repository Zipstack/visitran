import { Button, Typography } from "antd";
import PropTypes from "prop-types";
import { CloseOutlined } from "@ant-design/icons";

import {
  FlowsheetLightIcon,
  FlowsheetDarkIcon,
} from "../../base/icons/index.js";
import { useUserStore } from "../../store/user-store";
import { THEME } from "../../common/constants.js";

const { Text } = Typography;

function Header({ closeSequenceDrawer }) {
  const userDetails = useUserStore((state) => state.userDetails);
  const isDarkTheme = userDetails?.currentTheme === THEME.DARK;

  return (
    <div className="flex-space-between chat-ai-header">
      <div className="sequence-drawer-header-title">
        {isDarkTheme ? (
          <FlowsheetDarkIcon className="sequence-drawer-header-icon" />
        ) : (
          <FlowsheetLightIcon className="sequence-drawer-header-icon" />
        )}
        <Text strong>Transformation Sequence</Text>
      </div>
      <div>
        <Button
          type="text"
          size="small"
          icon={<CloseOutlined />}
          onClick={closeSequenceDrawer}
        />
      </div>
    </div>
  );
}

Header.propTypes = {
  closeSequenceDrawer: PropTypes.func.isRequired,
};

export { Header };
