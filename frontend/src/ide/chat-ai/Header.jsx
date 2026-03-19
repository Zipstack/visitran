import { memo, useState } from "react";
import PropTypes from "prop-types";
import { Button, Space, Typography } from "antd";
import {
  FileDoneOutlined,
  HistoryOutlined,
  CloseOutlined,
  MinusOutlined,
  PlusOutlined,
  RocketOutlined,
  ExpandOutlined,
  CompressOutlined,
} from "@ant-design/icons";

import { ContextRulesPanel } from "./ContextRulesPanel";
import { orgStore } from "../../store/org-store";

let TokenTopupDrawer;
try {
  TokenTopupDrawer = require("../../plugins/settings/TokenTopupDrawer").default;
} catch {
  /* plugin not available */
}

const Header = memo(function Header({
  resetSelectedChatId,
  closeChatDrawer,
  collapseDrawer,
  toggleFullWidth,
  isFullWidth,
  isPromptRunning,
  // Onboarding props
  isOnboardingCompleted,
  onResetOnboarding,
  // Token balance props
  tokenBalance,
  onBuyTokens,
}) {
  const [isSettingsPanelVisible, setIsSettingsPanelVisible] = useState(false);
  const [isTokenTopupDrawerVisible, setIsTokenTopupDrawerVisible] =
    useState(false);
  const { selectedOrgId } = orgStore();

  const handleSettingsClick = () => {
    setIsSettingsPanelVisible(true);
  };

  const handleSettingsPanelClose = () => {
    setIsSettingsPanelVisible(false);
  };

  const handleTokenTopupDrawerClose = () => {
    setIsTokenTopupDrawerVisible(false);
  };

  return (
    <>
      <div className="flex-space-between chat-ai-header">
        <div>
          <Typography.Text strong>Visitran AI</Typography.Text>
        </div>
        <div>
          <Space>
            <Button
              type="text"
              size="small"
              icon={<PlusOutlined />}
              disabled={isPromptRunning}
              onClick={resetSelectedChatId}
            />
            <Button
              type="text"
              size="small"
              icon={<FileDoneOutlined />}
              disabled={isPromptRunning}
              onClick={handleSettingsClick}
              title="Context & Rules Manager"
            />
            <Button
              type="text"
              size="small"
              icon={<HistoryOutlined />}
              disabled={isPromptRunning}
              onClick={resetSelectedChatId}
            />
            {isOnboardingCompleted && (
              <Button
                type="text"
                size="small"
                icon={<RocketOutlined />}
                disabled={isPromptRunning}
                onClick={onResetOnboarding}
                title="Reset Onboarding"
                style={{ color: "#1890ff" }}
              />
            )}
            <Button
              type="text"
              size="small"
              icon={isFullWidth ? <CompressOutlined /> : <ExpandOutlined />}
              onClick={toggleFullWidth}
              title={isFullWidth ? "Restore" : "Full Width"}
            />
            <Button
              type="text"
              size="small"
              icon={<MinusOutlined />}
              onClick={collapseDrawer}
              title="Collapse"
            />
            <Button
              type="text"
              size="small"
              icon={<CloseOutlined />}
              onClick={closeChatDrawer}
            />
          </Space>
        </div>
      </div>

      <ContextRulesPanel
        visible={isSettingsPanelVisible}
        onClose={handleSettingsPanelClose}
      />

      {TokenTopupDrawer && (
        <TokenTopupDrawer
          open={isTokenTopupDrawerVisible}
          onClose={handleTokenTopupDrawerClose}
          currentTokenBalance={tokenBalance?.current_balance || 0}
          selectedOrgId={selectedOrgId}
        />
      )}
    </>
  );
});

Header.propTypes = {
  resetSelectedChatId: PropTypes.func.isRequired,
  closeChatDrawer: PropTypes.func.isRequired,
  collapseDrawer: PropTypes.func.isRequired,
  toggleFullWidth: PropTypes.func.isRequired,
  isFullWidth: PropTypes.bool,
  isPromptRunning: PropTypes.bool.isRequired,
  // Onboarding props
  isOnboardingCompleted: PropTypes.bool,
  onResetOnboarding: PropTypes.func,
  // Token balance props
  tokenBalance: PropTypes.shape({
    current_balance: PropTypes.number,
    total_consumed: PropTypes.number,
    total_purchased: PropTypes.number,
    utilization_percentage: PropTypes.number,
  }),
  onBuyTokens: PropTypes.func,
};

Header.displayName = "Header";

export { Header };
