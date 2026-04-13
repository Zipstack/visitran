import { memo } from "react";
import PropTypes from "prop-types";
import { Button, Typography } from "antd";
import { CloseOutlined, PlusCircleOutlined, SyncOutlined } from "@ant-design/icons";

import { useProjectStore } from "../../store/project-store";
import { useVersionHistoryStore } from "../../store/version-history-store";
import { getActiveModelName } from "../../common/helpers";

const Header = memo(function Header({ closeDrawer, onSync }) {
  const { projectId, projectDetails } = useProjectStore.getState();
  const modelName = getActiveModelName(projectId, projectDetails);
  const openCommitModal = useVersionHistoryStore(
    (state) => state.openCommitModal
  );

  return (
    <div className="flex-space-between chat-ai-header">
      <div>
        <Typography.Text strong style={{ fontWeight: "bold" }}>
          [Version History]
        </Typography.Text>
        {modelName && <Typography.Text italic> {modelName}</Typography.Text>}
      </div>
      <div>
        <Button
          type="text"
          size="small"
          icon={<PlusCircleOutlined />}
          onClick={openCommitModal}
          className="commit-btn-header"
        >
          Commit
        </Button>
        <Button
          type="text"
          size="small"
          icon={<SyncOutlined />}
          onClick={onSync}
          title="Sync version history"
        />
        <Button
          type="text"
          size="small"
          icon={<CloseOutlined />}
          onClick={closeDrawer}
        />
      </div>
    </div>
  );
});

Header.propTypes = {
  closeDrawer: PropTypes.func.isRequired,
  onSync: PropTypes.func,
};

Header.displayName = "VersionHistoryHeader";

export { Header };
