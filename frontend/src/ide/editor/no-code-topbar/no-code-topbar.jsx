import { memo, useCallback } from "react";
import PropTypes from "prop-types";
import { Space, Tag, Tooltip, Typography } from "antd";

import { NoCodeConfiguration } from "../no-code-configuration/no-code-configuration.jsx";
import {
  DbSource,
  DbDestination,
  DbJoin,
  RightArrow,
  Join,
  Tech,
} from "../../../base/icons";
import { truncateText } from "../../../common/helpers.js";
import { joinTableColors } from "../../../common/constants.js";
import "./no-code-topbar.css";
import { RefreshData } from "../no-code-toolbar/toolbar-items/refresh-data.jsx";

const NoCodeTopbar = memo(
  ({
    handleModalOpen,
    modalData,
    disabled,
    sourceTable,
    destinationTable,
    joinedTables,
    spec,
    updateSpec,
    joinSeq,
    refresh,
    refreshLoading,
  }) => {
    const openPopup = useCallback(
      (key) => {
        handleModalOpen(key);
      },
      [handleModalOpen]
    );

    const renderReferences = () => {
      if (!spec?.reference || spec?.reference?.length === 0) return null;

      return (
        <Space className="no-code-top-item" size={4}>
          <Tag
            className="no-code-topbar-chip"
            bordered={false}
            onClick={() => openPopup("sourceDestination")}
          >
            <div className="no-code-topbar-flex">
              <div className="center no-code-topbar-icon-size">
                <Tech />
              </div>
              <div className="no-code-topbar-config">
                {truncateText(spec.reference.join(", "))}
              </div>
            </div>
          </Tag>
          {spec.reference.length > 0 && "::"}
        </Space>
      );
    };

    const renderSourceDestination = () => {
      return (
        <Space className="no-code-top-item" size={4}>
          {sourceTable && (
            <Tooltip title={sourceTable}>
              <Tag
                className="no-code-topbar-chip"
                bordered={false}
                onClick={() => openPopup("sourceDestination")}
              >
                <div className="no-code-topbar-flex">
                  <div className="center no-code-topbar-icon-size">
                    <DbSource />
                  </div>
                  <div className="no-code-topbar-config">
                    {truncateText(sourceTable)}
                  </div>
                </div>
              </Tag>
            </Tooltip>
          )}
          {destinationTable && (
            <>
              <div className="center">
                <RightArrow />
              </div>
              <Tooltip title={destinationTable}>
                <Tag
                  className="no-code-topbar-chip"
                  bordered={false}
                  onClick={() => openPopup("sourceDestination")}
                >
                  <div className="no-code-topbar-flex">
                    <div className="center no-code-topbar-icon-size">
                      <DbDestination />
                    </div>
                    <div className="no-code-topbar-config">
                      {truncateText(destinationTable)}
                    </div>
                  </div>
                </Tag>
              </Tooltip>
            </>
          )}
        </Space>
      );
    };

    const renderJoinedTables = () => {
      if (!joinedTables || joinedTables.length === 0) return null;

      return (
        <Space className="no-code-top-item" size={4}>
          <Join />
          <Tooltip title={joinedTables.join(",")}>
            <Tag
              className="no-code-topbar-chip"
              bordered={false}
              onClick={() => openPopup("joins")}
            >
              <div className="no-code-topbar-flex">
                {joinSeq && (
                  <div className="seq_badge_wrap">
                    <div className="seq_badge">{joinSeq}</div>
                  </div>
                )}
                <div className="center no-code-topbar-icon-size">
                  <DbJoin />
                </div>
                <div className="no-code-topbar-config no-code-topbar-joinTables_list">
                  {joinedTables.map((el, index) => (
                    <Typography.Text
                      key={`${index}-${el}`}
                      style={{ color: joinTableColors[index] }}
                    >
                      |
                      <Typography.Text className="joinTables_item">
                        {truncateText(el, 40)}
                      </Typography.Text>
                    </Typography.Text>
                  ))}
                </div>
              </div>
            </Tag>
          </Tooltip>
        </Space>
      );
    };

    return (
      <div className="no-code-top-layout">
        <div className="no-code-top-left">
          <Space size={4} wrap={false}>
            {renderReferences()}
            {renderSourceDestination()}
            {renderJoinedTables()}
          </Space>
        </div>
        <div className="no-code-top-right">
          <NoCodeConfiguration
            modalData={modalData}
            handleModalOpen={handleModalOpen}
          />
          <RefreshData
            refresh={refresh}
            isLoading={refreshLoading}
            disabled={disabled}
          />
        </div>
      </div>
    );
  }
);

NoCodeTopbar.propTypes = {
  sourceTable: PropTypes.string,
  destinationTable: PropTypes.string,
  joinedTables: PropTypes.arrayOf(PropTypes.string),
  handleModalOpen: PropTypes.func.isRequired,
  modalData: PropTypes.object.isRequired,
  disabled: PropTypes.bool.isRequired,
  updateSpec: PropTypes.func,
  spec: PropTypes.shape({
    reference: PropTypes.arrayOf(PropTypes.string),
  }),
  joinSeq: PropTypes.number,
  refresh: PropTypes.func.isRequired,
  refreshLoading: PropTypes.bool.isRequired,
};

NoCodeTopbar.displayName = "NoCodeTopbar";

export { NoCodeTopbar };
