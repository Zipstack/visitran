import { memo, useMemo, useCallback, useState } from "react";
import PropTypes from "prop-types";
import { Button, Modal, Space, Table } from "antd";
import { ArrowsAltOutlined } from "@ant-design/icons";

const SqlRun = memo(function SqlRun({
  message,
  selectedChatId,
  handleSqlRun,
  uiAction,
}) {
  // Render guard: only show when uiAction indicates SQL button should be shown
  if (!uiAction?.show_button || uiAction?.button_type !== "run_sql")
    return null;

  const [modalOpen, setModalOpen] = useState(false);

  const isLoading = message?.transformation_status === "RUNNING";

  const colNames = message?.query_result?.columns ?? [];
  const rawRows = message?.query_result?.rows ?? [];

  const columns = useMemo(
    () =>
      colNames.map((name) => ({
        title: name,
        dataIndex: name,
        key: name,
      })),
    [colNames]
  );

  const dataSource = useMemo(
    () =>
      rawRows.map((row, idx) =>
        row.reduce(
          (acc, cell, cellIdx) => ({
            ...acc,
            key: idx,
            [colNames[cellIdx]]: cell,
          }),
          {}
        )
      ),
    [rawRows, colNames]
  );

  const hasData = columns.length > 0 && dataSource.length > 0;

  const onSqlRun = useCallback(() => {
    handleSqlRun({
      chatId: selectedChatId,
      chatMessageId: message?.chat_message_id,
    });
  }, [handleSqlRun, selectedChatId, message?.chat_message_id]);

  const tableProps = {
    columns,
    dataSource,
    scroll: { x: "max-content" },
    loading: isLoading,
    rowKey: "key",
  };

  return (
    <Space direction="vertical" className="width-100">
      <Space className="flex-space-between">
        <Button
          type="primary"
          size="small"
          onClick={onSqlRun}
          loading={isLoading}
        >
          Run
        </Button>
        {hasData && (
          <Button
            type="text"
            size="small"
            icon={<ArrowsAltOutlined />}
            onClick={() => setModalOpen(true)}
          />
        )}
      </Space>

      {hasData && (
        <>
          <div className="chat-ai-sql-run-table-wrapper">
            <Table
              {...tableProps}
              pagination={{ pageSize: 5, showSizeChanger: false }}
              size="small"
            />
          </div>

          <Modal
            open={modalOpen}
            onCancel={() => setModalOpen(false)}
            maskClosable={false}
            centered
            footer={null}
            width={1500}
          >
            <div className="chat-ai-sql-run-table-wrapper">
              <Table
                {...tableProps}
                pagination={{ pageSize: 10, showSizeChanger: false }}
              />
            </div>
          </Modal>
        </>
      )}
    </Space>
  );
});

SqlRun.propTypes = {
  message: PropTypes.object.isRequired,
  selectedChatId: PropTypes.string.isRequired,
  handleSqlRun: PropTypes.func.isRequired,
  uiAction: PropTypes.object,
};

SqlRun.displayName = "SqlRun";

export { SqlRun };
