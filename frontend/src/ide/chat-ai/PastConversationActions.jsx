import { memo, useState, useCallback } from "react";
import PropTypes from "prop-types";
import { Space, Typography, Input, Spin } from "antd";
import {
  CheckOutlined,
  CloseOutlined,
  DeleteOutlined,
  EditOutlined,
} from "@ant-design/icons";

const PastConversationActions = memo(function PastConversationActions({
  isHovered,
  conversation,
  formatTimeAgo,
  handleDelete,
  handleUpdate,
}) {
  const [confirmDeleteId, setConfirmDeleteId] = useState(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const [editValue, setEditValue] = useState("");
  const [isSaving, setIsSaving] = useState(false);

  const handleConfirmDelete = useCallback((conversationId) => {
    setConfirmDeleteId(conversationId);
  }, []);

  const clearConfirmDeleteId = useCallback((e) => {
    e.stopPropagation();
    setConfirmDeleteId(null);
  }, []);

  const handleStartEdit = useCallback((e, conversationId, currentName) => {
    e.stopPropagation();
    setEditingId(conversationId);
    setEditValue(currentName);
  }, []);

  const handleCancelEdit = useCallback((e) => {
    e.stopPropagation();
    setEditingId(null);
    setEditValue("");
  }, []);

  const handleSaveEdit = useCallback(
    async (e, conversationId) => {
      e.stopPropagation();

      if (!editValue.trim()) {
        return;
      }

      setIsSaving(true);
      try {
        await handleUpdate(conversationId, editValue.trim());
        setEditingId(null);
        setEditValue("");
      } catch (error) {
        console.error("Failed to update chat name:", error);
      } finally {
        setIsSaving(false);
      }
    },
    [editValue, handleUpdate]
  );

  const handleInputChange = useCallback((e) => {
    setEditValue(e.target.value);
  }, []);

  const handleInputKeyDown = useCallback(
    (e, conversationId) => {
      if (e.key === "Enter") {
        handleSaveEdit(e, conversationId);
      } else if (e.key === "Escape") {
        handleCancelEdit(e);
      }
    },
    [handleSaveEdit, handleCancelEdit]
  );

  // Delete confirmation mode
  if (confirmDeleteId) {
    return (
      <Space>
        {isDeleting ? (
          <Spin size="small" />
        ) : (
          <>
            <CheckOutlined
              onClick={async (e) => {
                e.stopPropagation();
                setIsDeleting(true);
                try {
                  await handleDelete(confirmDeleteId);
                } finally {
                  setIsDeleting(false);
                  setConfirmDeleteId(null);
                }
              }}
              className="cursor-pointer past-conversation-action-icon"
            />
            <CloseOutlined
              onClick={clearConfirmDeleteId}
              className="cursor-pointer past-conversation-action-icon"
            />
          </>
        )}
      </Space>
    );
  }

  // Edit mode
  if (editingId === conversation?.chat_id) {
    return (
      <Space className="past-conversation-edit-mode">
        <Input
          value={editValue}
          onChange={handleInputChange}
          onKeyDown={(e) => handleInputKeyDown(e, conversation.chat_id)}
          onClick={(e) => e.stopPropagation()}
          className="past-conversation-edit-input"
          autoFocus
          maxLength={100}
        />
        <Space>
          {isSaving ? (
            <Spin size="small" />
          ) : (
            <CheckOutlined
              onClick={(e) => handleSaveEdit(e, conversation.chat_id)}
              className="cursor-pointer past-conversation-action-icon"
            />
          )}
          <CloseOutlined
            onClick={handleCancelEdit}
            className="cursor-pointer past-conversation-action-icon"
          />
        </Space>
      </Space>
    );
  }

  // Hover mode - show edit and delete icons
  if (isHovered) {
    return (
      <Space>
        <EditOutlined
          onClick={(e) =>
            handleStartEdit(e, conversation?.chat_id, conversation?.chat_name)
          }
          className="cursor-pointer past-conversation-action-icon"
        />
        <DeleteOutlined
          onClick={(e) => {
            e.stopPropagation();
            handleConfirmDelete(conversation?.chat_id);
          }}
          className="cursor-pointer past-conversation-action-icon"
        />
      </Space>
    );
  }

  // Default mode - show timestamp
  return (
    <Typography.Text type="secondary">
      {formatTimeAgo(conversation?.modified_at)}
    </Typography.Text>
  );
});

PastConversationActions.propTypes = {
  isHovered: PropTypes.bool.isRequired,
  conversation: PropTypes.shape({
    chat_id: PropTypes.string,
    chat_name: PropTypes.string,
    modified_at: PropTypes.string,
  }).isRequired,
  formatTimeAgo: PropTypes.func.isRequired,
  handleDelete: PropTypes.func.isRequired,
  handleUpdate: PropTypes.func.isRequired,
};

PastConversationActions.displayName = "PastConversationActions";

export { PastConversationActions };
