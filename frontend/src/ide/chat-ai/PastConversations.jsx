import { memo, useState, useEffect, useCallback } from "react";
import PropTypes from "prop-types";
import { Space, Typography, Tag } from "antd";

import { PastConversationActions } from "./PastConversationActions";
import { useChatAIService } from "./services";
import { useProjectStore } from "../../store/project-store";
import { formatTimeAgo } from "./helper";
import { useNotificationService } from "../../service/notification-service";

const PastConversations = memo(function PastConversations({
  isChatDrawerOpen,
  setSelectedChatId,
  setChatName,
  triggerGetChatMessagesApi,
  setSelectedLlmModel,
  setSelectedCoderLlmModel,
}) {
  const [pastConversations, setPastConversations] = useState([]);
  const [showAll, setShowAll] = useState(false);
  const [hoveredConversation, setHoveredConversation] = useState(null);

  const { projectId } = useProjectStore();
  const { getAllChats, deleteChatById, updateChatName } = useChatAIService();
  const { notify } = useNotificationService();

  useEffect(() => {
    const fetchChats = async () => {
      try {
        if (!projectId || !isChatDrawerOpen) return;
        const data = await getAllChats();
        setPastConversations(data);
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    };
    fetchChats();
  }, [projectId, isChatDrawerOpen]);

  const handleShowAll = useCallback(() => {
    setShowAll((prev) => !prev);
  }, []);

  const handleDelete = useCallback(
    async (conversationId) => {
      try {
        await deleteChatById(conversationId);
        setPastConversations((prev) =>
          prev.filter((c) => c.chat_id !== conversationId)
        );
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    },
    [deleteChatById, notify]
  );

  const handleUpdate = useCallback(
    async (chatId, newName) => {
      try {
        await updateChatName(chatId, newName);
        setPastConversations((prev) =>
          prev.map((c) =>
            c.chat_id === chatId ? { ...c, chat_name: newName } : c
          )
        );
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    },
    [updateChatName, notify]
  );

  const handleSelectChat = useCallback(
    (conversation) => {
      triggerGetChatMessagesApi();
      setSelectedChatId(conversation.chat_id);
      setChatName(conversation.chat_name || "");
      setSelectedLlmModel(conversation.llm_model_architect);
      setSelectedCoderLlmModel(conversation.llm_model_developer);
    },
    [
      triggerGetChatMessagesApi,
      setSelectedChatId,
      setChatName,
      setSelectedLlmModel,
      setSelectedCoderLlmModel,
    ]
  );

  const conversationsToRender = showAll
    ? pastConversations
    : pastConversations.slice(0, 3);

  return (
    <Space direction="vertical" className="width-100" size={5}>
      {pastConversations?.length > 0 && (
        <>
          <Typography.Text strong className="chat-ai-welcome-title">
            Past Conversations
          </Typography.Text>
          <Space direction="vertical" className="width-100" size={5}>
            {conversationsToRender.map((conversation) => {
              const isHovered = hoveredConversation === conversation.chat_id;
              return (
                <div
                  key={conversation.chat_id}
                  className="width-100 cursor-pointer chat-ai-past-conversations-card flex-space-between"
                  onMouseEnter={() =>
                    setHoveredConversation(conversation.chat_id)
                  }
                  onMouseLeave={() => setHoveredConversation(null)}
                  onClick={() => handleSelectChat(conversation)}
                >
                  <div className="pad-h-10 chat-ai-past-conversations-name">
                    <div className="past-conversations-chat-info">
                      <Typography.Text
                        type="secondary"
                        ellipsis={{ tooltip: conversation.chat_name }}
                      >
                        {conversation.chat_name}
                      </Typography.Text>
                      {conversation?.chat_intent_name && (
                        <Tag className="past-conversations-intent-chip">
                          {conversation.chat_intent_name}
                        </Tag>
                      )}
                    </div>
                  </div>
                  <div className="pad-h-10 align-items-center">
                    <PastConversationActions
                      isHovered={isHovered}
                      conversation={conversation}
                      formatTimeAgo={formatTimeAgo}
                      handleDelete={handleDelete}
                      handleUpdate={handleUpdate}
                    />
                  </div>
                </div>
              );
            })}
            {pastConversations.length > 3 && (
              <div className="pad-10-top">
                <Typography.Text
                  type="secondary"
                  className="font-size-12 cursor-pointer"
                  onClick={handleShowAll}
                >
                  {showAll
                    ? "Show less"
                    : `Show ${pastConversations.length - 3} more`}
                </Typography.Text>
              </div>
            )}
          </Space>
        </>
      )}
    </Space>
  );
});

PastConversations.propTypes = {
  isChatDrawerOpen: PropTypes.bool.isRequired,
  setSelectedChatId: PropTypes.func.isRequired,
  setChatName: PropTypes.func.isRequired,
  triggerGetChatMessagesApi: PropTypes.func.isRequired,
  setSelectedLlmModel: PropTypes.func.isRequired,
  setSelectedCoderLlmModel: PropTypes.func.isRequired,
};

PastConversations.displayName = "PastConversations";

export { PastConversations };
