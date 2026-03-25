import { memo, useEffect, useRef, useState, useMemo, useCallback } from "react";
import PropTypes from "prop-types";
import { Space, Button, Typography, Input, Spin } from "antd";
import {
  ArrowDownOutlined,
  EditOutlined,
  CheckOutlined,
  CloseOutlined,
} from "@ant-design/icons";

import { useChatAIService } from "./services";
import { InputPrompt } from "./InputPrompt";
import { Conversation } from "./Conversation";
import { LowBalanceWarning } from "./LowBalanceWarning";
import { TodoGuide } from "./TodoGuide";
import { OnboardingGuide } from "./OnboardingGuide";
import { OnboardingCompletionPopup } from "./OnboardingCompletionPopup";
import { useNotificationService } from "../../service/notification-service";
import { useAxiosPrivate } from "../../service/axios-service";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { useSessionStore } from "../../store/session-store";

// Cloud-only: fetch per-message token usage (unavailable in OSS — import fails gracefully)
let getTokenUsage = null;
try {
  ({ getTokenUsage } = require("../../plugins/token-management/token-usage"));
} catch {
  // OSS: token usage API not available
}

const ExistingChat = memo(function ExistingChat({
  selectedChatId,
  chatName,
  setChatName,
  savePrompt,
  chatMessages,
  setChatMessages,
  isGetChatMessages,
  resetChatMessageIdentifier,
  isPromptRunning,
  chatIntents,
  selectedChatIntent,
  setSelectedChatIntent,
  llmModels,
  selectedLlmModel,
  setSelectedLlmModel,
  selectedCoderLlmModel,
  setSelectedCoderLlmModel,
  handleTransformApply,
  triggerRetryTransform,
  stopPromptRun,
  handleSqlRun,
  promptAutoComplete,
  isChatConversationDisabled,
  tokenUsageData,
  isTodoGuideVisible,
  onPromptSelect,
  selectedPrompt,
  completedTodoTasks,
  onCompletedTodoTasksChange,
  // Onboarding props
  isOnboardingMode,
  onboardingConfig,
  completedOnboardingSteps,
  skippedOnboardingSteps,
  currentOnboardingStep,
  isTypingPrompt,
  onOnboardingPromptSelect,
  onSkipOnboarding,
  onOnboardingComplete,
  onSkipCurrentTask,
  onSendButtonClick,
}) {
  const { getChatMessagesByChatId, updateChatName } = useChatAIService();
  const axiosPrivate = useAxiosPrivate();
  const isCloud = useSessionStore((state) => state.sessionDetails?.is_cloud);
  const chatContainerRef = useRef(null);

  const [isLoadingChats, setIsLoadingChats] = useState(false);
  const [lastChatMessageId, setLastChatMessageId] = useState(null);
  const [isAtBottom, setIsAtBottom] = useState(true);
  const [showCompletionPopup, setShowCompletionPopup] = useState(false);
  const [hasShownCompletion, setHasShownCompletion] = useState(false);

  // Title bar inline edit state
  const [isEditingTitle, setIsEditingTitle] = useState(false);
  const [editTitleValue, setEditTitleValue] = useState("");
  const [isSavingTitle, setIsSavingTitle] = useState(false);

  const { notify } = useNotificationService();

  const ellipsisConfig = useMemo(() => ({ tooltip: chatName }), [chatName]);

  // Check if onboarding is complete (progress 100%)
  useEffect(() => {
    // Don't check if no config
    if (!onboardingConfig) {
      return;
    }

    // Check multiple conditions for completion
    const isComplete =
      onboardingConfig?.progress?.progress_percentage === 100 ||
      (onboardingConfig?.progress?.completed_tasks ===
        onboardingConfig?.progress?.total_tasks &&
        onboardingConfig?.progress?.total_tasks > 0);

    // Show popup if tasks are 100% complete AND not already marked as completed in database
    // Don't require isOnboardingMode because tasks might complete while in existing chat
    if (
      onboardingConfig &&
      isComplete &&
      !onboardingConfig.is_completed &&
      !showCompletionPopup &&
      !hasShownCompletion
    ) {
      setShowCompletionPopup(true);
    } else if (onboardingConfig?.is_completed) {
      // Make sure popup is hidden if already completed
      if (showCompletionPopup) {
        setShowCompletionPopup(false);
      }
      // Reset the flag for future use
      if (hasShownCompletion) {
        setHasShownCompletion(false);
      }
    }
  }, [onboardingConfig, showCompletionPopup, isOnboardingMode, hasShownCompletion]);

  // Handle prompt selection from TodoGuide
  const handlePromptSelect = useCallback(
    (prompt) => {
      if (onPromptSelect) {
        onPromptSelect(prompt);
      }
    },
    [onPromptSelect]
  );

  // Title bar handlers
  const handleStartEditTitle = useCallback(() => {
    setIsEditingTitle(true);
    setEditTitleValue(chatName || "");
  }, [chatName]);

  const handleCancelEditTitle = useCallback(() => {
    setIsEditingTitle(false);
    setEditTitleValue("");
  }, []);

  const handleSaveTitle = useCallback(async () => {
    const trimmed = editTitleValue.trim();
    if (!trimmed || trimmed === chatName) {
      setIsEditingTitle(false);
      return;
    }

    setIsSavingTitle(true);
    try {
      await updateChatName(selectedChatId, trimmed);
      setChatName(trimmed);
      setIsEditingTitle(false);
      setEditTitleValue("");
    } catch (error) {
      console.error("Failed to update chat name:", error);
      notify({ error });
    } finally {
      setIsSavingTitle(false);
    }
  }, [
    editTitleValue,
    chatName,
    selectedChatId,
    updateChatName,
    setChatName,
    notify,
  ]);

  const handleTitleKeyDown = useCallback(
    (e) => {
      if (e.key === "Enter") {
        handleSaveTitle();
      } else if (e.key === "Escape") {
        handleCancelEditTitle();
      }
    },
    [handleSaveTitle, handleCancelEditTitle]
  );

  // Handle completion popup close
  const handleCompletionPopupClose = useCallback(() => {
    setShowCompletionPopup(false);
  }, []);

  // Handle completion popup continue
  const handleCompletionPopupContinue = useCallback(() => {
    // Immediately close popup to prevent flickering
    setShowCompletionPopup(false);

    // Set a flag to prevent popup from showing again during API call
    setHasShownCompletion(true);

    // Call the onOnboardingComplete handler if available
    if (onOnboardingComplete) {
      onOnboardingComplete();
    } else {
      // Fallback to skip if no complete handler
      if (onSkipOnboarding) {
        onSkipOnboarding();
      }
    }
  }, [onOnboardingComplete, onSkipOnboarding]);

  // Use original savePrompt directly since selectedPrompt is now managed by parent
  const handleSavePrompt = useCallback(
    (...args) => {
      savePrompt(...args); // Call the original savePrompt
    },
    [savePrompt]
  );

  // runs on every scroll via onScroll
  const handleScroll = useCallback(() => {
    if (!chatContainerRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = chatContainerRef.current;
    const nearBottom = scrollHeight - (scrollTop + clientHeight) < 50;
    setIsAtBottom(nearBottom);
  }, []);

  const scrollToBottom = () => {
    if (chatContainerRef.current) {
      chatContainerRef.current.scrollTop =
        chatContainerRef.current.scrollHeight;
      setIsAtBottom(true);
    }
  };

  const lastTransformIndex = useMemo(() => {
    const intentsMap = chatIntents.reduce((acc, ci) => {
      acc[ci?.chat_intent_id] = ci?.name;
      return acc;
    }, {});
    for (let i = chatMessages.length - 1; i >= 0; i--) {
      if (intentsMap[chatMessages[i]?.chat_intent] === "TRANSFORM") return i;
    }
    return -1;
  }, [chatMessages, chatIntents]);

  useEffect(() => {
    if (chatMessages.length) {
      setLastChatMessageId(
        chatMessages[chatMessages.length - 1]?.chat_message_id
      );
    }
  }, [chatMessages]);

  // cleanup on unmount
  useEffect(() => () => setChatMessages([]), [setChatMessages]);

  // fetch messages on chat change / refresh
  useEffect(() => {
    if (isGetChatMessages && selectedChatId) fetchChatMessages();
  }, [isGetChatMessages, selectedChatId]);

  const fetchChatMessages = async () => {
    try {
      setIsLoadingChats(true);
      const apiData = await getChatMessagesByChatId(selectedChatId);
      const updatedData = (apiData || []).map((msg) => ({
        ...msg,
        response: Array.isArray(msg.response)
          ? msg.response
          : [msg.response].filter(Boolean),
      }));

      // Fetch token usage for all messages to display in historical conversations.
      // Only available in cloud mode via the token-usage plugin.
      if (getTokenUsage && isCloud && updatedData.length > 0) {
        const tokenUsagePromises = updatedData.map((msg) =>
          getTokenUsage(
            axiosPrivate,
            selectedChatId,
            msg.chat_message_id
          ).catch(() => null)
        );
        const tokenUsageResults = await Promise.all(tokenUsagePromises);

        tokenUsageResults.forEach((tokenUsage, index) => {
          if (tokenUsage) {
            updatedData[index] = {
              ...updatedData[index],
              token_usage_data: tokenUsage,
            };
          }
        });
      }

      setChatMessages(updatedData);
      setIsAtBottom(true);
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setIsLoadingChats(false);
      resetChatMessageIdentifier();
    }
  };

  // auto-scroll when new messages arrive, but only if user is already at bottom
  useEffect(() => {
    if (isAtBottom) scrollToBottom();
  }, [chatMessages]);

  if (!selectedChatId) return null;

  if (isLoadingChats) {
    return <SpinnerLoader />;
  }

  // Position button relative to container - always above the prompt area
  const scrollToBottomStyle = {
    position: "absolute",
    bottom: 180,
    right: 24,
    zIndex: 100,
    boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
  };

  return (
    <div className="existing-chat-outer-container">
      {chatName && (
        <div className="chat-title-bar">
          {isEditingTitle ? (
            <div className="chat-title-edit-row">
              <Input
                value={editTitleValue}
                onChange={(e) => setEditTitleValue(e.target.value)}
                onKeyDown={handleTitleKeyDown}
                onClick={(e) => e.stopPropagation()}
                className="chat-title-edit-input"
                autoFocus
                maxLength={100}
                size="small"
              />
              <Space size={4}>
                {isSavingTitle ? (
                  <Spin size="small" />
                ) : (
                  <CheckOutlined
                    onClick={handleSaveTitle}
                    className="chat-title-action-icon"
                  />
                )}
                <CloseOutlined
                  onClick={isSavingTitle ? undefined : handleCancelEditTitle}
                  className="chat-title-action-icon"
                  style={
                    isSavingTitle
                      ? { pointerEvents: "none", opacity: 0.5 }
                      : undefined
                  }
                />
              </Space>
            </div>
          ) : (
            <div className="chat-title-display-row">
              <Typography.Text
                ellipsis={ellipsisConfig}
                className="chat-title-text"
              >
                {chatName}
              </Typography.Text>
              <EditOutlined
                onClick={handleStartEditTitle}
                className="chat-title-edit-icon"
              />
            </div>
          )}
        </div>
      )}
      <div
        ref={chatContainerRef}
        className="chat-ai-existing-chat-container"
        onScroll={handleScroll}
      >
        <Space direction="vertical" className="width-100">
          {chatMessages.map((message, idx) => (
            <Conversation
              key={message.chat_message_id}
              message={message}
              chatIntents={chatIntents}
              llmModels={llmModels}
              isPromptRunning={isPromptRunning}
              isLastConversation={idx === chatMessages.length - 1}
              selectedChatId={selectedChatId}
              handleTransformApply={handleTransformApply}
              triggerRetryTransform={triggerRetryTransform}
              handleSqlRun={handleSqlRun}
              isLatestTransform={idx === lastTransformIndex}
              savePrompt={handleSavePrompt}
              selectedChatIntent={selectedChatIntent}
            />
          ))}
        </Space>
      </div>

      {!isAtBottom && (
        <Button
          shape="round"
          onClick={scrollToBottom}
          icon={<ArrowDownOutlined />}
          style={scrollToBottomStyle}
        />
      )}

      <div className="pad-8">
        <LowBalanceWarning tokenUsageData={tokenUsageData} />

        <div style={{ marginTop: "40px" }}>
          <OnboardingGuide
            visible={isOnboardingMode}
            config={onboardingConfig}
            onPromptSelect={onOnboardingPromptSelect}
            completedSteps={completedOnboardingSteps}
            skippedSteps={skippedOnboardingSteps}
            currentStep={
              onboardingConfig?.progress?.completed_tasks ||
              (completedOnboardingSteps ? completedOnboardingSteps.size : 0)
            }
            totalSteps={onboardingConfig ? onboardingConfig.totalSteps : 4}
            onSkip={onSkipOnboarding}
            onComplete={onOnboardingComplete}
            onSkipCurrentTask={onSkipCurrentTask}
            currentOnboardingStep={currentOnboardingStep}
            showWelcome={false}
            hideModeTag={true}
            collapsibleTodos={true}
          />
        </div>

        <TodoGuide
          visible={isTodoGuideVisible && !isOnboardingMode}
          onPromptSelect={handlePromptSelect}
          completedTasks={completedTodoTasks}
          onCompletedTasksChange={onCompletedTodoTasksChange}
        />

        {/* Onboarding Completion Popup */}
        <OnboardingCompletionPopup
          visible={showCompletionPopup}
          onClose={handleCompletionPopupClose}
          onContinue={handleCompletionPopupContinue}
        />

        <InputPrompt
          savePrompt={handleSavePrompt}
          isPromptRunning={isPromptRunning}
          chatIntents={chatIntents}
          selectedChatIntent={selectedChatIntent}
          setSelectedChatIntent={setSelectedChatIntent}
          llmModels={llmModels}
          selectedLlmModel={selectedLlmModel}
          setSelectedLlmModel={setSelectedLlmModel}
          selectedCoderLlmModel={selectedCoderLlmModel}
          setSelectedCoderLlmModel={setSelectedCoderLlmModel}
          stopPromptRun={stopPromptRun}
          selectedChatId={selectedChatId}
          lastChatMessageId={lastChatMessageId}
          promptAutoComplete={promptAutoComplete}
          isChatConversationDisabled={isChatConversationDisabled}
          prefilledPrompt={selectedPrompt}
          shouldHighlightSend={
            !!(isOnboardingMode && !isTypingPrompt && selectedPrompt)
          }
          isOnboardingMode={isOnboardingMode}
          isTypingPrompt={isTypingPrompt}
          disableSendDuringTyping={isOnboardingMode && isTypingPrompt}
          onSendButtonClick={onSendButtonClick}
        />
      </div>
    </div>
  );
});

ExistingChat.propTypes = {
  selectedChatId: PropTypes.string,
  chatName: PropTypes.string,
  setChatName: PropTypes.func.isRequired,
  savePrompt: PropTypes.func.isRequired,
  chatMessages: PropTypes.array,
  setChatMessages: PropTypes.func.isRequired,
  isGetChatMessages: PropTypes.bool.isRequired,
  resetChatMessageIdentifier: PropTypes.func.isRequired,
  isPromptRunning: PropTypes.bool.isRequired,
  chatIntents: PropTypes.array.isRequired,
  selectedChatIntent: PropTypes.string,
  setSelectedChatIntent: PropTypes.func.isRequired,
  llmModels: PropTypes.array,
  selectedLlmModel: PropTypes.string,
  setSelectedLlmModel: PropTypes.func.isRequired,
  selectedCoderLlmModel: PropTypes.string,
  setSelectedCoderLlmModel: PropTypes.func.isRequired,
  handleTransformApply: PropTypes.func.isRequired,
  triggerRetryTransform: PropTypes.bool.isRequired,
  stopPromptRun: PropTypes.func.isRequired,
  handleSqlRun: PropTypes.func.isRequired,
  promptAutoComplete: PropTypes.object,
  isChatConversationDisabled: PropTypes.bool.isRequired,
  tokenUsageData: PropTypes.object,
  isTodoGuideVisible: PropTypes.bool,
  onPromptSelect: PropTypes.func,
  selectedPrompt: PropTypes.string,
  completedTodoTasks: PropTypes.instanceOf(Set),
  onCompletedTodoTasksChange: PropTypes.func,
  // Onboarding props
  isOnboardingMode: PropTypes.bool,
  onboardingConfig: PropTypes.object,
  completedOnboardingSteps: PropTypes.instanceOf(Set),
  skippedOnboardingSteps: PropTypes.instanceOf(Set),
  currentOnboardingStep: PropTypes.object,
  isTypingPrompt: PropTypes.bool,
  onOnboardingPromptSelect: PropTypes.func,
  onSkipOnboarding: PropTypes.func,
  onOnboardingComplete: PropTypes.func,
  onSkipCurrentTask: PropTypes.func,
  onSendButtonClick: PropTypes.func,
};

ExistingChat.displayName = "ExistingChat";

export { ExistingChat };
