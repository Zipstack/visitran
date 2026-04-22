import { useState, useCallback, useEffect, useRef } from "react";
import PropTypes from "prop-types";

import { useSocketService } from "../../service/socket-service";
import { ExistingChat } from "./ExistingChat";
import { NewChat } from "./NewChat";
import { useChatAIService } from "./services";
import { useProjectStore } from "../../store/project-store";
import { useExplorerStore } from "../../store/explorer-store";
import { useNotificationService } from "../../service/notification-service";
import { explorerService } from "../explorer/explorer-service";

// Cloud-only: fetch token balance (unavailable in OSS — import fails gracefully)
let getTokenBalance = null;
try {
  ({
    getTokenBalance,
  } = require("../../plugins/token-management/token-balance"));
} catch {
  // OSS: token balance not available
}

const Body = function Body({
  isChatDrawerOpen,
  selectedChatId,
  setSelectedChatId,
  chatName,
  setChatName,
  chatMessages,
  setChatMessages,
  isPromptRunning,
  triggerRetryTransform,
  isChatConversationDisabled,
  tokenUsageData,
  onPromptSelect,
  selectedPrompt,
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
  const [isGetChatMessages, setIsGetChatMessages] = useState(false);
  const [chatIntents, setChatIntents] = useState([]);
  const [selectedChatIntent, setSelectedChatIntent] = useState(null);
  const [llmModels, setLlmModels] = useState([]);
  const [selectedLlmModel, setSelectedLlmModel] = useState(null);
  const [selectedCoderLlmModel, setSelectedCoderLlmModel] = useState(null);
  const [realTokenBalance, setRealTokenBalance] = useState(null);
  const [hasInsufficientTokens, setHasInsufficientTokens] = useState(false);
  const [insufficientTokensMessage, setInsufficientTokensMessage] =
    useState("");
  const {
    connectSocket,
    createChannel,
    disconnectSocket,
    handleTransformApply,
    stopPromptRun,
    handleSqlRun,
    isConnected,
  } = useSocketService();
  const [pendingChannel, setPendingChannel] = useState(null);
  const [promptAutoComplete, setPromptAutoComplete] = useState({
    modelsData: {},
    seedsData: {},
    dbData: {},
  });
  const explorerSvc = useRef(explorerService()).current;
  const { projectId } = useProjectStore();
  const explorerData = useExplorerStore((state) => state.explorerData);

  const { postChatPrompt, getChatIntents, getChatLlmModels } =
    useChatAIService();
  const { notify } = useNotificationService();

  useEffect(() => {
    if (!selectedChatId && isConnected) {
      disconnectSocket();
    }

    if (selectedChatId) {
      connectSocket(selectedChatId);
    }

    return () => {
      disconnectSocket();
    };
  }, [selectedChatId]);

  useEffect(() => {
    if (pendingChannel && isConnected) {
      createChannel(pendingChannel?.chatId, pendingChannel?.chatMessageId);
      setPendingChannel(null);
    }
  }, [pendingChannel, isConnected, createChannel]);

  useEffect(() => {
    if (!projectId || chatIntents?.length > 0 || !isChatDrawerOpen) return;

    getChatIntents()
      .then((data) => {
        setChatIntents(data);
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      });
  }, [projectId, isChatDrawerOpen]);

  useEffect(() => {
    if (!projectId || llmModels?.length > 0 || !isChatDrawerOpen) return;

    getChatLlmModels()
      .then((data) => {
        setLlmModels(data);
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      });
  }, [projectId, isChatDrawerOpen]);

  // Fetch real credit balance when no chat messages exist (Cloud only)
  // In OSS mode, the /token-balance/ endpoint doesn't exist — skip to avoid false warnings.
  // OSS gets token data from WebSocket prompt responses (token_usage_data in "done" message).
  useEffect(() => {
    if (
      !getTokenBalance ||
      selectedChatId ||
      chatMessages.length > 0 ||
      realTokenBalance
    ) {
      return;
    }

    getTokenBalance()
      .then((tokenBalance) => {
        const balance = tokenBalance.current_balance || 0;
        const totalConsumed = tokenBalance.total_consumed || 0;
        const totalPurchased = tokenBalance.total_purchased || 0;

        setRealTokenBalance({
          remaining_balance: balance,
          total_consumed: totalConsumed,
          message_tokens_consumed: 0,
          token_usage_found: false,
        });

        // Proactive credit balance warnings
        if (balance <= 0 && totalConsumed > 0) {
          setHasInsufficientTokens(true);
          setInsufficientTokensMessage(
            "You've run out of tokens! AI chat is temporarily unavailable. Top up your account to continue."
          );

          notify({
            type: "error",
            message: "Out of Tokens",
            description:
              "AI features are temporarily unavailable because your token balance has been used up.\n\n" +
              "Please visit [subscription page](/project/setting/subscriptions) to top up your tokens and continue using Visitran AI.\n\n" +
              "Your conversations are saved and will be here when you return!",
            renderMarkdown: true,
          });
        } else if (totalPurchased > 0) {
          const percentageRemaining = (balance / totalPurchased) * 100;

          if (percentageRemaining <= 10 && balance > 0) {
            notify({
              type: "warning",
              message: "Almost Out of Tokens",
              description:
                `**Only ${Math.round(balance)} tokens remaining** (${Math.round(
                  percentageRemaining
                )}% left)\n\n` +
                "You're running critically low! Top up soon to keep your AI conversations going.",
              renderMarkdown: true,
              duration: 10,
            });
          } else if (percentageRemaining <= 25 && balance > 0) {
            notify({
              type: "warning",
              message: "Time to Top Up Soon",
              description:
                `You have **${Math.round(balance)} tokens left** (${Math.round(
                  percentageRemaining
                )}% of your balance)\n\n` +
                "Consider adding more tokens to avoid running out during important conversations.",
              renderMarkdown: true,
              duration: 8,
            });
          }
        }
      })
      .catch(() => {
        setRealTokenBalance(null);
      });
  }, [selectedChatId, chatMessages.length, realTokenBalance, notify]);

  useEffect(() => {
    if (!projectId || !isChatDrawerOpen) return;

    // fetch database schemas -> update immediately when ready
    explorerSvc
      .getDbExplorer(projectId)
      .then((res) => {
        setPromptAutoComplete((prev) => ({
          ...prev,
          dbData: res?.data || {},
        }));
      })
      .catch(() => {
        console.error("Failed to fetch database schemas");
      });
  }, [projectId, isChatDrawerOpen, explorerSvc]);

  // Mirror shared explorer data (fetched by explorer-component) into the
  // prompt autocomplete shape consumed by NewChat / InputPrompt.
  useEffect(() => {
    const children = explorerData || [];
    setPromptAutoComplete((prev) => ({
      ...prev,
      modelsData: children[0] || {},
      seedsData: children[1] || {},
    }));
  }, [explorerData]);

  const triggerGetChatMessagesApi = useCallback(() => {
    setIsGetChatMessages(true);
  }, []);

  const resetChatMessageIdentifier = useCallback(() => {
    setIsGetChatMessages(false);
  }, []);

  // Auto-select intent based on onboarding step mode
  useEffect(() => {
    if (!isOnboardingMode || !currentOnboardingStep || !chatIntents.length)
      return;

    const step = currentOnboardingStep;
    let targetIntentName;

    // Map onboarding mode to intent name
    switch (step.mode) {
      case "transform":
        targetIntentName = "TRANSFORM";
        break;
      case "sql":
        targetIntentName = "SQL";
        break;
      case "chat":
        targetIntentName = "INFO";
        break;
      default:
        return;
    }

    // Find the intent with the matching name
    const targetIntent = chatIntents.find(
      (intent) => intent?.name === targetIntentName
    );

    if (targetIntent && selectedChatIntent !== targetIntent.chat_intent_id) {
      setSelectedChatIntent(targetIntent.chat_intent_id);

      // Add a visual animation hint
      setTimeout(() => {
        // You could add a toast notification here if needed
        // notify({ message: `Switched to ${targetIntentName} mode for this step` });
      }, 100);
    }
  }, [
    isOnboardingMode,
    currentOnboardingStep,
    chatIntents,
    selectedChatIntent,
    setSelectedChatIntent,
  ]);

  const savePrompt = useCallback(
    (
      prompt,
      selectedChatIntent,
      isNewChat = false,
      discussionStatus = null,
      chatMessageId = null
    ) => {
      postChatPrompt({
        prompt,
        llm_model_architect: selectedLlmModel,
        llm_model_developer: selectedCoderLlmModel,
        chatId: selectedChatId,
        chatIntentId: selectedChatIntent,
        discussionStatus,
        chatMessageId,
      })
        .then((data) => {
          const newChatId = data?.chat;
          const newChatMessageId = data?.chat_message_id;

          // Always sync chat name from API response
          if (data?.chat_name) setChatName(data.chat_name);

          if (isNewChat) {
            setSelectedChatId(newChatId);
            setChatMessages([data]);
          } else {
            setChatMessages((prev) => [...prev, data]);
          }

          setPendingChannel({
            chatId: newChatId,
            chatMessageId: newChatMessageId,
          });

          return { chatId: newChatId, chatMessageId: newChatMessageId };
        })
        .catch((error) => {
          console.error(error);

          // Check if it's an insufficient tokens error
          if (error.response?.status === 402) {
            const errorData = error.response.data;
            const messageArgs = errorData.message_args || {};

            setHasInsufficientTokens(true);
            setInsufficientTokensMessage(
              errorData.error_message ||
                `Insufficient tokens. You need ${messageArgs.tokens_required} tokens but only have ${messageArgs.tokens_available} remaining.`
            );
          }

          // Let the notification service handle the error display automatically
          // It will parse error_message, severity, and is_markdown from the response
          notify({ error });
        });
    },
    [
      selectedChatId,
      setSelectedChatId,
      setChatName,
      setChatMessages,
      postChatPrompt,
      createChannel,
      selectedLlmModel,
      selectedCoderLlmModel,
      notify,
    ]
  );

  // Determine credit usage data based on context.
  // Use null when no real data is available — LowBalanceWarning handles null
  // by not rendering, preventing false "Out of Tokens" warnings.
  let finalTokenUsageData;

  if (selectedChatId) {
    finalTokenUsageData =
      chatMessages.length > 0
        ? chatMessages[chatMessages.length - 1]?.token_usage_data || null
        : realTokenBalance || null;
  } else {
    finalTokenUsageData = realTokenBalance || null;
  }

  if (selectedChatId) {
    return (
      <ExistingChat
        selectedChatId={selectedChatId}
        chatName={chatName}
        setChatName={setChatName}
        savePrompt={savePrompt}
        chatMessages={chatMessages}
        setChatMessages={setChatMessages}
        isGetChatMessages={isGetChatMessages}
        resetChatMessageIdentifier={resetChatMessageIdentifier}
        isPromptRunning={isPromptRunning}
        chatIntents={chatIntents}
        selectedChatIntent={selectedChatIntent}
        setSelectedChatIntent={setSelectedChatIntent}
        llmModels={llmModels}
        selectedLlmModel={selectedLlmModel}
        setSelectedLlmModel={setSelectedLlmModel}
        selectedCoderLlmModel={selectedCoderLlmModel}
        setSelectedCoderLlmModel={setSelectedCoderLlmModel}
        handleTransformApply={handleTransformApply}
        triggerRetryTransform={triggerRetryTransform}
        stopPromptRun={stopPromptRun}
        handleSqlRun={handleSqlRun}
        promptAutoComplete={promptAutoComplete}
        isChatConversationDisabled={
          isChatConversationDisabled || hasInsufficientTokens
        }
        tokenUsageData={finalTokenUsageData}
        hasInsufficientTokens={hasInsufficientTokens}
        insufficientTokensMessage={insufficientTokensMessage}
        onPromptSelect={onPromptSelect}
        selectedPrompt={selectedPrompt}
        // Onboarding props
        isOnboardingMode={isOnboardingMode}
        onboardingConfig={onboardingConfig}
        completedOnboardingSteps={completedOnboardingSteps}
        skippedOnboardingSteps={skippedOnboardingSteps}
        currentOnboardingStep={currentOnboardingStep}
        isTypingPrompt={isTypingPrompt}
        onOnboardingPromptSelect={onOnboardingPromptSelect}
        onSkipOnboarding={onSkipOnboarding}
        onOnboardingComplete={onOnboardingComplete}
        onSkipCurrentTask={onSkipCurrentTask}
        onSendButtonClick={onSendButtonClick}
      />
    );
  }

  return (
    <NewChat
      isChatDrawerOpen={isChatDrawerOpen}
      setSelectedChatId={setSelectedChatId}
      setChatName={setChatName}
      savePrompt={savePrompt}
      triggerGetChatMessagesApi={triggerGetChatMessagesApi}
      isPromptRunning={isPromptRunning}
      chatIntents={chatIntents}
      selectedChatIntent={selectedChatIntent}
      setSelectedChatIntent={setSelectedChatIntent}
      llmModels={llmModels}
      selectedLlmModel={selectedLlmModel}
      setSelectedLlmModel={setSelectedLlmModel}
      selectedCoderLlmModel={selectedCoderLlmModel}
      setSelectedCoderLlmModel={setSelectedCoderLlmModel}
      promptAutoComplete={promptAutoComplete}
      tokenUsageData={finalTokenUsageData}
      hasInsufficientTokens={hasInsufficientTokens}
      insufficientTokensMessage={insufficientTokensMessage}
      onPromptSelect={onPromptSelect}
      selectedPrompt={selectedPrompt}
      // Onboarding props
      isOnboardingMode={isOnboardingMode}
      onboardingConfig={onboardingConfig}
      completedOnboardingSteps={completedOnboardingSteps}
      skippedOnboardingSteps={skippedOnboardingSteps}
      currentOnboardingStep={currentOnboardingStep}
      isTypingPrompt={isTypingPrompt}
      onOnboardingPromptSelect={onOnboardingPromptSelect}
      onSkipOnboarding={onSkipOnboarding}
      onOnboardingComplete={onOnboardingComplete}
      onSkipCurrentTask={onSkipCurrentTask}
      onSendButtonClick={onSendButtonClick}
    />
  );
};

Body.propTypes = {
  isChatDrawerOpen: PropTypes.bool.isRequired,
  selectedChatId: PropTypes.string,
  setSelectedChatId: PropTypes.func.isRequired,
  chatName: PropTypes.string,
  setChatName: PropTypes.func.isRequired,
  chatMessages: PropTypes.array,
  setChatMessages: PropTypes.func.isRequired,
  isPromptRunning: PropTypes.bool.isRequired,
  triggerRetryTransform: PropTypes.bool.isRequired,
  isChatConversationDisabled: PropTypes.bool.isRequired,
  tokenUsageData: PropTypes.object,
  onPromptSelect: PropTypes.func,
  selectedPrompt: PropTypes.string,
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

export { Body };
