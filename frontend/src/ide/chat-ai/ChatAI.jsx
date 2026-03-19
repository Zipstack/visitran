import {
  memo,
  useEffect,
  useState,
  useCallback,
  useRef,
  forwardRef,
  useImperativeHandle,
} from "react";
import PropTypes from "prop-types";

import { useSocketMessagesStore } from "../../store/socket-messages-store";
import { Header } from "./Header";
import { Body } from "./Body";
import { useRefreshModelsStore } from "../../store/refresh-models-store";
import { useProjectStore } from "../../store/project-store";
import { useTokenStore } from "../../store/token-store";

// Cloud-only: fetch token balance (unavailable in OSS — import fails gracefully)
let fetchOrganizationTokenBalance = null;
try {
  ({
    fetchOrganizationTokenBalance,
  } = require("../../plugins/token-management/token-balance"));
} catch {
  // OSS: token balance not available
}
// import { jaffleShopOnboardingConfig } from "./onboarding-config"; // Replaced with API data
import { useOnboardingService } from "../../services/onboardingService";
import { useChatAIService } from "./services";
// import { orgStore } from "../../store/org-store"; // Unused for now

const ChatAI = memo(
  forwardRef(function ChatAI(
    {
      isChatDrawerOpen,
      closeChatDrawer,
      collapseDrawer,
      toggleFullWidth,
      isFullWidth,
    },
    ref
  ) {
    const [selectedChatId, setSelectedChatId] = useState(null);
    const [chatName, setChatName] = useState("");
    const [chatMessages, setChatMessages] = useState([]);
    const [isPromptRunning, setIsPromptRunning] = useState(false);
    const [triggerRetryTransform, setTriggerRetryTransform] = useState(false);
    const [isChatConversationDisabled, setIsChatConversationDisabled] =
      useState(false);
    const [selectedPrompt, setSelectedPrompt] = useState("");

    // Onboarding state
    const [isOnboardingMode, setIsOnboardingMode] = useState(false);
    const [onboardingStatus, setOnboardingStatus] = useState(null);
    const [, setLoadingOnboardingStatus] = useState(false);
    const [completedOnboardingSteps, setCompletedOnboardingSteps] = useState(
      new Set()
    );
    const [skippedOnboardingSteps, setSkippedOnboardingSteps] = useState(
      new Set()
    );
    const [currentOnboardingStep, setCurrentOnboardingStep] = useState(null);
    const [isTypingPrompt, setIsTypingPrompt] = useState(false);
    const [typingPrompt, setTypingPrompt] = useState("");
    const typingIntervalRef = useRef(null);

    const { socketMessages, removeMessagesBatch } = useSocketMessagesStore();
    const {
      getOnboardingStatus,
      markOnboardingComplete,
      skipTask,
      resetOnboarding,
    } = useOnboardingService();
    const { completeOnboardingTask } = useChatAIService();
    const { setRefreshModels } = useRefreshModelsStore();
    // const { selectedOrgId } = orgStore(); // Unused for now
    const projectId = useProjectStore((state) => state.projectId);

    // Credit balance store
    const { tokenBalance, setTokenBalance, setLoading, setError } =
      useTokenStore();

    useEffect(() => {
      if (!socketMessages?.length) return;

      const updatedMessages = [...chatMessages];
      const uuidsToRemove = [];

      socketMessages.forEach((msg) => {
        const idx = updatedMessages.findIndex(
          (chatMsg) => chatMsg?.chat_message_id === msg?.chat_message_id
        );
        if (idx >= 0) {
          const existing = { ...updatedMessages[idx] };

          if (msg?.event_type !== "prompt_response") return;

          if (msg?.thought_chain) {
            existing.thought_chain = [
              ...(existing.thought_chain || []),
              msg.thought_chain,
            ];
          }

          if (msg?.summary) {
            existing.response = [...(existing.response || []), msg.summary];
          }

          if (msg?.prompt_status) {
            const previousStatus = existing.prompt_status;
            existing.prompt_status = msg?.prompt_status;

            // For non-TRANSFORM intents (INFO, SQL, CHAT), complete on prompt success
            // These intents don't require transformation validation
            if (
              msg?.prompt_status === "SUCCESS" &&
              previousStatus !== "SUCCESS" &&
              isOnboardingMode &&
              currentOnboardingStep &&
              currentOnboardingStep.mode !== "transform"
            ) {
              handleOnboardingTaskCompletion(currentOnboardingStep.id);

              // Clear the current step after triggering completion
              setCurrentOnboardingStep(null);
            }
          }

          if (msg?.transformation_status) {
            const previousTransformStatus = existing.transformation_status;
            existing.transformation_status = msg?.transformation_status;

            if (msg?.transformation_status === "SUCCESS") {
              setRefreshModels(true);

              // For TRANSFORM intent, complete onboarding task only when transformation succeeds
              // This ensures the transformation was actually applied successfully
              if (
                previousTransformStatus !== "SUCCESS" &&
                isOnboardingMode &&
                currentOnboardingStep &&
                currentOnboardingStep.mode === "transform" &&
                existing.prompt_status === "SUCCESS" // Ensure prompt was also successful
              ) {
                handleOnboardingTaskCompletion(currentOnboardingStep.id);

                // Clear the current step after triggering completion
                setCurrentOnboardingStep(null);
              }
            }
          }

          if (msg?.error_msg) {
            existing.error_msg = msg?.error_msg;
          }

          if (msg?.is_sql_query_runnable) {
            existing.is_sql_query_runnable = msg?.is_sql_query_runnable;
          }

          if (msg?.query_result) {
            existing.query_result = msg?.query_result;
          }

          if (msg?.is_retry_transform) {
            setTriggerRetryTransform((prev) => !prev);
          }

          if (msg?.discussion_status) {
            existing.discussion_type = msg?.discussion_status;
          }

          if (msg?.token_usage_data) {
            existing.token_usage_data = msg?.token_usage_data;
          }

          updatedMessages[idx] = existing;
        }
        uuidsToRemove.push(msg?.uuid);
      });

      setChatMessages(updatedMessages);
      removeMessagesBatch(uuidsToRemove);
    }, [socketMessages, chatMessages, removeMessagesBatch]);

    useEffect(() => {
      const isAnyRunning = chatMessages?.some(
        (m) => m?.prompt_status === "RUNNING"
      );
      setIsPromptRunning(Boolean(isAnyRunning));

      const chatConversationDisabledStatus = chatMessages?.some(
        (m) => m?.discussion_type === "APPROVED"
      );
      setIsChatConversationDisabled(chatConversationDisabledStatus);
    }, [chatMessages]);

    // Fetch credit balance when drawer opens (Cloud only)
    useEffect(() => {
      if (!fetchOrganizationTokenBalance || !isChatDrawerOpen) return;

      const fetchTokenBalance = async () => {
        try {
          setLoading(true);
          const tokenData = await fetchOrganizationTokenBalance();
          setTokenBalance(tokenData);
          setLoading(false);
        } catch (error) {
          console.error("Error fetching credit balance:", error);
          setError(error.message);
          setLoading(false);
        }
      };

      fetchTokenBalance();
    }, [isChatDrawerOpen, setLoading, setTokenBalance, setError]);

    const resetSelectedChatId = useCallback(() => {
      setSelectedChatId(null);
      setChatName("");
    }, []);

    // Expose startNewChat method via ref
    useImperativeHandle(
      ref,
      () => ({
        startNewChat: resetSelectedChatId,
      }),
      [resetSelectedChatId]
    );

    // Check onboarding status when drawer opens or project changes
    const checkOnboardingStatus = useCallback(async () => {
      if (!isChatDrawerOpen || !projectId) return;

      setLoadingOnboardingStatus(true);
      try {
        const response = await getOnboardingStatus(projectId);
        if (response.success) {
          setOnboardingStatus(response.data);

          // Auto-enable onboarding if enabled for project AND progress is less than 99%
          const progressPercentage =
            response.data.progress_percentage ||
            response.data.progress?.progress_percentage;

          if (response.data.onboarding_enabled && progressPercentage < 99) {
            setIsOnboardingMode(true);
          } else {
            setIsOnboardingMode(false);
          }
        }
      } catch (error) {
        // Silent error handling - onboarding is not critical
      } finally {
        setLoadingOnboardingStatus(false);
      }
    }, [isChatDrawerOpen, projectId]);

    // Handle onboarding task completion
    const handleOnboardingTaskCompletion = useCallback(
      async (taskId) => {
        if (
          !taskId ||
          !isOnboardingMode ||
          !onboardingStatus?.onboarding_enabled
        ) {
          return;
        }

        try {
          await completeOnboardingTask(taskId);

          // Refresh onboarding status to get updated progress
          await checkOnboardingStatus();

          // Mark task as completed locally
          setCompletedOnboardingSteps((prev) => new Set([...prev, taskId]));
        } catch (error) {
          // Silent error handling - don't show error to user as this is auto-completion
        }
      },
      [
        isOnboardingMode,
        onboardingStatus,
        completeOnboardingTask,
        checkOnboardingStatus,
      ]
    );

    // Check onboarding status when drawer opens or project changes
    useEffect(() => {
      checkOnboardingStatus();
    }, [checkOnboardingStatus]);

    const scrollToPromptArea = useCallback(() => {
      // Find the main scrollable container in the chat drawer
      const selectors = [
        ".chat-ai-container .height-100.overflow-y-auto",
        ".height-100.overflow-y-auto.pad-8",
        '[class*="overflow-y-auto"][class*="height-100"]',
      ];

      let scrollContainer = null;

      for (const selector of selectors) {
        const elements = document.querySelectorAll(selector);
        for (const element of elements) {
          // Check if this element is actually scrollable
          if (element.scrollHeight > element.clientHeight) {
            scrollContainer = element;
            break;
          }
        }
        if (scrollContainer) break;
      }

      if (scrollContainer) {
        // Scroll to bottom to show the input area
        scrollContainer.scrollTo({
          top: scrollContainer.scrollHeight,
          behavior: "smooth",
        });

        // Also try to bring input into view after a delay
        setTimeout(() => {
          const inputArea = document.querySelector(
            '.input-prompt-actions, [class*="Sender"]'
          );
          if (inputArea) {
            inputArea.scrollIntoView({ behavior: "smooth", block: "end" });
          }
        }, 500);
      }
    }, []);

    const handlePromptSelect = useCallback((prompt) => {
      setSelectedPrompt(prompt);
    }, []);

    const handleOnboardingPromptSelect = useCallback(
      (prompt, step) => {
        setCurrentOnboardingStep(step);
        setIsTypingPrompt(true);
        setTypingPrompt("");
        setSelectedPrompt("");

        // Clear any existing interval
        if (typingIntervalRef.current) {
          clearInterval(typingIntervalRef.current);
          typingIntervalRef.current = null;
        }

        // Scroll to show the prompt area immediately when Try This is clicked
        setTimeout(() => {
          scrollToPromptArea();
        }, 100);

        // Simulate typing effect - faster for transform mode
        // Transform mode: type line-by-line (much faster for long prompts)
        // Other modes (sql, chat): type character-by-character
        const isTransformMode = step?.mode === "transform";

        if (isTransformMode) {
          // Line-by-line typing for transform prompts
          const lines = prompt.split("\n");
          let lineIndex = 0;

          typingIntervalRef.current = setInterval(() => {
            if (lineIndex < lines.length) {
              const currentText = lines.slice(0, lineIndex + 1).join("\n");
              setTypingPrompt(currentText);
              lineIndex++;

              // Auto-scroll the textarea
              setTimeout(() => {
                const textarea =
                  document.querySelector(".ant-sender-input textarea") ||
                  document.querySelector(
                    'textarea[placeholder="How can I help you?"]'
                  );
                if (textarea) {
                  textarea.scrollTop = textarea.scrollHeight;
                }
              }, 10);
            } else {
              clearInterval(typingIntervalRef.current);
              typingIntervalRef.current = null;
              setIsTypingPrompt(false);
              setSelectedPrompt(prompt);
            }
          }, 80); // 80ms per line for fast typing
        } else {
          // Character-by-character typing for sql/chat prompts
          let index = 0;

          typingIntervalRef.current = setInterval(() => {
            if (index < prompt.length) {
              const currentText = prompt.slice(0, index + 1);
              setTypingPrompt(currentText);
              index++;

              // Auto-scroll the textarea every 10 characters
              if (index % 10 === 0) {
                setTimeout(() => {
                  const textarea =
                    document.querySelector(".ant-sender-input textarea") ||
                    document.querySelector(
                      'textarea[placeholder="How can I help you?"]'
                    );
                  if (textarea) {
                    textarea.scrollTop = textarea.scrollHeight;
                  }
                }, 10);
              }
            } else {
              clearInterval(typingIntervalRef.current);
              typingIntervalRef.current = null;
              setIsTypingPrompt(false);
              setSelectedPrompt(prompt);
            }
          }, 30); // 30ms per character for smooth typing
        }
      },
      [scrollToPromptArea]
    );

    // Note: Removed auto-clearing of currentOnboardingStep when chat is created
    // This was causing the step to be cleared before prompt completion
    // Now currentOnboardingStep is only cleared after successful auto-completion

    const handleSkipOnboarding = useCallback(async () => {
      // Clear typing interval
      if (typingIntervalRef.current) {
        clearInterval(typingIntervalRef.current);
        typingIntervalRef.current = null;
      }

      // Clear local state
      setIsOnboardingMode(false);
      setCompletedOnboardingSteps(new Set());
      setSkippedOnboardingSteps(new Set());
      setCurrentOnboardingStep(null);
      setIsTypingPrompt(false);
      setSelectedPrompt("");
      setTypingPrompt("");

      // Skip all incomplete tasks and mark onboarding as complete
      if (projectId && onboardingStatus?.tasks) {
        try {
          // Get all tasks that are not completed
          const incompleteTasks = onboardingStatus.tasks.filter(
            (task) => !completedOnboardingSteps.has(task.id)
          );

          // Skip each incomplete task
          const skipPromises = incompleteTasks.map((task) =>
            skipTask(projectId, task.id).catch((error) => {
              console.error(`Failed to skip task ${task.id}:`, error);
              // Continue even if one task fails
            })
          );

          // Wait for all skip operations to complete
          await Promise.all(skipPromises);

          // After skipping all tasks, mark onboarding as complete
          await markOnboardingComplete(projectId);

          // Update local onboarding status to reflect completion
          setOnboardingStatus((prev) => ({
            ...prev,
            is_completed: true,
          }));
        } catch (error) {
          // Silent error handling - skip still works locally even if API fails
          console.error("Failed to complete skip onboarding process:", error);
        }
      }
    }, [
      projectId,
      onboardingStatus,
      completedOnboardingSteps,
      skipTask,
      markOnboardingComplete,
    ]);

    const handleSkipCurrentTask = useCallback(
      async (stopPromptRun) => {
        if (!currentOnboardingStep || !projectId) {
          return;
        }

        try {
          // Stop the current prompt if it's running
          if (stopPromptRun && typeof stopPromptRun === "function") {
            stopPromptRun();
          }

          // Call the skip API
          const result = await skipTask(projectId, currentOnboardingStep.id);

          if (result.success) {
            // Mark task as skipped locally
            setSkippedOnboardingSteps(
              (prev) => new Set([...prev, currentOnboardingStep.id])
            );

            // Clear the current step
            setCurrentOnboardingStep(null);

            // Clear any typing animation
            if (typingIntervalRef.current) {
              clearInterval(typingIntervalRef.current);
              typingIntervalRef.current = null;
            }
            setIsTypingPrompt(false);
            setSelectedPrompt("");
            setTypingPrompt("");

            // Refresh onboarding status to get updated progress
            await checkOnboardingStatus();
          }
        } catch (error) {
          // Silent error handling
        }
      },
      [currentOnboardingStep, projectId, skipTask, checkOnboardingStatus]
    );

    const handleOnboardingComplete = useCallback(async () => {
      if (!projectId) {
        handleSkipOnboarding();
        return;
      }

      try {
        // Mark onboarding as complete in the backend
        const result = await markOnboardingComplete(projectId);

        if (result.success) {
          // Update local state to reflect completion
          setOnboardingStatus((prev) => ({
            ...prev,
            is_completed: true,
          }));

          // Clear onboarding mode
          setIsOnboardingMode(false);
          setCompletedOnboardingSteps(new Set());
          setSkippedOnboardingSteps(new Set());
          setCurrentOnboardingStep(null);
          setIsTypingPrompt(false);
          setSelectedPrompt("");
          setTypingPrompt("");

          // Navigate back to initial chat screen (NewChat component)
          setSelectedChatId(null);
        } else {
          // Still close onboarding even if API fails
          handleSkipOnboarding();
        }
      } catch (error) {
        // Still close onboarding even if there's an error
        handleSkipOnboarding();
      }
    }, [markOnboardingComplete, projectId, handleSkipOnboarding]);

    const handleResetOnboarding = useCallback(async () => {
      if (!projectId) return;

      try {
        // Call API to reset onboarding status
        const result = await resetOnboarding(projectId);

        if (result.success) {
          // Reset local onboarding state
          setIsOnboardingMode(true);
          setCompletedOnboardingSteps(new Set());
          setSkippedOnboardingSteps(new Set());
          setCurrentOnboardingStep(null);
          setIsTypingPrompt(false);
          setSelectedPrompt("");
          setTypingPrompt("");

          // Navigate back to initial chat screen
          setSelectedChatId(null);

          // Refresh onboarding status from server to get updated data
          await checkOnboardingStatus();
        } else {
          console.error("Failed to reset onboarding:", result.error);
        }
      } catch (error) {
        // Silent error handling - onboarding reset is not critical
        console.error("Failed to reset onboarding:", error);
      }
    }, [projectId, resetOnboarding, checkOnboardingStatus]);

    // Cleanup interval on unmount
    useEffect(() => {
      return () => {
        if (typingIntervalRef.current) {
          clearInterval(typingIntervalRef.current);
        }
      };
    }, []);

    // Get credit usage data from the latest chat message
    const latestMessage =
      chatMessages.length > 0 ? chatMessages[chatMessages.length - 1] : null;
    const tokenUsageData = latestMessage?.token_usage_data;

    const handleBuyTokens = useCallback(() => {
      // TODO: Implement credit purchase flow - for now just a placeholder
    }, []);

    return (
      <div
        className={`chat-ai-container ${isFullWidth ? "fullscreen-mode" : ""}`}
      >
        <Header
          resetSelectedChatId={resetSelectedChatId}
          closeChatDrawer={closeChatDrawer}
          collapseDrawer={collapseDrawer}
          toggleFullWidth={toggleFullWidth}
          isFullWidth={isFullWidth}
          isPromptRunning={isPromptRunning}
          isOnboardingCompleted={onboardingStatus?.is_completed}
          onResetOnboarding={handleResetOnboarding}
          tokenBalance={tokenBalance}
          onBuyTokens={handleBuyTokens}
        />

        <div className="flex-1 overflow-hidden">
          <Body
            isChatDrawerOpen={isChatDrawerOpen}
            selectedChatId={selectedChatId}
            setSelectedChatId={setSelectedChatId}
            chatName={chatName}
            setChatName={setChatName}
            chatMessages={chatMessages}
            setChatMessages={setChatMessages}
            isPromptRunning={isPromptRunning}
            triggerRetryTransform={triggerRetryTransform}
            isChatConversationDisabled={isChatConversationDisabled}
            tokenUsageData={tokenUsageData}
            onPromptSelect={handlePromptSelect}
            selectedPrompt={isTypingPrompt ? typingPrompt : selectedPrompt}
            // Onboarding props
            isOnboardingMode={isOnboardingMode}
            onboardingConfig={
              onboardingStatus?.template && onboardingStatus?.tasks
                ? {
                    title: onboardingStatus.template.title,
                    description: onboardingStatus.template.description,
                    steps: onboardingStatus.tasks,
                    totalSteps:
                      onboardingStatus.progress?.total_tasks ||
                      onboardingStatus.tasks.length,
                    progress: onboardingStatus.progress,
                    is_completed: onboardingStatus.is_completed,
                  }
                : null
            }
            completedOnboardingSteps={completedOnboardingSteps}
            skippedOnboardingSteps={skippedOnboardingSteps}
            currentOnboardingStep={currentOnboardingStep}
            isTypingPrompt={isTypingPrompt}
            onOnboardingPromptSelect={handleOnboardingPromptSelect}
            onSkipOnboarding={handleSkipOnboarding}
            onOnboardingComplete={handleOnboardingComplete}
            onSkipCurrentTask={handleSkipCurrentTask}
            onSendButtonClick={() => {
              // Clear the selected prompt to stop animation after send is clicked
              setSelectedPrompt("");
            }}
          />
        </div>
      </div>
    );
  })
);

ChatAI.propTypes = {
  isChatDrawerOpen: PropTypes.bool.isRequired,
  closeChatDrawer: PropTypes.func.isRequired,
  collapseDrawer: PropTypes.func.isRequired,
  toggleFullWidth: PropTypes.func.isRequired,
  isFullWidth: PropTypes.bool,
};

ChatAI.displayName = "ChatAI";

export { ChatAI };
