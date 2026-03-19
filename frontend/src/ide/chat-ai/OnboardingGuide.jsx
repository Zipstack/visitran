import { memo, useState, useEffect, useCallback } from "react";
import PropTypes from "prop-types";
import { Space, Card, Button, Typography, Tag, Tooltip, Modal } from "antd";
import {
  CheckCircleOutlined,
  CloseOutlined,
  RocketOutlined,
  DownOutlined,
  UpOutlined,
} from "@ant-design/icons";

import { useUserStore } from "../../store/user-store";
import "./OnboardingGuide.css";

const { Title, Text } = Typography;

const OnboardingGuide = memo(function OnboardingGuide({
  visible,
  config,
  onPromptSelect,
  completedSteps = new Set(),
  skippedSteps = new Set(),
  showWelcome = true,
  compact = false,
  currentStep = 0,
  totalSteps = 4,
  onSkip,
  hideModeTag = false,
  collapsibleTodos = false,
  onComplete,
  onSkipCurrentTask,
  currentOnboardingStep,
}) {
  const currentTheme = useUserStore(
    (state) => state?.userDetails?.currentTheme
  );
  const [showQuickGuide, setShowQuickGuide] = useState(false);
  const [showProgress, setShowProgress] = useState(false);
  const [showTasks, setShowTasks] = useState([]);
  const [isCollapsed, setIsCollapsed] = useState(collapsibleTodos);
  // const [showCompletionPopup, setShowCompletionPopup] = useState(false);
  // const [hasShownCompletion, setHasShownCompletion] = useState(false);
  const [showSkipModal, setShowSkipModal] = useState(false);
  const [skipModalType, setSkipModalType] = useState(null); // 'onboarding' or 'task'

  const handleTryThis = useCallback(
    (step) => {
      if (onPromptSelect) {
        onPromptSelect(step.prompt, step);
      }

      // Collapse the todo list when "Try This" is clicked
      if (collapsibleTodos) {
        setIsCollapsed(true);
      }
    },
    [onPromptSelect, collapsibleTodos]
  );

  // Check for completion and show popup (for NewChat view only)
  useEffect(() => {
    // Disable this popup logic - let ExistingChat handle it to avoid duplicates
    // The OnboardingGuide in NewChat doesn't need its own popup
    return;

    /* Original code disabled to prevent duplicate popups
    if (!visible || !config || hasShownCompletion) return;

    const totalTasks = config.totalSteps || config.steps?.length || 0;
    const completedCount = config.progress?.completed_tasks || completedSteps.size;
    const progressPercentage = totalTasks > 0 ? (completedCount / totalTasks * 100) : 0;

    // Only show popup if tasks are 100% complete AND is_completed is false
    const shouldShowPopup = progressPercentage === 100 && !config.is_completed;

    if (shouldShowPopup) {
      // Delay popup to allow for smooth transition
      const timer = setTimeout(() => {
        setShowCompletionPopup(true);
        setHasShownCompletion(true);
      }, 1000);

      return () => clearTimeout(timer);
    }
    */
  }, [visible, config, completedSteps]); // hasShownCompletion removed

  // const handleCompletionClose = useCallback(() => {
  //   setShowCompletionPopup(false);
  // }, []);

  // const handleCompletionContinue = useCallback(async () => {
  //   // Call the onComplete handler if provided (will mark as complete in backend)
  //   if (onComplete) {
  //     await onComplete();
  //   } else if (onSkip) {
  //     // Fallback to skip if no onComplete handler
  //     onSkip();
  //   }

  //   // Close the popup
  //   setShowCompletionPopup(false);
  // }, [onComplete, onSkip]);

  // Handle skip button click - show appropriate modal
  const handleSkipClick = useCallback(() => {
    const isInitialScreen = showWelcome !== false;

    // For task skip, check if there's an active task
    if (!isInitialScreen && !currentOnboardingStep) {
      // No active task to skip, do nothing
      return;
    }

    setSkipModalType(isInitialScreen ? "onboarding" : "task");
    setShowSkipModal(true);
  }, [showWelcome, currentOnboardingStep]);

  // Handle modal confirmation
  const handleModalConfirm = useCallback(() => {
    if (skipModalType === "onboarding") {
      if (onSkip) {
        onSkip();
      }
    } else {
      if (onSkipCurrentTask) {
        onSkipCurrentTask();
      }
      // Auto-expand the collapsed todo list after skip confirmation
      // so user can see the remaining tasks
      if (isCollapsed) {
        setIsCollapsed(false);
      }
    }
    setShowSkipModal(false);
    setSkipModalType(null);
  }, [skipModalType, onSkip, onSkipCurrentTask, isCollapsed]);

  // Handle modal cancel
  const handleModalCancel = useCallback(() => {
    setShowSkipModal(false);
    setSkipModalType(null);
  }, []);

  useEffect(() => {
    if (!visible || !config) {
      setShowQuickGuide(false);
      setShowProgress(false);
      setShowTasks([]);
      return;
    }

    // Stagger animations when onboarding becomes visible
    const timer1 = setTimeout(() => setShowQuickGuide(true), 300);
    const timer2 = setTimeout(() => setShowProgress(true), 600);
    const timer3 = setTimeout(() => {
      // Show tasks one by one
      config.steps.forEach((_, index) => {
        setTimeout(() => {
          setShowTasks((prev) => [...prev, index]);
        }, index * 200);
      });
    }, 900);

    return () => {
      clearTimeout(timer1);
      clearTimeout(timer2);
      clearTimeout(timer3);
    };
  }, [visible, config]);

  if (!visible || !config) return null;

  // const getStepIcon = (step) => {
  //   if (completedSteps.has(step.id)) {
  //     return <CheckCircleOutlined className="step-icon-completed" />;
  //   }
  //   return <PlayCircleOutlined className="step-icon-pending" />;
  // };

  // const getStepStatus = (step) => {
  //   if (completedSteps.has(step.id)) {
  //     return { status: "success", text: "Completed" };
  //   }
  //   return { status: "processing", text: "Try This" };
  // };

  const getModeColor = (mode) => {
    switch (mode) {
      case "transform":
        return "#FF4D6D";
      case "sql":
        return "#00A6ED";
      case "chat":
        return "#7FB800";
      default:
        return "#1890ff";
    }
  };

  const getModeIcon = (mode) => {
    switch (mode) {
      case "transform":
        return "🔄";
      case "sql":
        return "💾";
      case "chat":
        return "💬";
      default:
        return "▶️";
    }
  };

  if (compact) {
    // Compact version for ExistingChat
    const remainingSteps = config.steps.filter(
      (step) => !completedSteps.has(step.id) && !skippedSteps.has(step.id)
    );

    if (remainingSteps.length === 0) {
      return (
        <Card
          size="small"
          className="compact-card compact-completed"
          bodyStyle={{ padding: 8 }}
        >
          <div className="compact-center">
            <Text className="compact-completed-text">
              🎉 Onboarding Complete! All tasks finished.
            </Text>
          </div>
        </Card>
      );
    }

    return (
      <Card
        size="small"
        className="compact-card compact-remaining"
        bodyStyle={{ padding: 8 }}
      >
        <div className="compact-content">
          <Text strong className="compact-title">
            📋 Remaining Tasks ({remainingSteps.length})
          </Text>
          <div className="compact-tasks-container">
            {remainingSteps.slice(0, 2).map((step, index) => {
              const modeColor = getModeColor(step.mode);
              const modeIcon = getModeIcon(step.mode);

              return (
                <div
                  key={step.id}
                  className={`compact-task-item ${
                    index < remainingSteps.length - 1 ? "with-margin" : ""
                  }`}
                >
                  <div className="compact-task-content">
                    <span className="compact-task-icon">{modeIcon}</span>
                    <Text className="compact-task-text">
                      {step.title.length > 50
                        ? step.title.substring(0, 50) + "..."
                        : step.title}
                    </Text>
                  </div>
                  <Button
                    type="primary"
                    size="small"
                    onClick={() => handleTryThis(step)}
                    className="compact-try-button-style"
                    style={{
                      "--mode-color": modeColor,
                    }}
                  >
                    Try
                  </Button>
                </div>
              );
            })}
            {remainingSteps.length > 2 && (
              <Text className="compact-more-text">
                +{remainingSteps.length - 2} more tasks...
              </Text>
            )}
          </div>
        </div>
      </Card>
    );
  }

  return (
    <>
      {/* Completion popup removed - handled by ExistingChat to avoid duplicates */}

      <div className="onboarding-guide-container">
        <Space direction="vertical" className="width-100" size={8}>
          {/* Welcome Message */}
          {showWelcome && (
            <Card
              className={`quick-start-card ${
                currentTheme === "dark" ? "dark" : "light"
              } ${showQuickGuide ? "fade-in-card" : ""}`}
              bodyStyle={{ padding: 16 }}
            >
              <div className="quick-start-content">
                <div className="quick-start-icon">
                  <RocketOutlined className="rocket-icon" />
                </div>
                <div className="quick-start-text">
                  <Title
                    level={4}
                    className={`quick-start-title ${
                      currentTheme === "dark" ? "dark" : "light"
                    }`}
                  >
                    {config?.title || "🚀 Quick Start Guide"}
                  </Title>
                  <Text
                    className={`quick-start-description ${
                      currentTheme === "dark" ? "dark" : "light"
                    }`}
                  >
                    {config?.description ||
                      "Try the interactive tasks below to explore key features with your data."}
                  </Text>
                </div>
              </div>
            </Card>
          )}

          {/* To-Do List */}
          <Card
            className={`todo-list-card ${
              currentTheme === "dark" ? "dark" : "light"
            }`}
            bodyStyle={{ padding: 16 }}
          >
            <Space direction="vertical" className="width-100" size={12}>
              {/* Onboarding Progress */}
              <div
                className={`onboarding-progress ${
                  currentTheme === "dark" ? "dark" : "light"
                } ${showProgress ? "show" : ""}`}
              >
                <Space size={12}>
                  <CheckCircleOutlined className="progress-icon" />
                  <div>
                    <Text
                      strong
                      className={`progress-title ${
                        currentTheme === "dark" ? "dark" : "light"
                      }`}
                    >
                      Onboarding Progress
                    </Text>
                    <div>
                      <Text
                        className={`progress-step ${
                          currentTheme === "dark" ? "dark" : "light"
                        }`}
                      >
                        Step {currentStep} of {totalSteps}
                      </Text>
                    </div>
                  </div>
                </Space>

                <Space size={8} align="center">
                  <div className="progress-badge">
                    {currentStep}/{totalSteps}
                  </div>

                  <Tooltip
                    title={
                      showWelcome !== false
                        ? "Skip Onboarding"
                        : currentOnboardingStep
                        ? "Skip this task"
                        : "No active task to skip"
                    }
                    placement="top"
                  >
                    <Button
                      type="text"
                      size="small"
                      icon={<CloseOutlined />}
                      onClick={handleSkipClick}
                      disabled={showWelcome === false && !currentOnboardingStep}
                      className={`skip-button ${
                        currentTheme === "dark" ? "dark" : "light"
                      }`}
                    >
                      Skip
                    </Button>
                  </Tooltip>

                  {collapsibleTodos && (
                    <Button
                      type="text"
                      size="small"
                      icon={isCollapsed ? <DownOutlined /> : <UpOutlined />}
                      onClick={() => setIsCollapsed(!isCollapsed)}
                    >
                      {isCollapsed ? "Show Tasks" : "Hide Tasks"}
                    </Button>
                  )}
                </Space>
              </div>

              {!isCollapsed && (
                <div>
                  {config.steps.map((step, index) => {
                    // const stepStatus = getStepStatus(step);
                    const isCompleted =
                      step.status === "completed" ||
                      completedSteps.has(step.id);
                    const isSkipped =
                      step.status === "skipped" || skippedSteps.has(step.id);
                    // const isPending = !isCompleted && !isSkipped;
                    // const modeColor = getModeColor(step.mode);
                    // const modeIcon = getModeIcon(step.mode);

                    return (
                      <div
                        key={step.id}
                        className={`task-item ${
                          currentTheme === "dark" ? "dark" : "light"
                        } ${showTasks.includes(index) ? "show" : ""}`}
                      >
                        <div
                          className={`task-number ${
                            isCompleted
                              ? "completed"
                              : isSkipped
                              ? "skipped"
                              : "pending"
                          }`}
                        >
                          {index + 1}
                        </div>

                        <div className="task-content">
                          <div className="task-content-layout">
                            <Text
                              strong
                              className={`task-title ${
                                currentTheme === "dark" ? "dark" : "light"
                              }`}
                            >
                              {step.title}
                            </Text>
                            <div className="task-meta-container">
                              {step.mode && !hideModeTag && (
                                <Tag
                                  color={getModeColor(step.mode)}
                                  className="mode-tag-style"
                                >
                                  {step.mode.toUpperCase()}
                                </Tag>
                              )}
                              {step.description && (
                                <Text
                                  className={`task-description-style ${
                                    currentTheme === "dark" ? "dark" : "light"
                                  }`}
                                >
                                  {step.description}
                                </Text>
                              )}
                            </div>
                          </div>
                        </div>

                        <Button
                          type={isSkipped ? "default" : "primary"}
                          size="small"
                          onClick={() => handleTryThis(step)}
                          disabled={isCompleted || isSkipped}
                          className={`task-button ${
                            isCompleted
                              ? "completed"
                              : isSkipped
                              ? "skipped"
                              : "pending"
                          } ${isSkipped ? "skipped-task-button" : ""}`}
                        >
                          {isCompleted
                            ? "Completed"
                            : isSkipped
                            ? "Skipped"
                            : "Try This"}
                        </Button>
                      </div>
                    );
                  })}
                </div>
              )}
            </Space>
          </Card>
        </Space>
      </div>

      {/* Skip Confirmation Modal with custom positioning */}
      <Modal
        title={
          skipModalType === "onboarding"
            ? "Do you want to skip the onboarding?"
            : "Do you want to skip this task?"
        }
        open={showSkipModal}
        onOk={handleModalConfirm}
        onCancel={handleModalCancel}
        okText="Yes, Skip"
        cancelText="No, Continue"
        okButtonProps={{ danger: true }}
        width={400}
        getContainer={() =>
          document.querySelector(".chat-ai-container") || document.body
        }
        className="modal-container-style"
        wrapClassName="skip-confirmation-modal"
        centered={true}
        maskClassName="modal-mask-style"
      >
        {skipModalType === "onboarding" ? (
          <div>
            <p>
              The onboarding will stop and all remaining tasks will be marked as
              skipped.
            </p>
            <p>
              You can restart the full process anytime later from Reset
              Onboarding in the menu.
            </p>
          </div>
        ) : (
          <div>
            <p>This task will be marked as skipped and the prompt will stop.</p>
            <p>
              You can continue with other tasks or restart later from Reset
              Onboarding in the menu.
            </p>
          </div>
        )}
      </Modal>
    </>
  );
});

OnboardingGuide.propTypes = {
  visible: PropTypes.bool,
  config: PropTypes.object,
  onPromptSelect: PropTypes.func,
  completedSteps: PropTypes.instanceOf(Set),
  skippedSteps: PropTypes.instanceOf(Set),
  showWelcome: PropTypes.bool,
  compact: PropTypes.bool,
  currentStep: PropTypes.number,
  totalSteps: PropTypes.number,
  onSkip: PropTypes.func,
  hideModeTag: PropTypes.bool,
  collapsibleTodos: PropTypes.bool,
  onComplete: PropTypes.func,
  onSkipCurrentTask: PropTypes.func,
  currentOnboardingStep: PropTypes.object,
};

export { OnboardingGuide };
