import { memo, useEffect } from "react";
import PropTypes from "prop-types";
import { Space } from "antd";

import { InputPrompt } from "./InputPrompt";
import { PastConversations } from "./PastConversations";
import { WelcomeBanner } from "./WelcomeBanner";
import { LowBalanceWarning } from "./LowBalanceWarning";
import { OnboardingGuide } from "./OnboardingGuide";

const NewChat = memo(function NewChat({
  isChatDrawerOpen,
  setSelectedChatId,
  setChatName,
  savePrompt,
  triggerGetChatMessagesApi,
  isPromptRunning,
  chatIntents,
  selectedChatIntent,
  setSelectedChatIntent,
  llmModels = [],
  selectedLlmModel,
  setSelectedLlmModel,
  selectedCoderLlmModel,
  setSelectedCoderLlmModel,
  promptAutoComplete,
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
  useEffect(() => {
    setSelectedChatIntent(null);
    setSelectedLlmModel(null);
    setSelectedCoderLlmModel(null);
  }, []);

  return (
    <div className="height-100 overflow-y-auto pad-8">
      <Space
        direction="vertical"
        className="width-100"
        size={isOnboardingMode ? 5 : 20}
        style={{ marginTop: isOnboardingMode ? "-20px" : "0" }}
      >
        <WelcomeBanner isOnboardingMode={isOnboardingMode} />

        <LowBalanceWarning tokenUsageData={tokenUsageData} />

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
        />

        <InputPrompt
          isChatDrawerOpen={isChatDrawerOpen}
          savePrompt={savePrompt}
          isNewChat
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
          prefilledPrompt={selectedPrompt}
          shouldHighlightSend={
            !!(isOnboardingMode && !isTypingPrompt && selectedPrompt)
          }
          isOnboardingMode={isOnboardingMode}
          isTypingPrompt={isTypingPrompt}
          disableSendDuringTyping={isOnboardingMode && isTypingPrompt}
          onSendButtonClick={onSendButtonClick}
        />

        <PastConversations
          isChatDrawerOpen={isChatDrawerOpen}
          setSelectedChatId={setSelectedChatId}
          setChatName={setChatName}
          chatIntents={chatIntents}
          triggerGetChatMessagesApi={triggerGetChatMessagesApi}
          setSelectedChatIntent={setSelectedChatIntent}
          setSelectedLlmModel={setSelectedLlmModel}
          setSelectedCoderLlmModel={setSelectedCoderLlmModel}
        />
      </Space>
    </div>
  );
});

NewChat.propTypes = {
  isChatDrawerOpen: PropTypes.bool.isRequired,
  setSelectedChatId: PropTypes.func.isRequired,
  setChatName: PropTypes.func.isRequired,
  savePrompt: PropTypes.func.isRequired,
  triggerGetChatMessagesApi: PropTypes.func.isRequired,
  isPromptRunning: PropTypes.bool.isRequired,
  chatIntents: PropTypes.array,
  selectedChatIntent: PropTypes.oneOfType([PropTypes.object, PropTypes.string]),
  setSelectedChatIntent: PropTypes.func.isRequired,
  llmModels: PropTypes.array,
  selectedLlmModel: PropTypes.oneOfType([PropTypes.object, PropTypes.string]),
  setSelectedLlmModel: PropTypes.func.isRequired,
  selectedCoderLlmModel: PropTypes.oneOfType([
    PropTypes.object,
    PropTypes.string,
  ]),
  setSelectedCoderLlmModel: PropTypes.func.isRequired,
  promptAutoComplete: PropTypes.object,
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

NewChat.displayName = "NewChat";

export { NewChat };
