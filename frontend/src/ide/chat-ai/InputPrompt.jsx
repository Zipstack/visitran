import { memo, useState, useCallback, useRef, useEffect, useMemo } from "react";
import PropTypes from "prop-types";
import { Space } from "antd";

import { PromptInput } from "./PromptInput";
import { PromptActions } from "./PromptActions";
import { MentionPopup } from "./MentionPopup";

const InputPrompt = memo(function InputPrompt({
  savePrompt,
  isNewChat = false,
  isPromptRunning,
  isResponseStreaming = false,
  chatIntents,
  selectedChatIntent,
  setSelectedChatIntent,
  llmModels = [],
  selectedLlmModel,
  setSelectedLlmModel,
  selectedCoderLlmModel,
  setSelectedCoderLlmModel,
  stopPromptRun = () => {},
  selectedChatId,
  lastChatMessageId,
  promptAutoComplete,
  isChatConversationDisabled,
  prefilledPrompt,
  shouldHighlightSend,
  isOnboardingMode = false,
  isTypingPrompt = false,
  disableSendDuringTyping = false,
  onSendButtonClick,
  onBuyTokens,
}) {
  // To **persist** the 'useMonaco' preference across sessions using localStorage,
  // uncomment the below code and remove the hardcoded default.
  // const [useMonaco, setUseMonaco] = useState(() => {
  //   const storedValue = window.localStorage.getItem("useMonaco");
  //   return storedValue === "true";
  // });

  // To **disable persistence** and always default to true (non-persistent behavior),
  // use the following line instead:
  const [useMonaco, setUseMonaco] = useState(true);

  const [value, setValue] = useState("");

  // Update value when prefilledPrompt changes
  useEffect(() => {
    if (
      prefilledPrompt !== undefined &&
      prefilledPrompt !== null &&
      prefilledPrompt !== ""
    ) {
      setValue(prefilledPrompt);
    } else if (prefilledPrompt === "") {
      setValue("");
    }
  }, [prefilledPrompt]);
  const [editorHeight, setEditorHeight] = useState(100);
  const editorRef = useRef(null);

  // Persist 'useMonaco' preference
  useEffect(() => {
    window.localStorage.setItem("useMonaco", useMonaco);
  }, [useMonaco]);

  const selectedChatIntentName = useMemo(() => {
    if (!selectedChatIntent) return null;

    return chatIntents.find(
      (intent) => intent?.chat_intent_id === selectedChatIntent
    )?.name;
  }, [chatIntents, selectedChatIntent]);

  const handleUseMonacoSwitch = useCallback(
    (checked) => {
      // Disable monaco switch during onboarding mode when typing
      if (isOnboardingMode && isTypingPrompt) {
        return;
      }
      setUseMonaco(checked);
    },
    [isOnboardingMode, isTypingPrompt]
  );

  const handleSubmit = useCallback(
    (prompt) => {
      setValue("");
      savePrompt(prompt, selectedChatIntent, isNewChat);
      if (useMonaco) setEditorHeight(100);
      // Stop send button animation when clicked during onboarding
      if (onSendButtonClick) {
        onSendButtonClick();
      }
    },
    [savePrompt, isNewChat, selectedChatIntent, useMonaco, onSendButtonClick]
  );

  const handleStop = useCallback(() => {
    stopPromptRun({
      chatId: selectedChatId,
      chatMessageId: lastChatMessageId,
    });
  }, [stopPromptRun, selectedChatId, lastChatMessageId]);

  const handleSenderChange = useCallback((val) => {
    setValue(val);
  }, []);

  const handleEditorMount = useCallback((editor) => {
    editorRef.current = editor;
  }, []);

  const handleMonacoChange = useCallback(() => {
    if (!editorRef.current) return;
    const model = editorRef.current.getModel();
    if (!model) return;

    const textValue = model.getValue();
    setValue(textValue);

    const lineCount = model.getLineCount();
    const approxLineHeight = 18;
    const contentHeight = lineCount * approxLineHeight;
    const clampedHeight = Math.min(200, Math.max(100, contentHeight));
    setEditorHeight(clampedHeight);
    editorRef.current.layout();
  }, []);

  // ── Mention popup state ──
  const [showMentionPopup, setShowMentionPopup] = useState(false);
  const [mentionSearchText, setMentionSearchText] = useState("");
  const mentionTriggerRef = useRef(null);

  const handleMentionTrigger = useCallback((position) => {
    mentionTriggerRef.current = position;
    setMentionSearchText("");
    setShowMentionPopup(true);
  }, []);

  const handleMentionDismiss = useCallback(() => {
    mentionTriggerRef.current = null;
    setMentionSearchText("");
    setShowMentionPopup(false);
  }, []);

  const handleMentionSelect = useCallback(
    (insertText) => {
      const editor = editorRef.current;
      if (!editor || !mentionTriggerRef.current) {
        handleMentionDismiss();
        return;
      }

      const { lineNumber, column } = mentionTriggerRef.current;
      const model = editor.getModel();
      const lineContent = model.getLineContent(lineNumber);

      // Find the @ and everything after it until whitespace
      const afterAt = lineContent.slice(column - 1);
      const matchLen = afterAt.match(/^@[^\s]*/)?.[0]?.length || 1;

      const range = {
        startLineNumber: lineNumber,
        startColumn: column,
        endLineNumber: lineNumber,
        endColumn: column + matchLen,
      };

      editor.executeEdits("mention-popup", [{ range, text: insertText + " " }]);

      const newCol = column + insertText.length + 1;
      editor.setPosition({ lineNumber, column: newCol });
      editor.focus();

      handleMentionDismiss();
    },
    [handleMentionDismiss]
  );

  // ── Auto-flip popup placement based on available space ──
  const containerRef = useRef(null);
  const [mentionPlacement, setMentionPlacement] = useState("top");

  useEffect(() => {
    if (!showMentionPopup || !containerRef.current) return;
    const rect = containerRef.current.getBoundingClientRect();
    // 320px is the popup maxHeight; flip to bottom if not enough space above
    setMentionPlacement(rect.top < 340 ? "bottom" : "top");
  }, [showMentionPopup]);

  return (
    <Space className="input-prompt-actions width-100" direction="vertical">
      <div ref={containerRef} style={{ position: "relative" }}>
        {showMentionPopup && (
          <MentionPopup
            dbData={promptAutoComplete.dbData}
            modelsData={promptAutoComplete.modelsData}
            seedsData={promptAutoComplete.seedsData}
            searchText={mentionSearchText}
            onSelect={handleMentionSelect}
            onClose={handleMentionDismiss}
            onSearchChange={setMentionSearchText}
            placement={mentionPlacement}
          />
        )}
        <PromptInput
          useMonaco={useMonaco}
          value={value}
          editorHeight={editorHeight}
          isPromptRunning={isPromptRunning}
          isResponseStreaming={isResponseStreaming}
          onSenderChange={handleSenderChange}
          onSubmit={handleSubmit}
          onEditorMount={handleEditorMount}
          onMonacoChange={handleMonacoChange}
          onCancel={handleStop}
          isChatConversationDisabled={
            isChatConversationDisabled || disableSendDuringTyping
          }
          shouldHighlightSend={shouldHighlightSend}
          onSendButtonClick={onSendButtonClick}
          onMentionTrigger={handleMentionTrigger}
          onMentionSearchChange={setMentionSearchText}
          onMentionDismiss={handleMentionDismiss}
          mentionTriggerRef={mentionTriggerRef}
        />
      </div>

      <PromptActions
        useMonaco={useMonaco}
        isNewChat={isNewChat}
        onUseMonacoSwitch={handleUseMonacoSwitch}
        chatIntents={chatIntents}
        selectedChatIntent={selectedChatIntent}
        setSelectedChatIntent={setSelectedChatIntent}
        llmModels={llmModels}
        selectedLlmModel={selectedLlmModel}
        setSelectedLlmModel={setSelectedLlmModel}
        selectedCoderLlmModel={selectedCoderLlmModel}
        setSelectedCoderLlmModel={setSelectedCoderLlmModel}
        selectedChatId={selectedChatId}
        selectedChatIntentName={selectedChatIntentName}
        isOnboardingMode={isOnboardingMode}
        isTypingPrompt={isTypingPrompt}
        onBuyTokens={onBuyTokens}
      />
    </Space>
  );
});

InputPrompt.propTypes = {
  savePrompt: PropTypes.func.isRequired,
  isNewChat: PropTypes.bool,
  isPromptRunning: PropTypes.bool.isRequired,
  isResponseStreaming: PropTypes.bool,
  chatIntents: PropTypes.array.isRequired,
  selectedChatIntent: PropTypes.string,
  setSelectedChatIntent: PropTypes.func.isRequired,
  llmModels: PropTypes.array,
  selectedLlmModel: PropTypes.string,
  setSelectedLlmModel: PropTypes.func.isRequired,
  selectedCoderLlmModel: PropTypes.string,
  setSelectedCoderLlmModel: PropTypes.func.isRequired,
  stopPromptRun: PropTypes.func,
  selectedChatId: PropTypes.string,
  lastChatMessageId: PropTypes.string,
  promptAutoComplete: PropTypes.object,
  isChatConversationDisabled: PropTypes.bool,
  prefilledPrompt: PropTypes.string,
  shouldHighlightSend: PropTypes.bool,
  disableSendDuringTyping: PropTypes.bool,
  isOnboardingMode: PropTypes.bool,
  isTypingPrompt: PropTypes.bool,
  onSendButtonClick: PropTypes.func,
  onBuyTokens: PropTypes.func,
};

InputPrompt.displayName = "InputPrompt";

export { InputPrompt };
