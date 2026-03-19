import { memo, useCallback, useEffect, useRef } from "react";
import PropTypes from "prop-types";
import Editor from "@monaco-editor/react";
import { Button, Tooltip } from "antd";
import { CloseOutlined, SendOutlined } from "@ant-design/icons";

import { isMacOS } from "./helper";

/* ─────────────────── MonacoPromptInput ─────────────────── */

const MonacoPromptInput = memo(function MonacoPromptInput({
  value,
  editorHeight,
  isPromptRunning,
  onEditorMount,
  onMonacoChange,
  onSubmit,
  onCancel,
  userDetails,
  isChatConversationDisabled,
  shouldHighlightSend,
  onMentionTrigger,
  onMentionSearchChange,
  onMentionDismiss,
  mentionTriggerRef,
}) {
  const editorRef = useRef(null);
  const disposableRef = useRef(null);

  const onSubmitRef = useRef(onSubmit);
  useEffect(() => {
    onSubmitRef.current = onSubmit;
  }, [onSubmit]);

  const isRunningRef = useRef(isPromptRunning);
  useEffect(() => {
    isRunningRef.current = isPromptRunning;
  }, [isPromptRunning]);

  const onMentionTriggerRef = useRef(onMentionTrigger);
  useEffect(() => {
    onMentionTriggerRef.current = onMentionTrigger;
  }, [onMentionTrigger]);

  const onMentionSearchChangeRef = useRef(onMentionSearchChange);
  useEffect(() => {
    onMentionSearchChangeRef.current = onMentionSearchChange;
  }, [onMentionSearchChange]);

  const onMentionDismissRef = useRef(onMentionDismiss);
  useEffect(() => {
    onMentionDismissRef.current = onMentionDismiss;
  }, [onMentionDismiss]);

  const handleSubmit = useCallback(() => {
    onSubmit(value);
  }, [onSubmit, value]);

  // Clean up disposable on unmount
  useEffect(() => {
    return () => {
      disposableRef.current?.dispose();
    };
  }, []);

  const handleEditorMount = useCallback(
    (editor, monaco) => {
      editorRef.current = editor;
      onEditorMount?.(editor, monaco);

      // Ctrl/Cmd + Enter to submit
      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
        if (!isRunningRef.current) {
          onSubmitRef.current(editor.getValue());
        }
      });

      // Dispose previous listener if any (safety for StrictMode double-mount)
      disposableRef.current?.dispose();

      // Listen for content changes to detect @ trigger
      disposableRef.current = editor.onDidChangeModelContent((e) => {
        const changes = e.changes;
        if (!changes || changes.length === 0) return;

        const lastChange = changes[changes.length - 1];
        const insertedText = lastChange.text;

        // Check if user typed @
        if (insertedText === "@") {
          const position = editor.getPosition();
          onMentionTriggerRef.current({
            lineNumber: position.lineNumber,
            column: position.column - 1,
          });
          return;
        }

        // If mention popup is open, update search text from editor content
        if (mentionTriggerRef?.current) {
          const { lineNumber, column } = mentionTriggerRef.current;
          const model = editor.getModel();
          const lineContent = model.getLineContent(lineNumber);
          const cursorPos = editor.getPosition();

          if (
            cursorPos.lineNumber === lineNumber &&
            cursorPos.column > column
          ) {
            const textAfterAt = lineContent.slice(column, cursorPos.column - 1);
            if (textAfterAt.includes(" ") || textAfterAt.includes("\n")) {
              onMentionDismissRef.current();
            } else {
              onMentionSearchChangeRef.current(textAfterAt);
            }
          } else {
            onMentionDismissRef.current();
          }
        }
      });
    },
    [onEditorMount, mentionTriggerRef]
  );

  const editorContent = (
    <div className="monaco-editor-container monaco-editor-loading-font-color">
      <Editor
        height={`${editorHeight}px`}
        defaultLanguage="markdown"
        value={value}
        onMount={handleEditorMount}
        onChange={onMonacoChange}
        theme={userDetails?.currentTheme === "dark" ? "vs-dark" : "vs-light"}
        options={{
          lineNumbers: "off",
          readOnly: isPromptRunning || shouldHighlightSend,
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          automaticLayout: true,
          wordWrap: "on",
          wrappingIndent: "same",
          fixedOverflowWidgets: true,
          renderLineHighlight: "none",
          placeholder:
            "Describe what you want to build, analyze, or transform...",
          scrollbar: {
            horizontal: "hidden",
            vertical: "auto",
            horizontalScrollbarSize: 0,
          },
          overviewRulerLanes: 0,
          quickSuggestions: false,
          suggestOnTriggerCharacters: false,
        }}
      />
    </div>
  );

  return (
    <div className="monaco-editor-wrapper">
      {editorContent}

      <div className="monaco-editor-send">
        {isPromptRunning ? (
          <Tooltip title="Stop">
            <Button type="text" icon={<CloseOutlined />} onClick={onCancel} />
          </Tooltip>
        ) : (
          <Tooltip
            title={
              isChatConversationDisabled
                ? ""
                : value
                ? `${isMacOS() ? "⌘ + Enter" : "Ctrl + Enter"}`
                : "Please type something"
            }
          >
            <Button
              type="text"
              icon={
                <SendOutlined
                  style={
                    shouldHighlightSend
                      ? {
                          color: "#1890ff",
                          animation: "heartbeat-icon 1.2s ease-in-out infinite",
                        }
                      : {}
                  }
                />
              }
              disabled={!value}
              onClick={handleSubmit}
              className={shouldHighlightSend ? "onboarding-send-highlight" : ""}
            />
          </Tooltip>
        )}
      </div>
    </div>
  );
});

MonacoPromptInput.propTypes = {
  value: PropTypes.string.isRequired,
  editorHeight: PropTypes.number.isRequired,
  isPromptRunning: PropTypes.bool.isRequired,
  onEditorMount: PropTypes.func,
  onMonacoChange: PropTypes.func.isRequired,
  onSubmit: PropTypes.func.isRequired,
  onCancel: PropTypes.func.isRequired,
  userDetails: PropTypes.object,
  isChatConversationDisabled: PropTypes.bool,
  shouldHighlightSend: PropTypes.bool,
  onMentionTrigger: PropTypes.func,
  onMentionSearchChange: PropTypes.func,
  onMentionDismiss: PropTypes.func,
  mentionTriggerRef: PropTypes.object,
};

export { MonacoPromptInput };
