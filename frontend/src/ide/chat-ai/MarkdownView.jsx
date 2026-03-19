import { memo, useMemo } from "react";
import PropTypes from "prop-types";
import { Bubble } from "@ant-design/x";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";
import { vs } from "react-syntax-highlighter/dist/esm/styles/prism";
import { Tooltip, Button, Space } from "antd";
import { CopyOutlined } from "@ant-design/icons";

import { BlinkingLoader } from "./BlinkingLoader";

const MarkdownView = memo(function MarkdownView({
  markdownChunks = [],
  shouldStream = false,
  currentTheme,
  onActionDetected,
}) {
  const combinedText = useMemo(() => {
    const rawText = markdownChunks.join("");

    // Check for ACTION markers before removing them
    let detectedAction = null;
    if (rawText.includes("ACTION: SHOW_PROCEED_BUTTON")) {
      detectedAction = "proceed";
    } else if (rawText.includes("ACTION: SHOW_BUILD_MODELS_BUTTON")) {
      detectedAction = "build_models";
    } else if (rawText.includes("ACTION: SHOW_RUN_SQL_BUTTON")) {
      detectedAction = "run_sql";
    } else if (rawText.includes("ACTION: SHOW_APPLY_BUTTON")) {
      detectedAction = "apply";
    } else if (rawText.includes("MOVE_CONTROL_TO_BUILDER")) {
      detectedAction = "build_models";
    }

    // Notify parent about detected action if callback is provided
    if (detectedAction && onActionDetected && !shouldStream) {
      onActionDetected(detectedAction);
    }

    // Remove ACTION markers that are used for UI control
    return rawText
      .replace(/ACTION:\s*SHOW_PROCEED_BUTTON/g, "")
      .replace(/ACTION:\s*SHOW_BUILD_MODELS_BUTTON/g, "")
      .replace(/ACTION:\s*SHOW_RUN_SQL_BUTTON/g, "")
      .replace(/ACTION:\s*SHOW_APPLY_BUTTON/g, "")
      .replace(/MOVE_CONTROL_TO_BUILDER/g, "");
  }, [markdownChunks, onActionDetected, shouldStream]);

  const syntaxTheme = currentTheme === "dark" ? oneDark : vs;

  const customComponents = useMemo(() => {
    return {
      code({ inline, className, children, ...props }) {
        const match = /language-(\w+)/.exec(className || "");
        if (match) {
          const codeString = String(children).replace(/\n$/, "");

          return (
            <div className="chat-ai-markdown-code-wrapper">
              <Tooltip title="Copy to clipboard">
                <Button
                  type="text"
                  size="small"
                  icon={<CopyOutlined />}
                  className="chat-ai-markdown-copy-btn"
                  onClick={() => navigator.clipboard.writeText(codeString)}
                />
              </Tooltip>

              <SyntaxHighlighter
                style={syntaxTheme}
                language={match[1] || "text"}
                PreTag="div"
                className="chat-ai-markdown-code-block"
                codeTagProps={{ className: "chat-ai-markdown-code-tag" }}
                {...props}
              >
                {codeString}
              </SyntaxHighlighter>
            </div>
          );
        }

        return (
          <code {...props} className={`${className} chat-ai-inline-code`}>
            {children}
          </code>
        );
      },
    };
  }, [syntaxTheme]);

  const renderMarkdown = () => (
    <div className="width-100 .react-markdown-container">
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={customComponents}>
        {combinedText}
      </ReactMarkdown>
      {shouldStream && (
        <div className="width-100">
          <Space className="align-items-center">
            <BlinkingLoader />
          </Space>
        </div>
      )}
    </div>
  );

  return (
    <Bubble
      typing={shouldStream ? { step: 10, interval: 10 } : false}
      content={combinedText}
      messageRender={renderMarkdown}
      variant="borderless"
      className="width-100"
    />
  );
});

MarkdownView.propTypes = {
  markdownChunks: PropTypes.arrayOf(PropTypes.string),
  shouldStream: PropTypes.bool,
  currentTheme: PropTypes.string,
  onActionDetected: PropTypes.func,
};

MarkdownView.displayName = "MarkdownView";

export { MarkdownView };
