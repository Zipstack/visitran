import { memo, useMemo } from "react";
import PropTypes from "prop-types";
import { Alert, Button, Space } from "antd";
import { ToolOutlined } from "@ant-design/icons";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

/**
 * Shows socket / prompt / transformation errors as Markdown‑rendered alerts.
 * Rerenders only when any of the three error values change.
 *
 * Backend sends `is_warning: true` for non-critical errors (empty keys,
 * insufficient credits, temporary issues) — these render as orange warnings.
 * Auth failures and other errors render as red error alerts.
 */
const DisplayErrorMessages = memo(function DisplayErrorMessages({
  socketError,
  promptError,
  transformError,
  onTroubleshoot,
}) {
  const errors = useMemo(() => {
    const list = [];
    if (socketError)
      list.push({
        key: "socket",
        text: socketError.error_message,
        isWarning: socketError.is_warning || socketError.is_credit_error,
      });
    if (promptError?.error_message)
      list.push({ key: "prompt", text: promptError.error_message });
    if (transformError?.error_message)
      list.push({ key: "transform", text: transformError.error_message });
    return list;
  }, [socketError, promptError?.error_message, transformError?.error_message]);

  if (!errors.length) return null; // nothing to display

  return (
    <Space direction="vertical" className="width-100">
      {errors.map(({ key, text, isWarning }) => {
        // "Prompt Stopped" messages are also warnings
        const showAsWarning =
          isWarning || (text && text.includes("Prompt Stopped"));

        return (
          <div key={key}>
            <Alert
              message={
                <div className="react-markdown-container">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>
                    {text}
                  </ReactMarkdown>
                </div>
              }
              type={showAsWarning ? "warning" : "error"}
              className="width-100"
              description={
                !showAsWarning && onTroubleshoot ? (
                  <Button
                    type="link"
                    size="small"
                    icon={<ToolOutlined />}
                    onClick={() => onTroubleshoot(text)}
                    style={{ padding: 0, marginTop: 8 }}
                  >
                    Troubleshoot this
                  </Button>
                ) : null
              }
            />
          </div>
        );
      })}
    </Space>
  );
});

DisplayErrorMessages.propTypes = {
  socketError: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.shape({
      error_message: PropTypes.string,
      error_code: PropTypes.number,
      is_credit_error: PropTypes.bool,
      is_warning: PropTypes.bool,
    }),
  ]),
  promptError: PropTypes.shape({
    error_message: PropTypes.string,
  }),
  transformError: PropTypes.shape({
    error_message: PropTypes.string,
  }),
  onTroubleshoot: PropTypes.func,
};

DisplayErrorMessages.defaultProps = {
  socketError: null,
  promptError: null,
  transformError: null,
  onTroubleshoot: null,
};

export { DisplayErrorMessages };
