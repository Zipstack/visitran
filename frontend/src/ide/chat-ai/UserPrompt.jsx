import { memo, useState } from "react";
import PropTypes from "prop-types";
import { Bubble } from "@ant-design/x";
import { Button, message } from "antd";
import { CopyOutlined } from "@ant-design/icons";

import { useSessionStore } from "../../store/session-store";

const UserPrompt = memo(function UserPrompt({ prompt, user }) {
  const [isHovered, setIsHovered] = useState(false);
  const { sessionDetails } = useSessionStore();
  const email = user?.email || sessionDetails?.email || "";
  const avatarText = email ? email.slice(0, 2).toUpperCase() : "G";

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(prompt?.trim() || "");
      message.success("Copied to clipboard!");
    } catch (err) {
      message.error("Failed to copy text");
    }
  };

  const bubbleContent = (
    <div className="user-prompt-bubble-content">{prompt?.trim() || ""}</div>
  );

  return (
    <div
      className="user-prompt-container"
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <Bubble
        placement="end"
        content={bubbleContent}
        className="user-prompt-bubble"
        avatar={{
          icon: avatarText,
          className: "user-prompt-avatar",
        }}
      />
      <div className="user-prompt-actions">
        {isHovered && (
          <Button type="text" icon={<CopyOutlined />} onClick={handleCopy} />
        )}
      </div>
    </div>
  );
});

UserPrompt.propTypes = {
  prompt: PropTypes.string,
  user: PropTypes.shape({
    user_id: PropTypes.string,
    email: PropTypes.string,
    profile_picture_url: PropTypes.string,
  }),
};

UserPrompt.displayName = "UserPrompt";

export { UserPrompt };
