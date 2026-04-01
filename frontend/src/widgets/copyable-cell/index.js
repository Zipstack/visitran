import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import PropTypes from "prop-types";
import { Tooltip, theme } from "antd";
import { CopyOutlined, CheckOutlined } from "@ant-design/icons";

import "./copyable-cell.css";

const { useToken } = theme;
const MAX_TOOLTIP_LENGTH = 500;

function CopyableCell({ children, value, className = "" }) {
  const [copied, setCopied] = useState(false);
  const timeoutRef = useRef(null);
  const { token } = useToken();

  const textValue = value !== undefined && value !== null ? String(value) : "";

  // Cleanup timeout on unmount
  useEffect(() => {
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, []);

  const doCopy = useCallback(
    async (e) => {
      e.stopPropagation();
      if (!textValue) return;
      try {
        await navigator?.clipboard?.writeText(textValue);
        setCopied(true);
        if (timeoutRef.current) clearTimeout(timeoutRef.current);
        timeoutRef.current = setTimeout(() => setCopied(false), 2000);
      } catch {
        // silently fail – clipboard may not be available
      }
    },
    [textValue]
  );

  const overlayStyle = useMemo(
    () => ({
      border: `1px solid ${token.colorBorder}`,
      borderRadius: "8px",
    }),
    [token.colorBorder]
  );

  const copiedIconStyle = useMemo(
    () => ({ color: token.colorSuccess, fontSize: 14, padding: 2 }),
    [token.colorSuccess]
  );

  const copyIconStyle = useMemo(
    () => ({
      cursor: "pointer",
      color: token.colorTextSecondary,
      fontSize: 14,
      padding: 2,
    }),
    [token.colorTextSecondary]
  );

  if (!textValue) {
    return <div className={`copyable-cell ${className}`}>{children}</div>;
  }

  const truncatedValue =
    textValue.length > MAX_TOOLTIP_LENGTH
      ? textValue.slice(0, MAX_TOOLTIP_LENGTH) + "..."
      : textValue;

  const tooltipContent = (
    <div className="copyable-cell-tooltip">
      <span
        className="copyable-cell-tooltip-text"
        style={{ color: token.colorText }}
      >
        {truncatedValue}
      </span>
      {copied ? (
        <CheckOutlined style={copiedIconStyle} />
      ) : (
        <Tooltip title="Copy">
          <CopyOutlined style={copyIconStyle} onClick={doCopy} />
        </Tooltip>
      )}
    </div>
  );

  return (
    <Tooltip
      title={tooltipContent}
      placement="topLeft"
      color={token.colorBgElevated}
      overlayInnerStyle={overlayStyle}
      mouseEnterDelay={0.4}
      destroyTooltipOnHide
    >
      <div className={`copyable-cell ${className}`} onDoubleClick={doCopy}>
        {children}
      </div>
    </Tooltip>
  );
}

CopyableCell.propTypes = {
  children: PropTypes.node,
  value: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.number,
    PropTypes.bool,
  ]),
  className: PropTypes.string,
};

export { CopyableCell };
