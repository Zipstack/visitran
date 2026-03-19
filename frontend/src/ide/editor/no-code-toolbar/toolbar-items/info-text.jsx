import { InfoCircleOutlined } from "@ant-design/icons";
import { Typography } from "antd";
import PropTypes from "prop-types";

const matchTypeInfo = {
  TEXT: "Matches exact text content (case-sensitive).",
  EXACT_TEXT: "Matches the entire cell exactly as entered.",
  EMPTY: "Targets cells that have no value or only whitespace.",
  LETTERS: "Matches alphabetic characters (A–Z, a–z).",
  DIGITS: "Matches numeric characters (0–9).",
  SYMBOLS: "Matches non-alphanumeric special characters (!, @, #, etc.).",
  WHITESPACE: "Matches spaces, tabs, or line breaks.",
  CURRENCY: "Matches common currency symbols like $, €, ₹, etc.",
  PUNCTUATION: "Matches punctuation marks such as commas or periods.",
  REGEX: (
    <>
      Use regular expressions (e.g. <code>^start</code>, <code>\d+</code>). Be
      careful with escaping special characters.
    </>
  ),
  FILL_NULL: "Replaces null or missing values with the specified text.",
};

const FindReplaceInfoText = ({ type }) => {
  const text = matchTypeInfo[type];
  if (!text) return null;

  return (
    <Typography.Text
      type="secondary"
      style={{
        fontSize: 12,
        marginTop: 8,
        display: "flex",
        alignItems: "center",
      }}
    >
      <InfoCircleOutlined style={{ marginRight: 6, color: "#1677ff" }} />
      {text}
    </Typography.Text>
  );
};

FindReplaceInfoText.propTypes = {
  type: PropTypes.string.isRequired,
};

export default FindReplaceInfoText;
