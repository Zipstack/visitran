import { BookOutlined, SlackOutlined } from "@ant-design/icons";

import "./DocsFooter.css";

function DocsFooter() {
  return (
    <div className="docs-footer">
      <a
        href="https://docs.visitran.com"
        target="_blank"
        rel="noopener noreferrer"
        className="docs-footer-link"
      >
        <BookOutlined /> Documentation
      </a>
      <span className="docs-footer-dot" />
      <a
        href="https://visitran.slack.com"
        target="_blank"
        rel="noopener noreferrer"
        className="docs-footer-link"
      >
        <SlackOutlined /> Community
      </a>
    </div>
  );
}

export { DocsFooter };
