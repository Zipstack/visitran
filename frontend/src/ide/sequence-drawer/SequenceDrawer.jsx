import { memo } from "react";
import PropTypes from "prop-types";

import { Header } from "./Header";
import { Body } from "./Body";

const SequenceDrawer = memo(function SequenceDrawer({
  isSequenceDrawerOpen,
  closeSequenceDrawer,
}) {
  return (
    <div className="chat-ai-container">
      <Header closeSequenceDrawer={closeSequenceDrawer} />
      <div className="flex-1 overflow-hidden">
        <Body isSequenceDrawerOpen={isSequenceDrawerOpen} />
      </div>
    </div>
  );
});

SequenceDrawer.propTypes = {
  isSequenceDrawerOpen: PropTypes.bool.isRequired,
  closeSequenceDrawer: PropTypes.func.isRequired,
};

SequenceDrawer.displayName = "SequenceDrawer";

export { SequenceDrawer };
