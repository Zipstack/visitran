import { memo } from "react";
import PropTypes from "prop-types";

const StartBanner = memo(function StartBanner({
  onOpenModal: _onOpenModal,
  onActivateExp: _onActivateExp,
}) {
  // Content commented out - AI drawer now uses this space in full width mode
  // Return empty div - AI drawer takes over this space
  return <div className="height-100 width-100" />;
});

StartBanner.propTypes = {
  onOpenModal: PropTypes.func.isRequired,
  onActivateExp: PropTypes.func.isRequired,
};

export { StartBanner };
