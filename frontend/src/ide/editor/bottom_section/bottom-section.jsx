import PropTypes from "prop-types";

import { RESIZE_AXIS, ResizerComponent } from "../../../widgets/resizer";

function EditorBottomSection({ children }) {
  return (
    <div className="editorBottomSection">
      <ResizerComponent
        axis={RESIZE_AXIS.Y}
        width="100%"
        style={{ overflow: "hidden" }}
        height="200px"
      >
        {children}
      </ResizerComponent>
    </div>
  );
}

EditorBottomSection.propTypes = {
  children: PropTypes.element,
};

export { EditorBottomSection };
