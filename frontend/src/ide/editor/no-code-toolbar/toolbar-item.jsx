import { useRef, useState } from "react";
import { Button, Modal } from "antd";
import PropTypes from "prop-types";
import Draggable from "react-draggable";

import { generateKey } from "../../../common/helpers";
// CSS for this component is added in the parent component's CSS file (no-code-model)
function ToolbarItem({
  icon,
  label,
  open,
  children,
  className,
  disabled,
  handleOpenChange,
  step,
}) {
  const [bounds, setBounds] = useState({
    left: 0,
    top: 0,
    bottom: 0,
    right: 0,
  });
  const dragRef = useRef(null);

  const onStart = (_event, uiData) => {
    const { clientWidth, clientHeight } = window.document.documentElement;
    const targetRect = dragRef.current?.getBoundingClientRect();
    if (!targetRect) return;

    setBounds({
      left: -targetRect.left + uiData.x,
      right: clientWidth - (targetRect.right - uiData.x),
      top: -targetRect.top + uiData.y,
      bottom: clientHeight - (targetRect.bottom - uiData.y),
    });
  };

  return (
    <div>
      <div className="toolbar-item p-0">
        <div className="p-0">
          <div className="seq_badge_wrapper">
            {step?.map((el, index) =>
              el !== null && el !== undefined && el !== 0 ? (
                <div className="seq_badge" key={generateKey()}>
                  {el}
                </div>
              ) : null
            )}
          </div>
          <Button
            type="text"
            className={className}
            disabled={disabled}
            onClick={() => handleOpenChange(true)}
            icon={icon}
          >
            {label}
          </Button>
        </div>
        <Modal
          height="auto"
          open={open && !disabled}
          onCancel={() => handleOpenChange(false)}
          footer={null}
          destroyOnClose
          title={null}
          classNames="toolbar-item-modal"
          modalRender={(modal) => (
            <Draggable
              bounds={bounds}
              onStart={(event, uiData) => onStart(event, uiData)}
              handle=".draggable-title"
            >
              <div ref={dragRef}>{modal}</div>
            </Draggable>
          )}
          width="auto"
          centered
          className="mt-40"
          wrapClassName="draggable-modal"
          maskClosable={false}
        >
          {children}
        </Modal>
      </div>
    </div>
  );
}

ToolbarItem.propTypes = {
  icon: PropTypes.node.isRequired,
  label: PropTypes.string.isRequired,
  open: PropTypes.bool.isRequired,
  children: PropTypes.node.isRequired,
  className: PropTypes.string,
  disabled: PropTypes.bool.isRequired,
  handleOpenChange: PropTypes.func.isRequired,
  step: PropTypes.array,
};

ToolbarItem.defaultProps = {
  className: "",
};

export { ToolbarItem };
