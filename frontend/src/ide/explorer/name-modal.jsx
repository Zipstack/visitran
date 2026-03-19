import { useState, useRef, useEffect } from "react";
import PropTypes from "prop-types";
import { Modal, Input, Typography } from "antd";

const MODAL_WIDTH = 400;

function NameModal({
  name = "",
  contextMenu,
  onDone = () => {},
  onClose = () => {},
}) {
  const [openModal, setOpenModal] = useState(true);
  const [allowNaming, setAllowNaming] = useState(true);
  const [inputValue, setInputValue] = useState(name || "");
  const [showHint, setShowHint] = useState(false);
  const [loading, setLoading] = useState(false);
  const inputRef = useRef();

  useEffect(function autoFocus() {
    name ? inputRef.current.focus({ cursor: "all" }) : inputRef.current.focus();
  }, []);

  function updateName({ target: { value } }) {
    const validValue = value.replace(/[^a-zA-Z_]/g, "");

    setInputValue(validValue);
    setShowHint(value !== validValue);

    if (validValue === "" || validValue === name) {
      setAllowNaming(true);
      return;
    }
    setAllowNaming(false);
  }

  function onCancel() {
    setOpenModal(false);
    onClose();
  }
  function onCreate() {
    setLoading(true);
    onDone(contextMenu, inputValue, name);
    setTimeout(() => {
      setLoading(false);
      onCancel();
    }, 400);
  }

  return (
    <Modal
      title={name ? "Rename" : "Create"}
      open={openModal}
      onCancel={onCancel}
      onOk={onCreate}
      maskClosable={false}
      okText={name ? "Rename" : "Create"}
      width={MODAL_WIDTH}
      okButtonProps={{ disabled: allowNaming, loading: loading }}
      centered
    >
      <Input
        ref={inputRef}
        value={inputValue}
        onChange={updateName}
        defaultValue={name}
        placeholder="Name"
        onPressEnter={!loading ? onCreate : undefined}
        maxLength={100}
      />
      <div
        style={{
          minHeight: "20px",
        }}
      >
        <Typography.Text
          type="danger"
          className="hint"
          style={{
            visibility: showHint ? "visible" : "hidden",
          }}
        >
          *Only alphabhets and underscores are allowed.
        </Typography.Text>
      </div>
    </Modal>
  );
}

NameModal.propTypes = {
  name: PropTypes.string,
  contextMenu: PropTypes.object.isRequired,
  onDone: PropTypes.func,
  onClose: PropTypes.func,
};

export { NameModal };
