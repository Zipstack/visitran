import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Modal, Typography, Checkbox } from "antd";

const MODAL_WIDTH = 500;

function DeleteModal({
  contextMenu,
  onDelete = () => {},
  onClose = () => {},
  loading,
  checked,
  setChecked,
}) {
  const [openModal, setOpenModal] = useState(true);

  function onCancel() {
    setOpenModal(false);
    onClose();
  }
  function onDeleteClick() {
    onDelete(contextMenu, contextMenu.title);
  }
  const handleCheckedChange = () => setChecked(!checked);
  function getDeleteDialogContent(contextMenu) {
    const isAll = contextMenu.type === "all";
    const isModel = contextMenu.contextMenuKey === "nc_delete_model";

    let title;
    let message;

    if (isModel) {
      const isMultiple = contextMenu.type === "multiple";
      if (isAll) {
        title = "Delete All Models";
      } else if (isMultiple) {
        title = "Delete Selected Models";
      } else {
        title = "Delete Model";
      }
      message = (
        <>
          <Typography.Title level={5}>
            {isAll ? (
              "Are you sure you want to delete all models in this project?"
            ) : isMultiple ? (
              `Are you sure you want to delete ${contextMenu.key?.length} selected models?`
            ) : (
              <>
                Are you sure you want to delete{" "}
                <span className="red">{contextMenu.title}</span> model?
              </>
            )}
          </Typography.Title>

          <Checkbox checked={checked} onChange={handleCheckedChange}>
            Also delete associated {isAll || isMultiple ? "tables" : "table"}
            {!isAll && !isMultiple && " if it's not referenced in any model"}
          </Checkbox>
        </>
      );
    } else {
      const isMultiple = contextMenu.type === "multiple";
      if (isAll) {
        title = "Delete All Seeds";
        message = "Are you sure you want to delete all seeds?";
      } else if (isMultiple) {
        title = "Delete Selected Seeds";
        message = `Are you sure you want to delete ${contextMenu.key?.length} selected seeds?`;
      } else {
        title = "Delete Seed";
        message = (
          <Typography>
            Are you sure you want to delete{" "}
            <span className="red">{contextMenu.title}</span> seed?
          </Typography>
        );
      }
    }

    return { title, message };
  }

  useEffect(() => {
    if (openModal) {
      setChecked(true);
    }
  }, [openModal]);

  const { title, message } = getDeleteDialogContent(contextMenu);
  return (
    <Modal
      title={title}
      open={openModal}
      onOk={onDeleteClick}
      onCancel={onCancel}
      okType="danger"
      okText={
        contextMenu.type === "all"
          ? "Delete All"
          : contextMenu.type === "multiple"
          ? `Delete (${contextMenu.key?.length})`
          : "Delete"
      }
      width={MODAL_WIDTH}
      okButtonProps={{ disabled: loading }}
      centered
      maskClosable={false}
    >
      {message}
    </Modal>
  );
}

DeleteModal.propTypes = {
  contextMenu: PropTypes.object.isRequired,
  onDelete: PropTypes.func,
  onClose: PropTypes.func,
  loading: PropTypes.bool,
  checked: PropTypes.bool,
  setChecked: PropTypes.func,
};

export { DeleteModal };
