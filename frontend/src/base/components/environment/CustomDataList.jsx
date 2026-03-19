import { memo } from "react";
import PropTypes from "prop-types";
import { Space, Button, Input } from "antd";
import { DeleteOutlined, PlusOutlined } from "@ant-design/icons";

const CustomDataList = memo(
  ({
    customData,
    actionState,
    handleCustomFieldChange,
    handleDelete,
    AddnewEntry,
    disabledAddCustomBtn,
  }) => {
    return (
      <>
        {!customData.length ? (
          <Button
            onClick={AddnewEntry}
            className="noPaddingBtn"
            icon={<PlusOutlined />}
            type="link"
            disabled={actionState === "view"}
          >
            Add Custom Data
          </Button>
        ) : (
          <Space direction="vertical">
            {customData.map((item, index) => (
              <Space key={item.id} size={15}>
                <Space>
                  <Input
                    className="w-150px"
                    placeholder="Source Schema"
                    value={item.source_schema}
                    disabled={actionState === "view"}
                    onChange={(e) =>
                      handleCustomFieldChange(
                        e.target.value,
                        "source_schema",
                        item.id
                      )
                    }
                  />
                  <Input
                    className="w-150px"
                    placeholder="Destination Schema"
                    value={item.destination_schema}
                    disabled={actionState === "view"}
                    onChange={(e) =>
                      handleCustomFieldChange(
                        e.target.value,
                        "destination_schema",
                        item.id
                      )
                    }
                  />
                </Space>
                <Space>
                  <Button
                    onClick={() => handleDelete(item.id)}
                    danger
                    disabled={actionState === "view"}
                    icon={<DeleteOutlined />}
                  />
                  {index === customData.length - 1 && (
                    <Button
                      onClick={AddnewEntry}
                      icon={<PlusOutlined />}
                      disabled={actionState === "view" || disabledAddCustomBtn}
                    />
                  )}
                </Space>
              </Space>
            ))}
          </Space>
        )}
      </>
    );
  }
);

CustomDataList.displayName = "CustomDataList";

CustomDataList.propTypes = {
  customData: PropTypes.arrayOf(
    PropTypes.shape({
      id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]).isRequired,
      source_schema: PropTypes.string,
      destination_schema: PropTypes.string,
    })
  ).isRequired,
  actionState: PropTypes.string.isRequired,
  handleCustomFieldChange: PropTypes.func.isRequired,
  handleDelete: PropTypes.func.isRequired,
  AddnewEntry: PropTypes.func.isRequired,
  disabledAddCustomBtn: PropTypes.bool.isRequired,
};

export default CustomDataList;
