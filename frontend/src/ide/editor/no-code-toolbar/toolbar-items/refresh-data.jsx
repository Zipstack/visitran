import { Button, Tooltip } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";

// CSS for this component is added in the parent component's CSS file (no-code-model)
function RefreshData({ refresh, isLoading, disabled }) {
  return (
    <Tooltip title={isLoading ? "Loading..." : "Refresh Data"}>
      <div className="reload_icon_wrap">
        <Button
          type="text"
          disabled={disabled}
          onClick={refresh}
          loading={isLoading}
          icon={
            <ReloadOutlined
              style={{
                color: "var(--icons-color)",
              }}
            />
          }
        />
      </div>
    </Tooltip>
  );
}

RefreshData.propTypes = {
  refresh: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  disabled: PropTypes.bool.isRequired,
};

export { RefreshData };
