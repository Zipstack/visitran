import { memo } from "react";
import PropTypes from "prop-types";
import { Typography, Tag, Divider } from "antd";

const ConnectionUsageSection = memo(({ dbUsage, connectionId }) => {
  if (!connectionId) return null;

  return (
    <>
      <Divider />
      <div className="connectionUsageSection">
        <Typography className="sectionTitle">Connection Usage</Typography>
        <div className="usageWrapper">
          <div className="usageRow">
            <Typography className="usageLabel">Environments:</Typography>
            <div className="usageTags">
              {dbUsage.environment.map((el) => (
                <Tag color="processing" className="usageTag" key={el.name}>
                  {el.name}
                </Tag>
              ))}
            </div>
          </div>
          <div className="usageRow">
            <Typography className="usageLabel">Projects:</Typography>
            <div className="usageTags">
              {dbUsage.projects.map((el) => (
                <Tag color="success" className="usageTag" key={el.name}>
                  {el.name}
                </Tag>
              ))}
            </div>
          </div>
        </div>
      </div>
    </>
  );
});

ConnectionUsageSection.propTypes = {
  dbUsage: PropTypes.shape({
    projects: PropTypes.array.isRequired,
    environment: PropTypes.array.isRequired,
  }).isRequired,
  connectionId: PropTypes.string,
};

ConnectionUsageSection.defaultProps = {
  connectionId: "",
};

ConnectionUsageSection.displayName = "ConnectionUsageSection";

export default ConnectionUsageSection;
