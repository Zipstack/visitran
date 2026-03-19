import { memo } from "react";
import PropTypes from "prop-types";
import { Typography, Tag, Divider } from "antd";

const EnvironmentUsage = memo(({ id, projListDep }) => {
  if (!id) return null;

  return (
    <>
      <Divider className="divider-modal" />
      <div>
        <Typography.Title level={5} className="sectionTitle">
          Environment Usage
        </Typography.Title>
        <div className="flex-center mt10">
          <Typography className="w100">Projects:</Typography>
          <div className="flex1">
            {projListDep.length > 0 ? (
              projListDep.map((el) => (
                <Tag color="success" className="mx5" key={el.name}>
                  {el.name}
                </Tag>
              ))
            ) : (
              <Tag className="mx5" key="Not Assigned">
                Not Assigned
              </Tag>
            )}
          </div>
        </div>
      </div>
    </>
  );
});

EnvironmentUsage.propTypes = {
  id: PropTypes.string.isRequired,
  projListDep: PropTypes.arrayOf(
    PropTypes.shape({
      name: PropTypes.string.isRequired,
    })
  ).isRequired,
};

EnvironmentUsage.displayName = "EnvironmentUsage";

export default EnvironmentUsage;
