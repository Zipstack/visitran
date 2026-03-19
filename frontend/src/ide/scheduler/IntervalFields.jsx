import { memo } from "react";
import { Form, Radio, InputNumber, Typography } from "antd";
import PropTypes from "prop-types";

const IntervalFields = memo(({ canWrite }) => (
  <>
    <Typography>Interval</Typography>
    <div style={{ display: "flex", gap: 16, alignItems: "flex-start" }}>
      <Form.Item
        label="Period"
        name="period"
        rules={[{ required: true, message: "Please select a period" }]}
        style={{ flex: 1 }}
      >
        <Radio.Group disabled={!canWrite}>
          <Radio.Button value="Minutes">Minutes</Radio.Button>
          <Radio.Button value="Hours">Hours</Radio.Button>
        </Radio.Group>
      </Form.Item>

      <Form.Item
        label="Every"
        name="every"
        dependencies={["period"]}
        rules={[
          { required: true, message: "Please enter a valid number" },
          ({ getFieldValue }) => {
            const period = getFieldValue("period");
            const isHours = period === "Hours";
            const min = isHours ? 1 : 30;
            const max = isHours ? 24 : 60;
            return {
              type: "number",
              min,
              max,
              message: `Value must be between ${min} and ${max}`,
            };
          },
        ]}
        style={{ flex: 1 }}
      >
        <InputNumber
          step={1}
          precision={0}
          disabled={!canWrite}
          className="job-deploy-number-input"
        />
      </Form.Item>
    </div>
  </>
));

IntervalFields.propTypes = {
  canWrite: PropTypes.bool.isRequired,
};

IntervalFields.displayName = "IntervalFields";

export { IntervalFields };
