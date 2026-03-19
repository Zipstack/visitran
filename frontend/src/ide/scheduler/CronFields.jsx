import { memo, useEffect, useCallback } from "react";
import PropTypes from "prop-types";
import { Cron } from "react-js-cron";
import { Input, Typography, Button, Alert, Tooltip } from "antd";
import { InfoCircleOutlined } from "@ant-design/icons";
import "react-js-cron/dist/styles.css";

const validateCronExpression = (cronValue) => {
  if (!cronValue) return "Cron expression is required";

  const parts = cronValue.trim().split(" ");
  if (parts.length < 5) return "Invalid cron expression format";

  // eslint-disable-next-line no-unused-vars
  const [minute, hour, day, month, weekday] = parts;

  // Validate minute part specifically for 30-minute gap
  if (minute === "*") {
    return "Cannot run every minute. Minimum gap between runs must be 30 minutes.";
  }

  if (minute.includes("/")) {
    const step = parseInt(minute.split("/")[1], 10);
    if (step < 30) {
      return `Step value must be at least 30 minutes (current: ${step})`;
    }
    return null;
  }

  if (minute.includes(",")) {
    const minutes = minute
      .split(",")
      .map((m) => parseInt(m, 10))
      .filter((m) => !isNaN(m))
      .sort((a, b) => a - b);

    if (minutes.length < 2) return null;

    for (let i = 1; i < minutes.length; i++) {
      if (minutes[i] - minutes[i - 1] < 30) {
        return `Minutes must be at least 30 minutes apart (found ${
          minutes[i - 1]
        } and ${minutes[i]})`;
      }
    }

    // Check wrap-around from last to first minute of next hour
    if (60 - minutes[minutes.length - 1] + minutes[0] < 30) {
      return `Minutes must be at least 30 minutes apart (wrap-around from ${
        minutes[minutes.length - 1]
      } to ${minutes[0]})`;
    }

    return null;
  }

  if (minute.includes("-")) {
    const [start, end] = minute.split("-").map((m) => parseInt(m, 10));
    if (end - start < 30) {
      return `Range must span at least 30 minutes (${start}-${end})`;
    }
    return null;
  }

  // Single minute value - always valid since it will only run once per hour
  if (/^\d+$/.test(minute)) {
    return null;
  }

  return "Invalid minute format in cron expression";
};

const CronFields = memo(
  ({ canWrite, values, dispatchValues, error, setError }) => {
    const handleValueChange = useCallback(
      (newValue) => {
        const errorMessage = validateCronExpression(newValue);
        if (errorMessage) {
          setError({ description: errorMessage });
          return false;
        }
        setError(null);
        dispatchValues({ type: "set_values", value: newValue });
        return true;
      },
      [dispatchValues, setError]
    );

    const handleBlurOrEnter = useCallback(() => {
      const errorMessage = validateCronExpression(values.inputValue);
      if (errorMessage) {
        setError({ description: errorMessage });
      } else {
        setError(null);
        dispatchValues({ type: "set_cron_value", value: values.inputValue });
      }
    }, [values.inputValue, dispatchValues, setError]);

    useEffect(() => {
      const errorMessage = validateCronExpression(values.cronValue);
      if (errorMessage) {
        setError({ description: errorMessage });
      } else {
        setError(null);
      }
    }, [values.cronValue, setError]);

    return (
      <div>
        <div style={{ marginBottom: 16 }}>
          <Alert
            message="Schedule Configuration Rules"
            description={
              <ul style={{ marginBottom: 0, paddingLeft: 20 }}>
                <li>Minimum 30 minutes between job executions</li>
                <li>Use */30 for every 30 minutes</li>
                <li>For specific times, ensure at least 30-minute intervals</li>
                <li>
                  Default schedule: Every hour at minute 30 (&quot;30 * * *
                  *&quot;)
                </li>
              </ul>
            }
            type="info"
            showIcon
          />
        </div>

        <div style={{ marginBottom: 24 }}>
          <Typography.Title level={5} style={{ marginBottom: 8 }}>
            Manual Cron Input
            <Tooltip title="Format: minute hour day month weekday. Example: '30 * * * *' for every hour at 30 minutes">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14 }} />
            </Tooltip>
          </Typography.Title>
          <Input
            value={values.inputValue}
            disabled={!canWrite}
            onChange={(e) =>
              dispatchValues({ type: "set_input_value", value: e.target.value })
            }
            onBlur={handleBlurOrEnter}
            onPressEnter={handleBlurOrEnter}
            placeholder="e.g., 30 * * * *"
            status={error ? "error" : ""}
          />
          {error && (
            <Typography.Text
              type="danger"
              style={{ display: "block", marginTop: 8 }}
            >
              {error.description}
            </Typography.Text>
          )}
        </div>

        <div
          style={{
            display: "flex",
            alignItems: "flex-start",
            gap: 8,
            flexWrap: "wrap",
          }}
        >
          <div style={{ flex: 1, minWidth: 0 }}>
            <Cron
              disabled={!canWrite}
              value={values.cronValue}
              setValue={(newValue) => {
                if (newValue === "* * * * *") {
                  handleValueChange("30 * * * *");
                } else {
                  handleValueChange(newValue);
                }
              }}
              onError={(e) =>
                setError(e ? { description: e.description } : null)
              }
              clockFormat="24-hour"
              clearButton={false}
              defaultPeriod="hour"
              allowedPeriods={["hour", "day", "month", "week"]}
              allowedMinutes={[0, 15, 30, 45]}
              shortCuts={[
                { name: "Every 30 minutes", value: "*/30 * * * *" },
                { name: "Every hour at :30", value: "30 * * * *" },
                { name: "Every hour at :00", value: "0 * * * *" },
              ]}
            />
          </div>
          <Button size="small" onClick={() => handleValueChange("30 * * * *")}>
            Reset to Default
          </Button>
        </div>

        {error && (
          <Alert
            message="Validation Error"
            description={error.description}
            type="error"
            showIcon
            style={{ marginTop: 16 }}
          />
        )}
      </div>
    );
  }
);

CronFields.propTypes = {
  values: PropTypes.shape({
    cronValue: PropTypes.string,
    inputValue: PropTypes.string,
  }).isRequired,
  dispatchValues: PropTypes.func.isRequired,
  canWrite: PropTypes.bool.isRequired,
  error: PropTypes.shape({
    description: PropTypes.string,
  }),
  setError: PropTypes.func.isRequired,
};

CronFields.displayName = "CronFields";

export { CronFields };
