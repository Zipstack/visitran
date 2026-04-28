/* eslint-disable react/prop-types */
import { Tag } from "antd";
import {
  CheckCircleFilled,
  ExclamationCircleFilled,
  CloseCircleFilled,
} from "@ant-design/icons";

/* ── Fields that render side-by-side in credential forms ── */
export const HALF_WIDTH_FIELDS = new Set([
  "host",
  "port",
  "user",
  "passw",
  "account",
  "warehouse",
  "catalog",
  "schema",
  "dbname",
  "database",
  "project_id",
  "dataset_id",
  "token",
]);

/* ── Custom RJSF ObjectFieldTemplate for grid layout ── */
export const GridObjectFieldTemplate = (props) => (
  <div className="conn-cred-grid">
    {props.properties.map((prop) => (
      <div
        key={prop.name}
        className={
          HALF_WIDTH_FIELDS.has(prop.name)
            ? "conn-cred-field-half"
            : "conn-cred-field-full"
        }
      >
        {prop.content}
      </div>
    ))}
  </div>
);

/* ── Status tag component ── */
export const StatusTag = ({ flag }) => {
  if (flag === "GREEN")
    return (
      <Tag icon={<CheckCircleFilled />} color="success">
        Healthy
      </Tag>
    );
  if (flag === "YELLOW")
    return (
      <Tag icon={<ExclamationCircleFilled />} color="warning">
        Stale
      </Tag>
    );
  if (flag === "RED")
    return (
      <Tag icon={<CloseCircleFilled />} color="error">
        Error
      </Tag>
    );
  return null;
};
