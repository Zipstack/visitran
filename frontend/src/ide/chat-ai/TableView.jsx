import { Table } from "antd";
import PropTypes from "prop-types";

function TableView({ rows, columns }) {
  return (
    <Table
      dataSource={rows}
      columns={columns}
      size="small"
      bordered
      pagination={{ pageSize: 5 }}
      scroll={{ x: "max-content" }}
    />
  );
}

TableView.propTypes = {
  rows: PropTypes.array,
  columns: PropTypes.array,
};

export { TableView };
