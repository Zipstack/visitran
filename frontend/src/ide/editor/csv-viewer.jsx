import { useState, useEffect, useMemo } from "react";
import PropTypes from "prop-types";
import Papa from "papaparse";
import { Table, Typography, Input, Space } from "antd";
import { SearchOutlined } from "@ant-design/icons";

import { useProjectStore } from "../../store/project-store.js";
import { explorerService } from "../explorer/explorer-service.js";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { useNotificationService } from "../../service/notification-service.js";

import "./csv-viewer.css";

const { Text } = Typography;

const CSV_ROW_LIMIT = 5000;
const PAGE_SIZE = 100;

function CsvViewer({ nodeData = {} }) {
  const { projectId } = useProjectStore();
  const expService = explorerService();
  const { notify } = useNotificationService();

  const [rows, setRows] = useState(null);
  const [headers, setHeaders] = useState([]);
  const [totalRows, setTotalRows] = useState(0);
  const [truncated, setTruncated] = useState(false);
  const [searchText, setSearchText] = useState("");

  useEffect(
    function fetchFile() {
      expService
        .getFileContent(projectId, nodeData.key)
        .then((res) => {
          const parsed = Papa.parse(res.data, {
            header: true,
            skipEmptyLines: true,
          });

          const allRows = parsed.data;
          setTotalRows(allRows.length);
          setTruncated(allRows.length > CSV_ROW_LIMIT);

          const limited = allRows.slice(0, CSV_ROW_LIMIT);
          const dataWithKeys = limited.map((row, idx) => ({
            ...row,
            _rowKey: idx,
            _rowNum: idx + 1,
          }));

          setHeaders(parsed.meta.fields || []);
          setRows(dataWithKeys);
        })
        .catch((error) => {
          setRows([]);
          console.error(error);
          notify({ error });
        });
    },
    [] // eslint-disable-next-line
  );

  // Filter rows by search text
  const filteredRows = useMemo(() => {
    if (!rows) return [];
    if (!searchText.trim()) return rows;
    const term = searchText.toLowerCase();
    return rows.filter((row) =>
      headers.some((h) =>
        String(row[h] ?? "")
          .toLowerCase()
          .includes(term)
      )
    );
  }, [rows, headers, searchText]);

  // Build antd Table columns
  const columns = useMemo(() => {
    const rowNumCol = {
      title: "#",
      dataIndex: "_rowNum",
      key: "_rowNum",
      width: 60,
      fixed: "left",
      className: "csv-row-num",
    };

    const dataCols = headers.map((h) => ({
      title: h,
      dataIndex: h,
      key: h,
      ellipsis: true,
      sorter: (a, b) => {
        const va = a[h] ?? "";
        const vb = b[h] ?? "";
        // Try numeric sort first
        const na = Number(va);
        const nb = Number(vb);
        if (!isNaN(na) && !isNaN(nb)) return na - nb;
        return String(va).localeCompare(String(vb));
      },
    }));

    return [rowNumCol, ...dataCols];
  }, [headers]);

  if (rows === null) {
    return <SpinnerLoader />;
  }

  return (
    <div className="csv-viewer-container">
      {/* Toolbar */}
      <div className="csv-viewer-toolbar">
        <Space size="middle">
          <Input
            placeholder="Search rows..."
            prefix={<SearchOutlined />}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            allowClear
            size="small"
            style={{ width: 220 }}
          />
          <Text type="secondary" className="csv-viewer-stats">
            {filteredRows.length !== rows.length
              ? `${filteredRows.length} of ${rows.length} rows`
              : `${rows.length} rows`}
            {" \u00B7 "}
            {headers.length} columns
          </Text>
        </Space>
        {truncated && (
          <Text type="warning" className="csv-viewer-warning">
            Showing first {CSV_ROW_LIMIT.toLocaleString()} of{" "}
            {totalRows.toLocaleString()} rows
          </Text>
        )}
      </div>

      {/* Table */}
      <div className="csv-viewer-table">
        <Table
          dataSource={filteredRows}
          columns={columns}
          rowKey="_rowKey"
          size="small"
          bordered
          pagination={{
            pageSize: PAGE_SIZE,
            showSizeChanger: true,
            pageSizeOptions: [50, 100, 250, 500],
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total}`,
            size: "small",
          }}
          scroll={{ x: "max-content", y: "calc(100vh - 240px)" }}
          showSorterTooltip={false}
        />
      </div>
    </div>
  );
}

CsvViewer.propTypes = {
  nodeData: PropTypes.object,
};

export { CsvViewer };
