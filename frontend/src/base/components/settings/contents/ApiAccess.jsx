import { useCallback, useEffect, useMemo, useState } from "react";
import { Typography, Space, Table, Tabs, Button, Select, Tooltip } from "antd";
import { CopyOutlined, CheckOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";

import { useAxiosPrivate } from "../../../../service/axios-service";
import { orgStore } from "../../../../store/org-store";
import { useNotificationService } from "../../../../service/notification-service";

import "./ApiAccess.css";

const { Text } = Typography;

const ApiAccess = () => {
  const axios = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const { notify } = useNotificationService();

  const [projects, setProjects] = useState([]);
  const [selectedProject, setSelectedProject] = useState(null);
  const [copiedField, setCopiedField] = useState(null);

  const orgId = selectedOrgId || "default_org";
  const baseUrl = window.location.origin;
  const apiPrefix = `/api/v1/visitran/${orgId}`;

  /* ------------------------------------------------------------------ */
  /*                         fetch projects                              */
  /* ------------------------------------------------------------------ */
  const fetchProjects = useCallback(async () => {
    try {
      const { data } = await axios.get(`${apiPrefix}/projects`);
      const list = Array.isArray(data) ? data : data?.page_items || [];
      setProjects(list);
      if (list.length > 0 && !selectedProject) {
        setSelectedProject(list[0].project_id || list[0].project_uuid);
      }
    } catch {
      // Projects may not be loaded yet
    }
  }, [selectedOrgId]);

  useEffect(() => {
    fetchProjects();
  }, []);

  /* ------------------------------------------------------------------ */
  /*                          copy helper                                */
  /* ------------------------------------------------------------------ */
  const copyToClipboard = useCallback(async (text, field) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedField(field);
      setTimeout(() => setCopiedField(null), 2000);
    } catch {
      notify({
        type: "error",
        message: "Copy Failed",
        description: "Unable to copy to clipboard.",
      });
    }
  }, []);

  const CopyIcon = ({ text, field }) =>
    copiedField === field ? (
      <CheckOutlined style={{ color: "green", cursor: "pointer" }} />
    ) : (
      <Tooltip title="Copy">
        <CopyOutlined
          style={{ cursor: "pointer" }}
          onClick={() => copyToClipboard(text, field)}
        />
      </Tooltip>
    );
  CopyIcon.propTypes = { text: PropTypes.string, field: PropTypes.string };

  /* ------------------------------------------------------------------ */
  /*                      endpoint table data                            */
  /* ------------------------------------------------------------------ */
  const projectId = selectedProject || "{project_id}";

  const endpoints = useMemo(
    () => [
      {
        key: "run",
        method: "POST",
        path: `/project/${projectId}/execute/run`,
        description: "Trigger an async model run (returns run_id)",
      },
      {
        key: "status",
        method: "GET",
        path: `/project/${projectId}/execute/run/{run_id}/status`,
        description: "Poll run status until completion",
      },
      {
        key: "manifest",
        method: "GET",
        path: `/project/${projectId}/models/manifest`,
        description: "List all models with dependencies",
      },
      {
        key: "seed",
        method: "POST",
        path: `/project/${projectId}/execute/seed`,
        description: "Run seed data loading",
      },
    ],
    [projectId]
  );

  const endpointColumns = [
    {
      title: "Method",
      dataIndex: "method",
      width: 80,
      render: (method) => (
        <span className={`endpoint-method method-${method.toLowerCase()}`}>
          {method}
        </span>
      ),
    },
    {
      title: "Endpoint",
      dataIndex: "path",
      render: (path) => <span className="endpoint-path">{path}</span>,
    },
    {
      title: "Description",
      dataIndex: "description",
    },
  ];

  /* ------------------------------------------------------------------ */
  /*                      code snippets                                  */
  /* ------------------------------------------------------------------ */
  const curlTriggerRun = `curl -X POST \\
  -H "Authorization: Bearer \${VISITRAN_API_TOKEN}" \\
  -H "Content-Type: application/json" \\
  -d '{"select_models": ["model_name"]}' \\
  "${baseUrl}${apiPrefix}/project/${projectId}/execute/run"`;

  const curlPollStatus = `curl -H "Authorization: Bearer \${VISITRAN_API_TOKEN}" \\
  "${baseUrl}${apiPrefix}/project/${projectId}/execute/run/{run_id}/status"`;

  const curlManifest = `curl -H "Authorization: Bearer \${VISITRAN_API_TOKEN}" \\
  "${baseUrl}${apiPrefix}/project/${projectId}/models/manifest"`;

  const pythonSnippet = `import requests
import time

VISITRAN_URL = "${baseUrl}${apiPrefix}"
API_TOKEN = "vtk_..."  # Your API token
PROJECT_ID = "${projectId}"

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
}

# 1. Trigger a run
resp = requests.post(
    f"{VISITRAN_URL}/project/{PROJECT_ID}/execute/run",
    json={"select_models": ["model_name"]},
    headers=headers,
)
run_id = resp.json()["run_id"]
print(f"Run started: {run_id}")

# 2. Poll until complete
while True:
    status = requests.get(
        f"{VISITRAN_URL}/project/{PROJECT_ID}/execute/run/{run_id}/status",
        headers=headers,
    ).json()
    print(f"Status: {status['status']}")
    if status["status"] in ("SUCCESS", "FAILURE"):
        break
    time.sleep(5)`;

  const airflowSnippet = `from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests, time

VISITRAN_URL = "${baseUrl}${apiPrefix}"
PROJECT_ID = "${projectId}"

@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False)
def visitran_pipeline():

    @task
    def trigger_run(**context):
        token = context["var"]["value"].get("VISITRAN_API_TOKEN")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.post(
            f"{VISITRAN_URL}/project/{PROJECT_ID}/execute/run",
            json={"select_models": ["orders", "customers"]},
            headers=headers,
        )
        return resp.json()["run_id"]

    @task
    def wait_for_completion(run_id, **context):
        token = context["var"]["value"].get("VISITRAN_API_TOKEN")
        headers = {"Authorization": f"Bearer {token}"}
        while True:
            resp = requests.get(
                f"{VISITRAN_URL}/project/{PROJECT_ID}/execute/run/{run_id}/status",
                headers=headers,
            ).json()
            if resp["status"] in ("SUCCESS", "FAILURE"):
                return resp
            time.sleep(10)

    run_id = trigger_run()
    wait_for_completion(run_id)

visitran_pipeline()`;

  /* ------------------------------------------------------------------ */
  /*                              render                                */
  /* ------------------------------------------------------------------ */
  const CodeBlock = ({ code, id }) => (
    <div className="api-access-code-block">
      <Button
        size="small"
        type="text"
        className="copy-btn"
        icon={<CopyIcon text={code} field={id} />}
        onClick={() => copyToClipboard(code, id)}
      />
      <pre>{code}</pre>
    </div>
  );
  CodeBlock.propTypes = { code: PropTypes.string, id: PropTypes.string };

  return (
    <div className="api-access-wrap">
      <Space direction="vertical" size={10} style={{ width: "100%" }}>
        <Typography.Title level={5}>API Access</Typography.Title>
        <Text type="secondary" style={{ fontSize: 13 }}>
          Use these endpoints to integrate Visitran with external orchestrators
          like Airflow, Dagster, or Prefect.
        </Text>

        {/* ─── Connection Details ──────────────────────────────── */}
        <div className="api-access-section" style={{ marginTop: 20 }}>
          <Typography.Title level={5} style={{ fontSize: 14 }}>
            Connection Details
          </Typography.Title>

          <div className="api-access-info-row">
            <span className="info-label">Base URL</span>
            <span className="info-value">{baseUrl}</span>
            <CopyIcon text={baseUrl} field="base-url" />
          </div>

          <div className="api-access-info-row">
            <span className="info-label">Org ID</span>
            <span className="info-value">{orgId}</span>
            <CopyIcon text={orgId} field="org-id" />
          </div>

          <div className="api-access-info-row">
            <span className="info-label">Project</span>
            <Select
              size="small"
              style={{ flex: 1 }}
              value={selectedProject}
              onChange={setSelectedProject}
              placeholder="Select a project"
              options={projects.map((p) => ({
                value: p.project_id || p.project_uuid,
                label: p.project_name,
              }))}
            />
          </div>

          <Text
            type="secondary"
            style={{ fontSize: 12, display: "block", marginTop: 8 }}
          >
            API tokens can be managed in{" "}
            <strong>Settings &gt; API Tokens</strong>. Pass the token as{" "}
            <code>Authorization: Bearer vtk_...</code>
          </Text>
        </div>

        {/* ─── Endpoints ──────────────────────────────────────── */}
        <div className="api-access-section">
          <Typography.Title level={5} style={{ fontSize: 14 }}>
            Available Endpoints
          </Typography.Title>
          <Table
            className="api-access-endpoint-table"
            columns={endpointColumns}
            dataSource={endpoints}
            pagination={false}
            size="small"
          />
        </div>

        {/* ─── Code Examples ──────────────────────────────────── */}
        <div className="api-access-section">
          <Typography.Title level={5} style={{ fontSize: 14 }}>
            Code Examples
          </Typography.Title>

          <Tabs
            className="api-access-tabs"
            items={[
              {
                key: "curl-trigger",
                label: "cURL - Trigger Run",
                children: <CodeBlock code={curlTriggerRun} id="curl-trigger" />,
              },
              {
                key: "curl-status",
                label: "cURL - Poll Status",
                children: <CodeBlock code={curlPollStatus} id="curl-status" />,
              },
              {
                key: "curl-manifest",
                label: "cURL - Manifest",
                children: <CodeBlock code={curlManifest} id="curl-manifest" />,
              },
              {
                key: "python",
                label: "Python",
                children: <CodeBlock code={pythonSnippet} id="python" />,
              },
              {
                key: "airflow",
                label: "Airflow DAG",
                children: <CodeBlock code={airflowSnippet} id="airflow" />,
              },
            ]}
          />
        </div>
      </Space>
    </div>
  );
};

export { ApiAccess };
export default ApiAccess;
