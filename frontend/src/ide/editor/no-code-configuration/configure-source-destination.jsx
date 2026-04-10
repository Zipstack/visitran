import {
  Select,
  Row,
  Col,
  AutoComplete,
  Modal,
  Radio,
  Card,
  Divider,
  Space,
  theme,
} from "antd";
import {
  ApartmentOutlined,
  DatabaseOutlined,
  ApiOutlined,
} from "@ant-design/icons";
import PropTypes from "prop-types";
import { useImmer } from "use-immer";
import { useEffect, useState } from "react";

import { useProjectStore } from "../../../store/project-store.js";
import { useAxiosPrivate } from "../../../service/axios-service.js";
import { orgStore } from "../../../store/org-store.js";
function ConfigureSourceDestination({
  modelName,
  reference,
  setReference,
  source,
  setSource,
  model,
  setModel,
}) {
  const axios = useAxiosPrivate();
  const { dbConfigDetails, projectId } = useProjectStore();
  const { selectedOrgId } = orgStore();
  const isSchemaExists = dbConfigDetails?.is_schema_exists ?? false;
  const [referenceList, setReferenceList] = useState([]);
  const [allSchemas, setAllSchemas] = useState([]);
  const [allTables, setAllTables] = useImmer({});
  const [openDestChangeAlert, setOpenDestChangeAlert] = useState(false);
  const [modelType, setModelType] = useState("root");
  const [prevReference, setPrevReference] = useState([]);
  const [isLoadingSchemas, setIsLoadingSchemas] = useState(false);
  const [isLoadingTables, setIsLoadingTables] = useImmer({});
  const { token } = theme.useToken();
  // Get all schemas of configured DB
  const getAllSchemas = () => {
    setIsLoadingSchemas(true);
    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/schemas`,
    };
    axios(requestOptions)
      .then((res) => {
        setAllSchemas(res.data?.schema_names.sort());
      })
      .catch((error) => {
        console.error(error);
      })
      .finally(() => {
        setIsLoadingSchemas(false);
      });
  };
  const setTables = (data, schema, setter, mount) => {
    setAllTables((newAllTables) => {
      const allTables = Object.keys(data).sort((a, b) => a.localeCompare(b));
      newAllTables[schema] = allTables;
      // Change table when schema is changed
      setter((draft) => {
        if (!mount) {
          draft.table_name = allTables[0];
        }
      });
    });
  };
  // Get all tables of a specific schema
  const getAllTables = (schema, setter, mount = false, isSource = true) => {
    // Check if tables are already fetched for the given schema
    if (schema in allTables) {
      // Change table when schema is changed
      setter((draft) => {
        if (!mount) {
          draft.table_name = allTables[schema][0];
        }
      });
      return;
    }

    // Set loading state for this specific schema
    setIsLoadingTables((draft) => {
      draft[schema] = true;
    });

    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/schema/${schema || "default"}/tables`,
    };
    axios(requestOptions)
      .then((res) => {
        setTables(res.data?.table_description, schema, setter, mount);
      })
      .catch((error) => {
        console.error(error);
      })
      .finally(() => {
        // Clear loading state for this specific schema
        setIsLoadingTables((draft) => {
          draft[schema] = false;
        });
      });
  };
  const getrefValues = (selectedValues) => {
    const queryParams = selectedValues.length
      ? `?selected_references=${selectedValues.join(",")}`
      : "";
    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/no_code_model/${modelName}/supported_references${queryParams}`,
    };
    axios(requestOptions)
      .then((res) => {
        const data = res.data?.supported_reference_models || [];
        const options = data.map((value) => ({ value, label: value }));
        setReferenceList(options);
      })
      .catch((err) => {
        console.error(`Failed to get reference list for "${modelName}"`, err);
      });
  };
  // Handles source/model schema and table change
  const handleChange = (key, value, setter, isSource = true) => {
    if (key === "reference") {
      setter(value);
      getrefValues(value); // Call API with selected values
      return;
    }
    if (key === "schema_name" && allSchemas.includes(value)) {
      if (isSource) {
        getAllTables(value, setter, isSource);
      } else if (source.schema_name !== value) {
        setModel((newDestination) => {
          newDestination.table_name = "";
        });
      } else if (source.table_name) {
        setModel((newDestination) => {
          newDestination.table_name = modelName;
        });
      }
    }
    setter((draft) => {
      draft[key] = value;
      // Get all columns if table is changed
      if (key === "table_name" && isSource) {
        if (isSchemaExists) {
          if (model.schema_name) {
            if (model.schema_name === source.schema_name) {
              setModel((newDestination) => {
                newDestination.table_name = modelName;
              });
            } else {
              setModel((newDestination) => {
                newDestination.table_name = "";
              });
            }
          }
        } else {
          setModel((newDestination) => {
            newDestination.table_name = modelName;
          });
        }
      }
    });
  };
  const closeDestChangeWarning = () => {
    setOpenDestChangeAlert(false);
  };
  useEffect(() => {
    getrefValues(reference); // Fetch initial reference values
    if (isSchemaExists) {
      getAllSchemas();
      if (source.schema_name) {
        getAllTables(source.schema_name, setSource, true, true);
      }
      if (model.schema_name && source.schema_name !== model.schema_name) {
        getAllTables(model.schema_name, setModel, true, false);
      }
    } else {
      getAllTables("default", setSource, true, true);
    }
  }, []);
  useEffect(() => {
    if (reference.length > 0) {
      setModelType("child");
    } else {
      setModelType("root");
    }
  }, [reference]);
  return (
    <>
      {/* ---------- Hierarchy ---------- */}
      <Card style={{ marginBottom: "12px" }}>
        <h4 style={{ marginTop: 0 }}>
          <Space>
            <ApartmentOutlined />
            Hierarchy
          </Space>
        </h4>
        <Radio.Group
          value={modelType}
          onChange={(e) => {
            const newModelType = e.target.value;
            if (newModelType === "root") {
              setPrevReference(reference.length ? reference : []);
              setReference([]);
            } else {
              setReference(
                prevReference.length ? prevReference : reference
              );
            }
            setModelType(newModelType);
          }}
          className="mb-10"
        >
          <Radio value="root">Root model</Radio>
          <Space>
            <Radio value="child">Child of</Radio>
          </Space>
        </Radio.Group>
        {modelType === "child" && (
          <Select
            mode="multiple"
            style={{ width: "100%" }}
            placeholder="Select the model"
            value={reference}
            onChange={(value) =>
              handleChange("reference", value, setReference, false)
            }
            options={referenceList}
            showSearch
          />
        )}
      </Card>

      {/* ---------- Configure Source & Destination ---------- */}
      <Card
        className="configure-tables"
        style={{
          backgroundColor: token.colorFillTertiary,
          marginBottom: "12px",
        }}
        bodyStyle={{ padding: "12px 16px" }}
      >
        <h4 style={{ marginTop: 0 }}>
          <Space>
            <DatabaseOutlined />
            Configure Source
          </Space>
        </h4>
        <Row gutter={[12]}>
          {isSchemaExists && (
            <Col span={12}>
              <Select
                className="mb-10"
                style={{ width: "100%" }}
                placeholder="Select the schema"
                value={source.schema_name}
                onChange={(value) =>
                  handleChange("schema_name", value, setSource)
                }
                options={allSchemas.map((value) => ({ value }))}
                showSearch
                popupMatchSelectWidth={false}
                loading={isLoadingSchemas}
                notFoundContent={
                  isLoadingSchemas
                    ? "Loading schemas..."
                    : "No schemas found"
                }
              />
            </Col>
          )}
          <Col span={isSchemaExists ? 12 : 24}>
            <Select
              className="mb-10"
              style={{ width: "100%" }}
              placeholder="Select the table"
              value={source.table_name}
              onChange={(value) =>
                handleChange("table_name", value, setSource)
              }
              options={allTables[
                isSchemaExists ? source.schema_name : "default"
              ]?.map((value) => ({
                value,
              }))}
              showSearch
              popupMatchSelectWidth={false}
              disabled={isSchemaExists && !source.schema_name}
              loading={
                isLoadingTables[
                  isSchemaExists ? source.schema_name : "default"
                ]
              }
              notFoundContent={
                isLoadingTables[
                  isSchemaExists ? source.schema_name : "default"
                ]
                  ? "Loading tables..."
                  : "No tables found"
              }
            />
          </Col>
        </Row>
        <Divider />
        <h4>
          <Space>
            <ApiOutlined />
            Configure Destination
          </Space>
        </h4>
        <Row gutter={[12]}>
          {isSchemaExists && (
            <Col span={12}>
              <Select
                className="mb-10"
                style={{ width: "100%" }}
                placeholder="Select the schema"
                value={model.schema_name}
                onChange={(value) =>
                  handleChange("schema_name", value, setModel, false)
                }
                options={allSchemas.map((value) => ({ value }))}
                showSearch
                popupMatchSelectWidth={false}
                loading={isLoadingSchemas}
                notFoundContent={
                  isLoadingSchemas
                    ? "Loading schemas..."
                    : "No schemas found"
                }
              />
            </Col>
          )}
          <Col span={isSchemaExists ? 12 : 24}>
            <AutoComplete
              className="mb-10"
              style={{ width: "100%" }}
              placeholder="Select the table"
              value={model.table_name}
              onChange={(value) => {
                const noSpacesValue = value.replace(/\s/g, "");
                handleChange(
                  "table_name",
                  noSpacesValue,
                  setModel,
                  false
                );
              }}
              disabled={isSchemaExists && !model.schema_name}
            />
          </Col>
        </Row>
      </Card>
      <Divider />
      <Modal
        open={openDestChangeAlert}
        title={"Warning"}
        onOk={closeDestChangeWarning}
        cancelButtonProps={{ style: { display: "none" } }}
        closable={false}
        centered
        maskClosable={false}
      >
        Destination table already exists and will be overwritten.
      </Modal>
    </>
  );
}
ConfigureSourceDestination.propTypes = {
  modelName: PropTypes.string.isRequired,
  reference: PropTypes.array,
  setReference: PropTypes.func,
  source: PropTypes.object.isRequired,
  setSource: PropTypes.func.isRequired,
  model: PropTypes.object.isRequired,
  setModel: PropTypes.func.isRequired,
};
export { ConfigureSourceDestination };
