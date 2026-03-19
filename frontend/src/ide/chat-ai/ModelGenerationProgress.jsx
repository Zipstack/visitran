import { useMemo } from "react";
import PropTypes from "prop-types";
import {
  CheckCircleFilled,
  CloseCircleFilled,
  LoadingOutlined,
  ClockCircleOutlined,
} from "@ant-design/icons";
import "./ThoughtChainEnhancements.css";

/**
 * ModelGenerationProgress - Shows progress of model generation with shimmer effects
 * Uses generate_model_list from WebSocket to track models being created
 *
 * @return {JSX.Element} Progress component showing model generation status
 */
const ModelGenerationProgress = ({ modelList, modelStatus, isProcessing }) => {
  // Parse model list and determine status for each model
  const models = useMemo(() => {
    if (!modelList || modelList.length === 0) return [];

    return modelList.map((modelName, index) => {
      let status = "pending";

      // Determine status based on modelStatus and position
      if (modelStatus === "MODEL_CREATED" || modelStatus === "MODEL_UPDATED") {
        // All models up to current index are complete
        status = "success";
      } else if (isProcessing && index === modelList.length - 1) {
        // Last model is currently being created
        status = "creating";
      } else if (
        modelStatus === "MODEL_CREATE_FAILED" &&
        index === modelList.length - 1
      ) {
        status = "failed";
      } else if (index < modelList.length - 1) {
        // Previous models are successful
        status = "success";
      }

      return {
        name: modelName,
        status,
        index: index + 1,
      };
    });
  }, [modelList, modelStatus, isProcessing]);

  if (models.length === 0) return null;

  const completedCount = models.filter((m) => m.status === "success").length;
  const totalCount = models.length;

  return (
    <div className="model-progress-tracker fade-in">
      <div className="model-progress-header">
        <div className="model-progress-title">
          {isProcessing ? (
            <span className="shimmer-text">Generating Models</span>
          ) : (
            "Model Generation"
          )}
        </div>
        <div className="model-progress-count">
          {completedCount} of {totalCount} completed
        </div>
      </div>

      <div className="model-progress-list">
        {models.map((model, idx) => (
          <div
            key={`${model.name}-${idx}`}
            className={`model-progress-item ${model.status}`}
          >
            <div className={`model-progress-status-icon ${model.status}`}>
              {model.status === "pending" && <ClockCircleOutlined />}
              {model.status === "creating" && <LoadingOutlined />}
              {model.status === "success" && (
                <CheckCircleFilled className="success-checkmark" />
              )}
              {model.status === "failed" && <CloseCircleFilled />}
            </div>

            <div className="model-progress-name">
              {model.status === "creating" ? (
                <span className="shimmer-text-subtle">{model.name}</span>
              ) : (
                model.name
              )}
            </div>

            <div className="model-progress-index">
              {model.index} of {totalCount}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

ModelGenerationProgress.propTypes = {
  modelList: PropTypes.arrayOf(PropTypes.string),
  modelStatus: PropTypes.string,
  isProcessing: PropTypes.bool,
};

ModelGenerationProgress.defaultProps = {
  modelList: [],
  modelStatus: null,
  isProcessing: false,
};

export default ModelGenerationProgress;
