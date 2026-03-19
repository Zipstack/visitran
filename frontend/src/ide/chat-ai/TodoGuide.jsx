import { memo, useState } from "react";
import PropTypes from "prop-types";
import { Card, Checkbox, Button, Typography } from "antd";
import { RocketOutlined } from "@ant-design/icons";
import "./TodoGuide.css";

const { Text } = Typography;

const TodoGuide = memo(function TodoGuide({
  visible,
  onPromptSelect,
  completedTasks: externalCompletedTasks,
  onCompletedTasksChange,
}) {
  const [internalCompletedTasks, setInternalCompletedTasks] = useState(
    new Set()
  );

  // Use external completed tasks if provided, otherwise use internal state
  const completedTasks = externalCompletedTasks || internalCompletedTasks;
  const setCompletedTasks = onCompletedTasksChange || setInternalCompletedTasks;

  const todoItems = [
    {
      id: "explore-database",
      title: "Explore Customer Database",
      description:
        "Connect to the sample customer database and explore available tables",
      prompt:
        "Show me all the tables in the database and their structure. I want to understand what customer data is available.",
    },
    {
      id: "customer-segmentation",
      title: "Customer Segmentation Analysis",
      description:
        "Create customer segments based on purchase behavior and demographics",
      prompt:
        "Create customer segments based on purchase behavior and demographics. Show me different customer groups and their characteristics.",
    },
    {
      id: "sales-analytics",
      title: "Sales Analytics Pipeline",
      description: "Generate monthly sales reports with trend analysis",
      prompt:
        "Create a monthly sales analytics pipeline with trend analysis. Show me sales performance over time and key insights.",
    },
    {
      id: "data-visualization",
      title: "Data Visualization Dashboard",
      description: "Create interactive charts for key business metrics",
      prompt:
        "Build a data visualization dashboard with interactive charts for key business metrics like revenue, customer growth, and sales trends.",
    },
  ];

  // Check if a task can be enabled based on step-wise progression
  const isTaskEnabled = (taskIndex) => {
    // First task is always enabled
    if (taskIndex === 0) return true;

    // Subsequent tasks are enabled only if the previous task is completed
    const previousTaskId = todoItems[taskIndex - 1].id;
    return completedTasks.has(previousTaskId);
  };

  const handleCheckboxChange = (taskId, checked, taskIndex) => {
    // Only allow checking if task is enabled
    if (!isTaskEnabled(taskIndex) && checked) return;

    const newCompleted = new Set(completedTasks);
    if (checked) {
      newCompleted.add(taskId);
    } else {
      // When unchecking, also uncheck all subsequent tasks
      for (let i = taskIndex; i < todoItems.length; i++) {
        newCompleted.delete(todoItems[i].id);
      }
    }
    setCompletedTasks(newCompleted);
  };

  const handleTryThis = (prompt) => {
    if (onPromptSelect) {
      onPromptSelect(prompt);
    }
  };

  const completedCount = completedTasks.size;
  const totalCount = todoItems.length;

  if (!visible) return null;

  return (
    <Card className="todo-guide-card">
      <div className="todo-guide-header">
        <div className="todo-guide-title">
          <RocketOutlined className="todo-guide-icon" />
          <Text strong className="todo-guide-title-text">
            Try the Starter Project
          </Text>
        </div>
        <div className="todo-guide-progress">
          <Text
            className={`todo-guide-progress-text ${
              completedCount === totalCount ? "completed" : ""
            }`}
          >
            {completedCount === totalCount
              ? "✓ All Complete!"
              : `${completedCount}/${totalCount} Complete`}
          </Text>
        </div>
      </div>

      <div className="todo-guide-content">
        {todoItems.map((item, index) => {
          const isCompleted = completedTasks.has(item.id);
          const isEnabled = isTaskEnabled(index);
          const showTryButton = isEnabled && !isCompleted;

          return (
            <div
              key={item.id}
              className={`todo-guide-item ${isCompleted ? "completed" : ""} ${
                !isEnabled ? "disabled" : ""
              }`}
            >
              <div className="todo-guide-item-content">
                <div className="todo-guide-item-header">
                  <Checkbox
                    checked={isCompleted}
                    onChange={(e) =>
                      handleCheckboxChange(item.id, e.target.checked, index)
                    }
                    disabled={!isEnabled}
                    className="todo-guide-checkbox"
                  />
                  <div className="todo-guide-item-text">
                    <Text
                      strong
                      className={`todo-guide-item-title ${
                        isCompleted ? "strikethrough" : ""
                      }`}
                    >
                      {item.title}
                    </Text>
                    <Text
                      className={`todo-guide-item-description ${
                        isCompleted ? "strikethrough" : ""
                      }`}
                    >
                      {item.description}
                    </Text>
                  </div>
                </div>

                {showTryButton && (
                  <div className="todo-guide-item-actions">
                    <Button
                      type="primary"
                      size="small"
                      onClick={() => handleTryThis(item.prompt)}
                      className="todo-guide-try-button"
                    >
                      Try Now →
                    </Button>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
});

TodoGuide.propTypes = {
  visible: PropTypes.bool.isRequired,
  onPromptSelect: PropTypes.func,
  completedTasks: PropTypes.instanceOf(Set),
  onCompletedTasksChange: PropTypes.func,
};

TodoGuide.displayName = "TodoGuide";

export { TodoGuide };
