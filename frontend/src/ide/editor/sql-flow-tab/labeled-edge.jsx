import { getBezierPath, EdgeLabelRenderer } from "reactflow";
import PropTypes from "prop-types";

/**
 * LabeledEdge - Custom edge with curved bezier line and label.
 *
 * Displays edge type (JOIN, UNION, reference) as a small label on the edge.
 * @param {object} props - Edge props from ReactFlow
 * @return {JSX.Element} The rendered edge component
 */
const LabeledEdge = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  style,
  markerEnd,
}) => {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  // Get label text based on edge type
  const getLabel = () => {
    const edgeType = data?.edgeType;
    switch (edgeType) {
      case "join":
        return "JOIN";
      case "union":
        return "UNION";
      case "reference":
        return "REF";
      default:
        return null; // No label for "source" type edges
    }
  };

  const label = getLabel();
  const edgeType = data?.edgeType || "source";

  return (
    <>
      <path
        id={id}
        className="react-flow__edge-path"
        d={edgePath}
        style={style}
        markerEnd={markerEnd}
      />
      {label && (
        <EdgeLabelRenderer>
          <div
            className={`edge-label ${edgeType}`}
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              pointerEvents: "all",
            }}
          >
            {label}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
};

LabeledEdge.propTypes = {
  id: PropTypes.string.isRequired,
  sourceX: PropTypes.number.isRequired,
  sourceY: PropTypes.number.isRequired,
  targetX: PropTypes.number.isRequired,
  targetY: PropTypes.number.isRequired,
  sourcePosition: PropTypes.string.isRequired,
  targetPosition: PropTypes.string.isRequired,
  data: PropTypes.shape({
    edgeType: PropTypes.string,
  }),
  style: PropTypes.object,
  markerEnd: PropTypes.string,
};

LabeledEdge.propTypes = {
  id: PropTypes.string.isRequired,
  sourceX: PropTypes.number.isRequired,
  sourceY: PropTypes.number.isRequired,
  targetX: PropTypes.number.isRequired,
  targetY: PropTypes.number.isRequired,
  sourcePosition: PropTypes.string.isRequired,
  targetPosition: PropTypes.string.isRequired,
  data: PropTypes.shape({
    edgeType: PropTypes.string,
  }),
  style: PropTypes.object,
  markerEnd: PropTypes.string,
};

export default LabeledEdge;
