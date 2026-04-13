/**
 * Shared utility functions for lineage scoping.
 * Used by both lineage-tab.jsx (standalone) and no-code-model.jsx (bottom section).
 */

/**
 * Find all ancestor and descendant node IDs for a given model.
 * @param {Array} allEdges - Array of { source, target } edge objects
 * @param {string} selectedLabel - The label of the selected model
 * @param {Array} allNodes - Array of node objects with data.originalLabel or data.label
 * @return {Set|null} Set of related node IDs, or null if selected model not found
 */
export const getRelatedNodeIds = (allEdges, selectedLabel, allNodes) => {
  const nodeByLabel = {};
  allNodes.forEach((n) => {
    nodeByLabel[n.data.originalLabel || n.data.label] = n.id;
  });
  const selectedId = nodeByLabel[selectedLabel];
  if (!selectedId) return null;

  const related = new Set([selectedId]);
  const findAncestors = (id) => {
    allEdges.forEach((e) => {
      if (e.target === id && !related.has(e.source)) {
        related.add(e.source);
        findAncestors(e.source);
      }
    });
  };
  const findDescendants = (id) => {
    allEdges.forEach((e) => {
      if (e.source === id && !related.has(e.target)) {
        related.add(e.target);
        findDescendants(e.target);
      }
    });
  };
  findAncestors(selectedId);
  findDescendants(selectedId);
  return related;
};

/**
 * Apply scoped styles to nodes and edges based on the selected model's lineage chain.
 * Related nodes stay full opacity, unrelated nodes are faded.
 * @param {Array} layoutedNodes - Array of positioned node objects
 * @param {Array} layoutedEdges - Array of edge objects
 * @param {string} selectedLabel - The label of the selected model
 * @return {Object} { nodes, edges } with scoped styles applied
 */
export const applyScopedStyles = (
  layoutedNodes,
  layoutedEdges,
  selectedLabel
) => {
  const rawEdges = layoutedEdges.map((e) => ({
    source: e.source,
    target: e.target,
  }));
  const related = getRelatedNodeIds(rawEdges, selectedLabel, layoutedNodes);
  if (!related) return { nodes: layoutedNodes, edges: layoutedEdges };

  const styledNodes = layoutedNodes.map((node) => {
    const nodeLabel = node.data.originalLabel || node.data.label;
    const isSelected = nodeLabel === selectedLabel;
    const isRelated = related.has(node.id);
    return {
      ...node,
      style: {
        ...node.style,
        opacity: isRelated ? 1 : 0.25,
        border: isSelected
          ? "2px dashed var(--lineage-selected-border)"
          : node.style?.border || "1px solid var(--black)",
      },
    };
  });

  const relatedEdgeSet = new Set();
  layoutedEdges.forEach((e) => {
    if (related.has(e.source) && related.has(e.target)) {
      relatedEdgeSet.add(e.id);
    }
  });

  const styledEdges = layoutedEdges.map((edge) => ({
    ...edge,
    style: {
      ...edge.style,
      opacity: relatedEdgeSet.has(edge.id) ? 1 : 0.15,
      stroke: relatedEdgeSet.has(edge.id)
        ? "var(--lineage-selected-border)"
        : undefined,
    },
  }));

  return { nodes: styledNodes, edges: styledEdges };
};
