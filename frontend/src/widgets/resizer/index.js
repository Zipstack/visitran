import {
  useState,
  useRef,
  useMemo,
  useEffect,
  useCallback,
  forwardRef,
  useImperativeHandle,
} from "react";
import PropTypes from "prop-types";
import {
  LeftCircleFilled,
  RightCircleFilled,
  UpCircleFilled,
  DownCircleFilled,
  CloseCircleFilled,
} from "@ant-design/icons";
import { Typography } from "antd";

import "./resizer.css";

const RESIZE_AXIS = { X: "x", Y: "y" };
const DRAGGER_THICKNESS = { X: 10, Y: 10 };

const DEFAULT_WIDTH = "340px";
const DEFAULT_HEIGHT = "200px";
const HIDDEN_WIDTH = "48px";
const HIDDEN_HEIGHT = "18px";

const ResizerComponent = forwardRef(function ResizerComponent(
  {
    children,
    defaultCollapsed = false,
    disabled = false,
    limit = 500,
    axis = RESIZE_AXIS.X,
    width = defaultCollapsed ? HIDDEN_WIDTH : DEFAULT_WIDTH,
    height = defaultCollapsed ? HIDDEN_HEIGHT : DEFAULT_HEIGHT,
    style = {},
    forceCollapse = false,
  },
  ref
) {
  const [isResizing, setIsResizing] = useState(false);
  const [isDragDisabled, setIsDragDisabled] = useState(
    defaultCollapsed || forceCollapse
  );

  const dragRef = useRef();
  const toggleRef = useRef();
  const isResizingRef = useRef(false);
  const prevSizeRef = useRef(null);
  const initialPosRef = useRef(0);
  const initialSizeRef = useRef(0);

  const resizeComponentStyle = useMemo(
    () => getResizeComponentStyle(axis, defaultCollapsed),
    [axis, defaultCollapsed]
  );
  const draggerStyle = getDraggerStyle(axis);
  const isXaxis = () => axis === RESIZE_AXIS.X;

  /* ---------- external collapse / restore ---------- */
  useEffect(() => {
    const dragEl = dragRef.current;
    const toggleEl = toggleRef.current;
    if (!dragEl) return;

    const rememberSize = () => {
      if (!prevSizeRef.current) {
        prevSizeRef.current = isXaxis()
          ? dragEl.style.width || width
          : dragEl.style.height || height;
      }
    };

    const setTogglePos = (size) => {
      if (!toggleEl) return;
      if (isXaxis()) {
        toggleEl.style.left = `calc(${size} - 8px)`;
      } else {
        toggleEl.style.bottom = `calc(${size} - 18px)`;
      }
    };

    const collapse = () => {
      rememberSize();
      if (isXaxis()) {
        dragEl.style.width = HIDDEN_WIDTH;
        setTogglePos(HIDDEN_WIDTH);
      } else {
        dragEl.style.height = HIDDEN_HEIGHT;
        dragEl.style.overflow = "hidden";
        setTogglePos(HIDDEN_HEIGHT);
      }
      setIsDragDisabled(true);
    };

    const restore = () => {
      if (!prevSizeRef.current) return;

      const panelIsCollapsed =
        (isXaxis() && dragEl.style.width === HIDDEN_WIDTH) ||
        (!isXaxis() && dragEl.style.height === HIDDEN_HEIGHT);

      if (!panelIsCollapsed) return;

      if (isXaxis()) {
        dragEl.style.width = prevSizeRef.current;
      } else {
        dragEl.style.height = prevSizeRef.current;
        dragEl.style.overflow = "auto";
      }
      setTogglePos(prevSizeRef.current);
      setIsDragDisabled(false);
    };

    forceCollapse ? collapse() : restore();
    dispatchResizeEvent();
  }, [forceCollapse]);

  /* ---------- hover highlight ---------- */
  const highLightResize = (e) => {
    if (isDragDisabled) return;
    if (isXaxis()) {
      e.target.style.borderInlineEnd = "4px solid #4C9AFF";
    } else {
      e.target.style.borderBlockStart = "4px solid #4C9AFF";
    }
  };
  const nonHighLightResize = (e) => {
    if (isXaxis()) {
      e.target.style.borderInlineEnd = "1px solid var(--border-color-1)";
    } else {
      e.target.style.borderBlockStart = "1px solid var(--border-color-1)";
    }
  };

  /* ---------- mouse handling ---------- */
  function initial(e) {
    e.preventDefault();
    const pos = isXaxis() ? e.clientX : e.clientY;
    const size = isXaxis()
      ? dragRef.current.offsetWidth
      : dragRef.current.offsetHeight;
    initialPosRef.current = pos;
    initialSizeRef.current = size;
  }

  const resize = useCallback(
    (e) => {
      if (!isResizingRef.current) return;

      if (isXaxis()) {
        const resizingWidth =
          initialSizeRef.current + (e.clientX - initialPosRef.current);
        if (resizingWidth >= parseInt(width) && resizingWidth <= limit) {
          dragRef.current.style.width = `${resizingWidth}px`;
          highLightResize({ target: dragRef.current });
        } else {
          nonHighLightResize({ target: dragRef.current });
        }
      } else {
        const resizingHeight =
          initialSizeRef.current - (e.clientY - initialPosRef.current);
        const maxHeight = window.innerHeight * 0.85;
        if (resizingHeight >= parseInt(height) && resizingHeight <= maxHeight) {
          dragRef.current.style.height = `${resizingHeight}px`;
          highLightResize({ target: dragRef.current });
        } else {
          nonHighLightResize({ target: dragRef.current });
        }
      }
    },
    [isXaxis, width, height, limit, highLightResize, nonHighLightResize]
  );

  const onMouseUp = useCallback(() => {
    isResizingRef.current = false;
    setIsResizing(false);
    nonHighLightResize({ target: dragRef.current });

    /* update prev size so future collapse/restore uses latest size */
    if (isXaxis()) {
      prevSizeRef.current = dragRef.current.style.width;
    } else {
      prevSizeRef.current = dragRef.current.style.height;
    }

    window.removeEventListener("mousemove", resize);
    window.removeEventListener("mouseup", onMouseUp);
    dispatchResizeEvent();
  }, [isXaxis, resize, nonHighLightResize]);

  const onMouseDown = useCallback(
    (e) => {
      if (isDragDisabled) return;
      initial(e);
      isResizingRef.current = true;
      setIsResizing(true);
      window.addEventListener("mousemove", resize);
      window.addEventListener("mouseup", onMouseUp);
    },
    [isDragDisabled, initial, resize, onMouseUp]
  );

  /* ---------- toggle button ---------- */
  const toggleVisibility = useCallback(() => {
    const dragStyle = dragRef.current.style;
    const toggleStyle = toggleRef.current.style;

    if (isXaxis()) {
      if (dragStyle.width === HIDDEN_WIDTH) {
        dragStyle.width = prevSizeRef.current || DEFAULT_WIDTH;
        toggleStyle.left = `calc(${dragStyle.width} - 8px)`;
        setIsDragDisabled(false);
      } else {
        prevSizeRef.current = dragStyle.width;
        dragStyle.width = HIDDEN_WIDTH;
        toggleStyle.left = `calc(${HIDDEN_WIDTH} - 8px)`;
        setIsDragDisabled(true);
      }
    } else if (dragStyle.height === HIDDEN_HEIGHT) {
      dragStyle.height = prevSizeRef.current || DEFAULT_HEIGHT;
      dragStyle.overflow = "auto";
      toggleStyle.bottom = `calc(${dragStyle.height} - 18px)`;
      setIsDragDisabled(false);
    } else {
      prevSizeRef.current = dragStyle.height;
      dragStyle.height = HIDDEN_HEIGHT;
      dragStyle.overflow = "hidden";
      toggleStyle.bottom = `calc(${HIDDEN_HEIGHT} - 18px)`;
      setIsDragDisabled(true);
    }

    dispatchResizeEvent();
  }, [isXaxis]);

  /* ---------- expose expand method via ref ---------- */
  useImperativeHandle(
    ref,
    () => ({
      expand: () => {
        if (isDragDisabled) {
          toggleVisibility();
        }
      },
      isCollapsed: () => isDragDisabled,
    }),
    [isDragDisabled, toggleVisibility]
  );

  const dragComponent = () => (
    <Typography.Text
      draggable={false}
      style={draggerStyle}
      onMouseDown={onMouseDown}
    />
  );

  /* ---------- resize event ---------- */
  function dispatchResizeEvent() {
    setTimeout(() => {
      window.dispatchEvent(
        new CustomEvent("resize", {
          detail: {
            width: dragRef.current.style.width,
            height: dragRef.current.style.height,
          },
        })
      );
    }, 200);
  }

  /* ---------- render ---------- */
  return (
    <div
      ref={dragRef}
      className={`resizeComponent ${isDragDisabled ? "hide" : ""}`}
      style={{
        ...resizeComponentStyle,
        width,
        height,
        ...style,
      }}
    >
      {/* vertical controls */}
      {!isXaxis() &&
        (() => {
          const currentH = parseInt(
            dragRef.current?.style.height || height,
            10
          );
          const maxH = Math.floor(window.innerHeight * 0.85);
          const isAtMax = currentH >= maxH;

          return (
            <Typography.Text
              ref={toggleRef}
              style={{
                bottom: `calc(${
                  dragRef.current?.style.height || height
                } - 14px)`,
                opacity: isResizing ? 0 : 1,
              }}
              className="toggleBtn vertical-resize-controls"
            >
              {isAtMax ? (
                <DownCircleFilled
                  className="toggleIcon"
                  onClick={() => {
                    dragRef.current.style.height = DEFAULT_HEIGHT;
                    dispatchResizeEvent();
                  }}
                />
              ) : (
                <UpCircleFilled
                  className="toggleIcon"
                  onClick={() => {
                    dragRef.current.style.height =
                      currentH < parseInt(DEFAULT_HEIGHT, 10)
                        ? DEFAULT_HEIGHT
                        : `${maxH}px`;
                    dispatchResizeEvent();
                  }}
                />
              )}
              <CloseCircleFilled
                className="toggleIcon"
                onClick={() => {
                  dragRef.current.style.height = HIDDEN_HEIGHT;
                  dispatchResizeEvent();
                }}
              />
            </Typography.Text>
          );
        })()}

      {!disabled && !isXaxis() && dragComponent()}
      {children}
      {!disabled && isXaxis() && dragComponent()}

      {/* horizontal resize grip handle */}
      {isXaxis() && !isDragDisabled && (
        <div
          className="resize-grip-handle"
          onMouseDown={onMouseDown}
          style={{ opacity: isResizing ? 0 : 0.6 }}
        />
      )}

      {/* horizontal controls */}
      {isXaxis() && (
        <Typography.Text
          ref={toggleRef}
          style={{
            position: "absolute",
            left: `calc(${dragRef.current?.style.width || width} - 8px)`,
            top: "10px",
            opacity: isResizing ? 0 : 1,
            zIndex: 100,
            cursor: "pointer",
          }}
          className="toggleBtn"
          onClick={toggleVisibility}
        >
          {isDragDisabled ? <RightCircleFilled /> : <LeftCircleFilled />}
        </Typography.Text>
      )}
    </div>
  );
});

ResizerComponent.propTypes = {
  children: PropTypes.oneOfType([PropTypes.node, PropTypes.element]),
  width: PropTypes.string,
  limit: PropTypes.number,
  height: PropTypes.string,
  axis: PropTypes.string,
  disabled: PropTypes.bool,
  defaultCollapsed: PropTypes.bool,
  style: PropTypes.object,
  forceCollapse: PropTypes.bool,
};

/* helpers */
function getResizeComponentStyle(axis, defaultCollapsed) {
  return axis === RESIZE_AXIS.X
    ? {
        display: "grid",
        gridAutoFlow: "column",
        gridTemplateColumns: `1fr ${DRAGGER_THICKNESS.X}px`,
        overflow: defaultCollapsed ? "hidden" : "auto",
      }
    : {
        display: "grid",
        gridAutoFlow: "row",
        gridTemplateRows: `${DRAGGER_THICKNESS.Y}px 1fr`,
        overflow: defaultCollapsed ? "hidden" : "auto",
      };
}

function getDraggerStyle(axis) {
  return axis === RESIZE_AXIS.X
    ? {
        width: `${DRAGGER_THICKNESS.X}px`,
        cursor: "ew-resize",
        borderInlineEnd: "1px solid var(--border-color-1)",
      }
    : {
        height: `${DRAGGER_THICKNESS.Y}px`,
        cursor: "ns-resize",
        borderBlockStart: "1px solid var(--border-color-1)",
      };
}

export { ResizerComponent, RESIZE_AXIS };
