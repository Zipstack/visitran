import { useEffect, useRef, memo, useMemo } from "react";
import PropTypes from "prop-types";
import { Typography } from "antd";

const CanvasWaveAnimation = memo(function CanvasWaveAnimation({
  height = 50,
  width,
  numOfBytes,
}) {
  const canvasRef = useRef(null);
  const animationRef = useRef(null);

  const waves = useRef([
    { color: "#40a9ff", amplitude: 20, speed: 0.04, frequency: 0.02, phase: 0 },
    { color: "#4169e1", amplitude: 15, speed: 0.03, frequency: 0.03, phase: 2 },
    {
      color: "#8a2be2",
      amplitude: 10,
      speed: 0.02,
      frequency: 0.025,
      phase: 4,
    },
  ]).current;

  // Format bytes with commas; show 'NA' for null / undefined / invalid input
  const formattedBytes = useMemo(() => {
    if (numOfBytes === null) return "NA";
    const n = Number(numOfBytes);
    return Number.isNaN(n) ? "NA" : n.toLocaleString("en-US");
  }, [numOfBytes]);

  useEffect(() => {
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");

    const updateCanvasSize = () => {
      canvas.width = width || canvas.parentElement.offsetWidth;
      canvas.height = height;
    };

    updateCanvasSize();

    const drawWave = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      waves.forEach((wave, index) => {
        ctx.beginPath();
        wave.phase += wave.speed;

        for (let x = 0; x <= canvas.width; x += 5) {
          const halfHeight = canvas.height / 2;
          const frequencyCalculation = x * wave.frequency;
          const totalAngle = frequencyCalculation + wave.phase;
          const sineValue = Math.sin(totalAngle);
          const amplitudeResult = wave.amplitude * sineValue;
          const y = halfHeight + amplitudeResult;

          x === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
        }

        ctx.lineTo(canvas.width, canvas.height);
        ctx.lineTo(0, canvas.height);
        ctx.closePath();

        // Adjust wave opacity
        ctx.fillStyle =
          wave.color + (index === 0 ? "70" : index === 1 ? "80" : "60");
        ctx.fill();
      });

      ctx.shadowColor = "#40a9ff";
      ctx.shadowBlur = 15;

      animationRef.current = requestAnimationFrame(drawWave);
    };

    animationRef.current = requestAnimationFrame(drawWave);

    window.addEventListener("resize", updateCanvasSize);

    return () => {
      cancelAnimationFrame(animationRef.current);
      window.removeEventListener("resize", updateCanvasSize);
    };
  }, [height, width, waves]);

  return (
    <div
      className="chat-ai-canvas-wave-container"
      style={{
        width: width ? `${width}px` : "100%",
        height: `${height}px`,
      }}
    >
      <canvas ref={canvasRef} className="chat-ai-canvas-wave-canvas" />
      <div className="chat-ai-canvas-wave-overlay">
        <Typography.Text className="chat-ai-canvas-wave-text">
          Bytes received: {formattedBytes}
        </Typography.Text>
      </div>
    </div>
  );
});

CanvasWaveAnimation.propTypes = {
  height: PropTypes.number,
  width: PropTypes.number,
  numOfBytes: PropTypes.number.isRequired,
};

CanvasWaveAnimation.displayName = "CanvasWaveAnimation";

export { CanvasWaveAnimation };
