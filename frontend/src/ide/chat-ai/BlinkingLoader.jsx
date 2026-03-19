import { memo } from "react";

const BlinkingLoader = memo(() => {
  return <span className="blinking-dot" />;
});

BlinkingLoader.displayName = "BlinkingLoader";

export { BlinkingLoader };
