const formatTimeAgo = (dateString) => {
  if (!dateString) return "";
  const now = Date.now();
  const modified = new Date(dateString).getTime();
  const diffInSeconds = Math.floor((now - modified) / 1000);

  if (diffInSeconds < 60) {
    return `${diffInSeconds}s`;
  }
  const diffInMinutes = Math.floor(diffInSeconds / 60);
  if (diffInMinutes < 60) {
    return `${diffInMinutes}m`;
  }
  const diffInHours = Math.floor(diffInMinutes / 60);
  if (diffInHours < 24) {
    return `${diffInHours}h`;
  }
  const diffInDays = Math.floor(diffInHours / 24);
  if (diffInDays < 365) {
    return `${diffInDays}d`;
  }
  const diffInYears = Math.floor(diffInDays / 365);
  return `${diffInYears}y`;
};

const CHAT_INTENTS = {
  INFO: "INFO",
  SQL: "SQL",
  TRANSFORM: "TRANSFORM",
};

export { formatTimeAgo, CHAT_INTENTS };

export function isMacOS() {
  if (navigator.userAgentData?.platform) {
    return navigator.userAgentData.platform.toLowerCase().includes("mac");
  }
  return /mac/i.test(navigator.userAgent);
}
