const THEME = {
  DARK: "dark",
  LIGHT: "light",
};

const FILE_EXTENSION_VS_MONACO_LANG_MAPPING = {
  py: "python",
  md: "markdown",
  yaml: "yaml",
};

const joinTableColors = [
  "#FF4D6D",
  "#FFB400",
  "#00A6ED",
  "#7FB800",
  " #0D3A63",
  "#714DFF",
  "#FF784D",
];

const IMAGE_URLS = {
  logo: "https://storage.googleapis.com/visitran-static/logo/visitran_logo.svg",
  usaFlag: "https://storage.googleapis.com/visitran-static/flags/us.svg",
  euFlag: "https://storage.googleapis.com/visitran-static/flags/eu.svg",
  background:
    "https://storage.googleapis.com/visitran-static/logo/landing-page-img.png",
};
const getLanguageByExtension = (extension) => {
  return FILE_EXTENSION_VS_MONACO_LANG_MAPPING[extension] || extension;
};

const DRAWER_TYPES = {
  CHAT_AI: "CHAT_AI",
  SQL: "SQL",
  PYTHON: "PYTHON",
  SEQUENCE: "SEQUENCE",
  VERSION_HISTORY: "VERSION_HISTORY",
};

const runHistoryTagColor = {
  SUCCESS: "green",
  FAILURE: "red",
  RETRY: "orange",
  STARTED: "blue",
  REVOKED: "orange",
};
export {
  THEME,
  getLanguageByExtension,
  joinTableColors,
  IMAGE_URLS,
  DRAWER_TYPES,
  runHistoryTagColor,
};
