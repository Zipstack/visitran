const { createProxyMiddleware } = require("http-proxy-middleware");

const apiProxy = createProxyMiddleware("/api/v1/", {
  target: process.env.REACT_APP_BACKEND_URL,
  changeOrigin: true,
});

module.exports = (app) => {
  app.use(apiProxy);
};
