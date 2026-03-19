import { render, screen } from "@testing-library/react";

import "../../../setupTests";
import { App } from "../../../app.jsx";

test("dummy test case to work with sonar", () => {
  render(<App />);
  const linkElement = screen.getByText(/Visitran/i);
  expect(linkElement).toBeInTheDocument();
});
