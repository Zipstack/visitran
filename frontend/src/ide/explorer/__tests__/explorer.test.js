import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { ErrorBoundary } from "../../../widgets/error_boundary";

describe("ErrorBoundary", () => {
  test("renders children when no error occurs", () => {
    render(
      <ErrorBoundary onError={() => {}}>
        <div>Test Content</div>
      </ErrorBoundary>
    );
    expect(screen.getByText("Test Content")).toBeInTheDocument();
  });

  test("renders fallback when child throws", () => {
    const ThrowError = () => {
      throw new Error("Test error");
    };
    // Suppress console.error for expected error
    const spy = jest.spyOn(console, "error").mockImplementation(() => {});
    render(
      <ErrorBoundary
        onError={() => {}}
        fallbackComponent={<div>Error occurred</div>}
      >
        <ThrowError />
      </ErrorBoundary>
    );
    expect(screen.getByText("Error occurred")).toBeInTheDocument();
    spy.mockRestore();
  });
});
