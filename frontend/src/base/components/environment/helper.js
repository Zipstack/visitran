// 3–100 chars, allows letters (a-z, A-Z), numbers (0-9), spaces, underscores, and hyphens.
// Disallows consecutive spaces.
const CONNECTION_NAME_REGEX = /^(?!.* {2})[A-Za-z0-9 _-]{3,100}$/;

const collapseSpaces = (value) => value.replace(/ {2,}(?=\S)/g, " ");

const validateFormFieldName = (_, value) => {
  if (!value) return Promise.resolve();
  const trimmed = value.trim();

  if (!CONNECTION_NAME_REGEX.test(trimmed)) {
    return Promise.reject(
      new Error(
        "Name must be 3–100 characters long and may include letters, numbers, spaces, underscores, and hyphens."
      )
    );
  }
  return Promise.resolve();
};
const validateFormFieldDescription = (_, value) => {
  if (!value) return Promise.resolve();
  const trimmed = value.trim();

  if (trimmed.length < 3) {
    return Promise.reject(
      new Error("Description must be at least 3 characters long.")
    );
  }
  if (trimmed.length > 500) {
    return Promise.reject(
      new Error("Description cannot exceed 500 characters.")
    );
  }
  return Promise.resolve();
};

export { validateFormFieldName, validateFormFieldDescription, collapseSpaces };
