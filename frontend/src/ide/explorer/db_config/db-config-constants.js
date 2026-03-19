const FIELD_TYPES = {
  PASSWORD: "password",
  FILE: "file",
};

const DB_NO_NEED_TO_TEST = ["duckdb"];

function constructFieldsBody(fields) {
  const body = {};
  fields.forEach((field) => {
    body[field.label] = field.value;
  });
  return body;
}

export { constructFieldsBody, DB_NO_NEED_TO_TEST, FIELD_TYPES };
