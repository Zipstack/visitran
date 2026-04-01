import logging
import os
from typing import Any

import openai

from backend.application.context.application import ApplicationContext

logger = logging.getLogger(__name__)


class FormulaContext(ApplicationContext):
    """Context class for handling formula generation."""

    def __init__(self, project_id: str) -> None:
        super().__init__(project_id)
        # Cache environment variables
        self.openai_api_key = os.environ.get("OPEN_AI")
        self.model = os.environ.get("MODEL")
        self.max_tokens = int(os.environ.get("MAX_TOKEN", 100))
        self.temperature = float(os.environ.get("TEMPERATURE", 0.5))
        self.formulas = os.environ.get("FORMULA", "").split(",")

    def get_schema_details(self, model_name: str) -> list[dict[str, Any]]:
        no_code_model: dict = self.session.fetch_model_data(model_name=model_name)
        source_schema_name: str = no_code_model.get("source", {}).get("schema_name", "")
        source_table_name: str = no_code_model.get("source", {}).get("table_name", "")
        destination_schema_name: str = no_code_model.get("destination", {}).get("schema_name", "")
        destination_table_name: str = no_code_model.get("destination", {}).get("table_name", "")

        # Fetching the source and destination table columns
        column_details: list[dict[str, Any]] = []
        column_details.extend(self.get_table_columns(source_schema_name, source_table_name))
        column_details.extend(self.get_table_columns(destination_schema_name, destination_table_name))

        return column_details

    def generate_formula(self, prompt: str) -> str:
        try:
            openai.api_key = self.openai_api_key
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
            # Extract and return the result
            return response["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"Error in ChatGPT API call: {e}")
            return ""

    def construct_prompt(self, user_prompt: str, schema_details: list[dict[str, Any]]) -> str:
        # Convert schema list to a readable format
        schema_description = "\n".join(
            [f"Column Name: {col['column_name']}, Data Type: {col['data_type']}" for col in schema_details]
        )
        # Prepare the ChatGPT prompt
        prompt = f"""
        You are an Excel transformation assistant. Your task is to generate Excel formulas for requested
         transformations based on a schema and with supported formulas.

        Schema:
        {schema_description}

        Supported Formulas:
        {', '.join(self.formulas)}

        Transformation Request:
        {user_prompt}

        Requirements:
        - Only use the formulas listed in the supported formulas. Do not use any formula not mentioned in the
         allowed formulas list.
        - If the operation cannot be performed due to a data type mismatch, return 'Data type Mismatch'.
        - Do not add unnecessary checks (e.g., ISNUMBER for string columns).
        - Use the column name directly in the formula (e.g., `=AVERAGE(Customer_ID)`), and **do not** use cell
         reference notations (e.g., `=AVERAGE(B2:B100)`).
        - Do not include the equal sign (`=`) in the formula.
        - Remove all double quotes from the formula.
        - Only return the formula or result. Do not include explanations or commentary.
        """
        return prompt.strip()
