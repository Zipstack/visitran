import unittest

import ibis
import pytest

from formulasql.formulasql import FormulaSQL

# Reference Table:
#                    name continent  population  area_km2
# 0               Andorra        EU       84000     468.0
# 1  United Arab Emirates        AS     4975593   82880.0
# 2           Afghanistan        AS    29121286  647500.0
# 3   Antigua and Barbuda        NA       86754     443.0
# 4              Anguilla        NA       13254     102.0


class TestFormulaSQLOperators:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = ibis.sqlite.connect("formulasql/tests/db_data/geography.db")
        self.countries = self.connection.table("countries")

    def test_division(self):
        formula = FormulaSQL(self.countries, "density", "=population/area_km2/10")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "density"].head().execute().iloc[0]
        assert row["density"] == 84000 / 468 / 10

    def test_multiplication(self):
        formula = FormulaSQL(self.countries, "test_col", "=population*area_km2*10")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 84000 * 468 * 10

    def test_addition(self):
        formula = FormulaSQL(self.countries, "test_col", "=population+area_km2+10")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 84000 + 468 + 10

    def test_subtraction(self):
        formula = FormulaSQL(self.countries, "test_col", "=population-area_km2-10")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 84000 - 468 - 10

    def test_amperstand(self):
        formula = FormulaSQL(self.countries, "test_col", '= name & " " & population')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == "Andorra 84000"

    def test_equal(self):
        formula = FormulaSQL(self.countries, "test_col", "=population=84000")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True
        formula = FormulaSQL(self.countries, "test_col", "=population=84001")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False

    def test_not_equal(self):
        formula = FormulaSQL(self.countries, "test_col", "=population<>84000")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False
        formula = FormulaSQL(self.countries, "test_col", "=population<>84001")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True

    def test_greater_than(self):
        formula = FormulaSQL(self.countries, "test_col", "=population>84000")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False
        formula = FormulaSQL(self.countries, "test_col", "=population>83999")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True

    def test_greater_than_or_equal(self):
        formula = FormulaSQL(self.countries, "test_col", "=population>=84001")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False
        formula = FormulaSQL(self.countries, "test_col", "=population>=83999")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True

    def test_less_than(self):
        formula = FormulaSQL(self.countries, "test_col", "=population<84000")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False
        formula = FormulaSQL(self.countries, "test_col", "=population<84002")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True

    def test_less_than_or_equal(self):
        formula = FormulaSQL(self.countries, "test_col", "=population<=84000")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True
        formula = FormulaSQL(self.countries, "test_col", "=population<=83999")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False

    def test_and(self):
        formula = FormulaSQL(self.countries, "test_col", "=AND(population>83998,population<84001)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True
        formula = FormulaSQL(self.countries, "test_col", "=AND(population>83998,population>84001)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False

    def test_or(self):
        formula = FormulaSQL(self.countries, "test_col", "=OR(population>83998,population>84001)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True
        formula = FormulaSQL(self.countries, "test_col", "=OR(population>84001,population>84002)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False

    def test_xor(self):
        formula = FormulaSQL(self.countries, "test_col", "=XOR(population>83998,population>84001)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True
        formula = FormulaSQL(self.countries, "test_col", "=XOR(population<83998,population>83998)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True
        formula = FormulaSQL(self.countries, "test_col", "=XOR(population>83998,population>83999)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False
        formula = FormulaSQL(self.countries, "test_col", "=XOR(population<83998,population<83999)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False

    def test_not(self):
        formula = FormulaSQL(self.countries, "test_col", "=NOT(population>84001)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == True
        formula = FormulaSQL(self.countries, "test_col", "=NOT(population>83999)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == False

    def test_n(self):
        formula = FormulaSQL(self.countries, "test_col", "=N(TRUE())")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=N(FALSE())")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", '=N("34")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 34


if __name__ == "__main__":
    unittest.main()
