import datetime
import unittest

import ibis
import pytest

from formulasql.formulasql import FormulaSQL


class TestFormulaSQLMath:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = ibis.sqlite.connect("formulasql/tests/db_data/geography.db")
        self.countries = self.connection.table("countries")

    def test_abs(self):
        formula = FormulaSQL(self.countries, "test_col", "=ABS(-1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=ABS(1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=ABS(-10.34)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 10.34

    def test_acos(self):
        formula = FormulaSQL(self.countries, "test_col", "=ACOS(1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0.0

    def test_asin(self):
        formula = FormulaSQL(self.countries, "test_col", "=ASIN(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0.0

    def test_atan(self):
        formula = FormulaSQL(self.countries, "test_col", "=ATAN(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0.0

    def test_atan2(self):
        formula = FormulaSQL(self.countries, "test_col", "=ATAN2(0, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0.0

    def test_bitand(self):
        formula = FormulaSQL(self.countries, "test_col", "=BITAND(1, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=BITAND(1, 0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0

    def test_bitor(self):
        formula = FormulaSQL(self.countries, "test_col", "=BITOR(1, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=BITOR(1, 0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1

    def test_bitxor(self):
        formula = FormulaSQL(self.countries, "test_col", "=BITXOR(1, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=BITXOR(1, 0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1

    def test_bitlshift(self):
        formula = FormulaSQL(self.countries, "test_col", "=BITLSHIFT(1, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 2
        formula = FormulaSQL(self.countries, "test_col", "=BITLSHIFT(1, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 8

    def test_bitrshift(self):
        formula = FormulaSQL(self.countries, "test_col", "=BITRSHIFT(2, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=BITRSHIFT(8, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1

    def test_ceil(self):
        formula = FormulaSQL(self.countries, "test_col", "=CEILING(1.1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 2
        formula = FormulaSQL(self.countries, "test_col", "=CEILING(-1.1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -1

    def test_cos(self):
        formula = FormulaSQL(self.countries, "test_col", "=COS(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1.0

    def test_cot(self):
        formula = FormulaSQL(self.countries, "test_col", "=COT(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == float("inf")

    def test_degrees(self):
        formula = FormulaSQL(self.countries, "test_col", "=DEGREES(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0.0
        formula = FormulaSQL(self.countries, "test_col", "=DEGREES(22/7)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 180.07244989825872

    def test_delta(self):
        formula = FormulaSQL(self.countries, "test_col", "=DELTA(10, 10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=DELTA(1, 0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0

    def test_even(self):
        formula = FormulaSQL(self.countries, "test_col", "=EVEN(10.5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 12
        formula = FormulaSQL(self.countries, "test_col", "=EVEN(11)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 12
        formula = FormulaSQL(self.countries, "test_col", "=EVEN(-13.5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -14

    def test_odd(self):
        formula = FormulaSQL(self.countries, "test_col", "=ODD(1.5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3
        formula = FormulaSQL(self.countries, "test_col", "=ODD(3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3
        formula = FormulaSQL(self.countries, "test_col", "=ODD(-2)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -3

    def test_exp(self):
        formula = FormulaSQL(self.countries, "test_col", "=EXP(1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 2.718281828459045
        formula = FormulaSQL(self.countries, "test_col", "=EXP(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1

    def test_floor(self):
        formula = FormulaSQL(self.countries, "test_col", "=FLOOR(1.1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=FLOOR(-1.1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -2

    def test_int(self):
        formula = FormulaSQL(self.countries, "test_col", "=INT(8.9)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 8
        formula = FormulaSQL(self.countries, "test_col", "=INT(-8.9)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -9

    def test_ln(self):
        formula = FormulaSQL(self.countries, "test_col", "=LN(1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=LN(2.718281828459045)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1

    def test_log(self):
        formula = FormulaSQL(self.countries, "test_col", "=LOG(1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=LOG(10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=LOG(100, 10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 2

    def test_max(self):
        countries_x = self.countries.mutate(ibis.literal("2024-02-02", type="date").name("date_1")).mutate(
            ibis.literal("2024-02-03", type="date").name("date_2")
        )
        formula = FormulaSQL(countries_x, "test_col", "=MAX(date_1, date_2)")
        new_col = formula.ibis_column()
        countries_x = countries_x.mutate(new_col)
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == datetime.date(2024, 2, 3)

        # test for max/min of None and date type column
        # right now behavior max(None,date_colum) varies on backend database.
        # this test is on sqlite which return None from above example
        # but for other db like duckdb, it returns valid date field
        countries_y = self.countries.mutate(ibis.literal(None, type="date").name("date_1")).mutate(
            ibis.literal("2024-02-03", type="date").name("date_2")
        )
        formula = FormulaSQL(countries_y, "test_col", "=MAX(date_1, date_2)")
        new_col = formula.ibis_column()
        countries_y = countries_y.mutate(new_col)
        row = countries_y["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == None  # for sqlite
        # assert (row['test_col'] == datetime.date(2024, 2, 3)) # for duckdb

        formula = FormulaSQL(self.countries, "test_col", "=MAX(name, continent)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == "EU"

        formula = FormulaSQL(self.countries, "test_col", "=MAX(1, 23, 3, 4, 5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 23

        formula = FormulaSQL(self.countries, "test_col", "=MAX(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 10

    def test_min(self):
        countries_x = self.countries.mutate(ibis.literal("2024-02-02", type="date").name("date_1")).mutate(
            ibis.literal("2024-02-03", type="date").name("date_2")
        )
        formula = FormulaSQL(countries_x, "test_col", "=MIN(date_1, date_2)")
        new_col = formula.ibis_column()
        countries_x = countries_x.mutate(new_col)
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == datetime.date(2024, 2, 2)

        formula = FormulaSQL(self.countries, "test_col", "=MIN(name, continent)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == "Andorra"

        # test for max/min of None and date type column
        # right now behavior min(None,date_colum) varies on backend database.
        # this test is on sqlite which return None from above example
        # but for other db like duckdb, it returns valid date field
        countries_y = self.countries.mutate(ibis.literal(None, type="date").name("date_1")).mutate(
            ibis.literal("2024-02-03", type="date").name("date_2")
        )
        formula = FormulaSQL(countries_y, "test_col", "=MIN(date_1, date_2)")
        new_col = formula.ibis_column()
        countries_y = countries_y.mutate(new_col)
        row = countries_y["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] is None  # for sqlite
        # assert (row['test_col'] == datetime.date(2024, 2, 3)) # for duckdb

        formula = FormulaSQL(self.countries, "test_col", "=MIN(10, 20, 3, 4, 5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3
        formula = FormulaSQL(self.countries, "test_col", "=MIN(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1

    def test_mod(self):
        formula = FormulaSQL(self.countries, "test_col", "=MOD(10, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=MOD(10, 3.5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3

    def test_modulus(self):
        formula = FormulaSQL(self.countries, "test_col", "=MODULUS(10, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1
        formula = FormulaSQL(self.countries, "test_col", "=MODULUS(10, 3.5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3

    def test_pi(self):
        formula = FormulaSQL(self.countries, "test_col", "=PI()")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3.141592653589793

    def test_power(self):
        formula = FormulaSQL(self.countries, "test_col", "=POWER(2, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 8
        formula = FormulaSQL(self.countries, "test_col", "=POWER(2, 3.5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 11.313708498984761

    def test_product(self):
        formula = FormulaSQL(self.countries, "test_col", "=PRODUCT(2, 3, 4)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 24
        formula = FormulaSQL(self.countries, "test_col", "=PRODUCT(2, 3.5, 1.0, 10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 70

    def test_quotient(self):
        formula = FormulaSQL(self.countries, "test_col", "=QUOTIENT(10, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3
        formula = FormulaSQL(self.countries, "test_col", "=QUOTIENT(10, 3.5)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 2

    def test_radians(self):
        formula = FormulaSQL(self.countries, "test_col", "=RADIANS(180)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3.141592653589793

    def test_round(self):
        formula = FormulaSQL(self.countries, "test_col", "=ROUND(10.5, 0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 11
        formula = FormulaSQL(self.countries, "test_col", "=ROUND(10.543, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 10.5

    def test_rounddown(self):
        formula = FormulaSQL(self.countries, "test_col", "=ROUNDDOWN(3.2, 0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3
        formula = FormulaSQL(self.countries, "test_col", "=ROUNDDOWN(3.14159, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3.141
        formula = FormulaSQL(self.countries, "test_col", "=ROUNDDOWN(-3.14159, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -3.2  # -3.1 is expected in Excel. Not supported
        formula = FormulaSQL(self.countries, "test_col", "=ROUNDDOWN(31415.92654, -2)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 31400

    def test_roundup(self):
        formula = FormulaSQL(self.countries, "test_col", "=ROUNDUP(3.2, 0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 4
        formula = FormulaSQL(self.countries, "test_col", "=ROUNDUP(3.14159, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 3.142
        formula = FormulaSQL(self.countries, "test_col", "=ROUNDUP(-3.14159, 1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -3.1  # -3.2 is expected in Excel. Not supported
        formula = FormulaSQL(self.countries, "test_col", "=ROUNDUP(31415.92654, -2)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 31500

    def test_sign(self):
        formula = FormulaSQL(self.countries, "test_col", "=SIGN(-10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -1
        formula = FormulaSQL(self.countries, "test_col", "=SIGN(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=SIGN(10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1

    def test_sin(self):
        formula = FormulaSQL(self.countries, "test_col", "=SIN(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=SIN(1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0.8414709848078965

    def test_sqrt(self):
        formula = FormulaSQL(self.countries, "test_col", "=SQRT(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=SQRT(4)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 2

    def test_sqrtpi(self):
        formula = FormulaSQL(self.countries, "test_col", "=SQRTPI(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=SQRTPI(4)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 6.283185307179586

    def test_sum(self):
        formula = FormulaSQL(self.countries, "test_col", "=SUM(1, 2, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 6
        formula = FormulaSQL(self.countries, "test_col", "=SUM(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 55

    def test_sumsq(self):
        formula = FormulaSQL(self.countries, "test_col", "=SUMSQ(1, 2, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 14
        formula = FormulaSQL(self.countries, "test_col", "=SUMSQ(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 385

    def test_tan(self):
        formula = FormulaSQL(self.countries, "test_col", "=TAN(0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=TAN(1)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 1.5574077246549023

    def test_trunc(self):
        formula = FormulaSQL(self.countries, "test_col", "=TRUNC(8.9,0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 8
        formula = FormulaSQL(self.countries, "test_col", "=TRUNC(-8.9,0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -8
        formula = FormulaSQL(self.countries, "test_col", "=TRUNC(0.234, 0)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 0
        formula = FormulaSQL(self.countries, "test_col", "=TRUNC(8.238, 2)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 8.23
        formula = FormulaSQL(self.countries, "test_col", "=TRUNC(-8.238, 2)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == -8.23

    def test_average(self):
        formula = FormulaSQL(self.countries, "test_col", "=AVERAGE(1, 2, 3)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 2
        formula = FormulaSQL(self.countries, "test_col", "=AVERAGE(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)")
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x["name", "continent", "population", "area_km2", "test_col"].head().execute().iloc[0]
        assert row["test_col"] == 5.5


if __name__ == "__main__":
    unittest.main()
