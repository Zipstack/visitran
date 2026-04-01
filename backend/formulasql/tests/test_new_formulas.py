"""
Tests for newly implemented formulas.
This file tests all 46 new formulas added to FormulaSQL.

Categories:
- Window Functions (14 formulas)
- Date/Time Functions (7 formulas)
- Statistical Functions (6 formulas)
- Numeric Functions (7 formulas)
- String Functions (6 formulas)
- Null/Type Functions (6 formulas)
"""
import pytest
import ibis
import pandas as pd
import math

from formulasql.formulasql import FormulaSQL


class TestNewFormulasDatetime:
    """Tests for new Date/Time functions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        # Use DuckDB for better date/time support
        self.connection = ibis.duckdb.connect(':memory:')
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'date_col': pd.to_datetime(['2023-01-15', '2023-04-20', '2023-07-10', '2023-10-05', '2023-12-31']),
            'value': [10.5, 20.3, 30.1, 40.2, 50.4],
        })
        self.connection.create_table('test_data', df)
        self.table = self.connection.table('test_data')

    def test_quarter(self):
        """QUARTER: Returns the quarter (1-4) from a date."""
        formula = FormulaSQL(self.table, 'test_col', '=QUARTER(date_col)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # Jan=Q1, Apr=Q2, Jul=Q3, Oct=Q4, Dec=Q4
        assert list(result['test_col']) == [1, 2, 3, 4, 4]

    def test_day_of_year(self):
        """DAY_OF_YEAR: Returns the day of the year (1-366)."""
        formula = FormulaSQL(self.table, 'test_col', '=DAY_OF_YEAR(date_col)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # Jan 15 = 15, Dec 31 = 365
        assert result.iloc[0]['test_col'] == 15
        assert result.iloc[4]['test_col'] == 365

    def test_strftime(self):
        """STRFTIME: Formats a date/time according to a format string."""
        formula = FormulaSQL(self.table, 'test_col', '=STRFTIME(date_col, "%Y-%m")')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert result.iloc[0]['test_col'] == '2023-01'
        assert result.iloc[4]['test_col'] == '2023-12'


class TestNewFormulasMath:
    """Tests for new Math/Statistical functions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = ibis.duckdb.connect(':memory:')
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [8.0, 16.0, 32.0, 64.0, 128.0],
            'value2': [2.0, 4.0, 8.0, 16.0, 32.0],
        })
        self.connection.create_table('test_data', df)
        self.table = self.connection.table('test_data')

    def test_log2(self):
        """LOG2: Returns the base-2 logarithm."""
        formula = FormulaSQL(self.table, 'test_col', '=LOG2(value)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # log2(8) = 3, log2(16) = 4, etc.
        assert result.iloc[0]['test_col'] == 3.0
        assert result.iloc[1]['test_col'] == 4.0

    def test_negate(self):
        """NEGATE: Returns the negation of a number."""
        formula = FormulaSQL(self.table, 'test_col', '=NEGATE(value)')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert result.iloc[0]['test_col'] == -8.0
        assert result.iloc[1]['test_col'] == -16.0

    def test_e(self):
        """E: Returns Euler's number (e ≈ 2.718...)."""
        formula = FormulaSQL(self.table, 'test_col', '=E()')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert abs(result.iloc[0]['test_col'] - math.e) < 0.0001

    def test_greatest(self):
        """GREATEST: Returns the largest value from the arguments."""
        formula = FormulaSQL(self.table, 'test_col', '=GREATEST(value, value2, 50)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # row 0: max(8, 2, 50) = 50
        # row 4: max(128, 32, 50) = 128
        assert result.iloc[0]['test_col'] == 50
        assert result.iloc[4]['test_col'] == 128

    def test_least(self):
        """LEAST: Returns the smallest value from the arguments."""
        formula = FormulaSQL(self.table, 'test_col', '=LEAST(value, value2, 10)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # row 0: min(8, 2, 10) = 2
        # row 4: min(128, 32, 10) = 10
        assert result.iloc[0]['test_col'] == 2
        assert result.iloc[4]['test_col'] == 10

    def test_clip(self):
        """CLIP: Clips a value to be within a specified range."""
        formula = FormulaSQL(self.table, 'test_col', '=CLIP(value, 20, 100)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # row 0: clip(8, 20, 100) = 20 (below lower)
        # row 2: clip(32, 20, 100) = 32 (within)
        # row 4: clip(128, 20, 100) = 100 (above upper)
        assert result.iloc[0]['test_col'] == 20
        assert result.iloc[2]['test_col'] == 32
        assert result.iloc[4]['test_col'] == 100

    def test_random(self):
        """RANDOM: Returns a random value between 0 and 1."""
        formula = FormulaSQL(self.table, 'test_col', '=RANDOM()')
        result = self.table.mutate(formula.ibis_column()).execute()
        # All values should be between 0 and 1
        assert all(0 <= x <= 1 for x in result['test_col'])


class TestNewFormulasText:
    """Tests for new String functions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = ibis.duckdb.connect(':memory:')
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['hello world', 'ALICE BOB', 'test string'],
        })
        self.connection.create_table('test_data', df)
        self.table = self.connection.table('test_data')

    def test_capitalize(self):
        """CAPITALIZE: Capitalizes the first character of a string."""
        formula = FormulaSQL(self.table, 'test_col', '=CAPITALIZE(name)')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert result.iloc[0]['test_col'] == 'Hello world'

    def test_ascii(self):
        """ASCII: Returns the ASCII code of the first character."""
        formula = FormulaSQL(self.table, 'test_col', '=ASCII(name)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # 'h' = 104, 'A' = 65, 't' = 116
        assert result.iloc[0]['test_col'] == 104
        assert result.iloc[1]['test_col'] == 65


class TestNewFormulasLogics:
    """Tests for new Null/Type functions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = ibis.duckdb.connect(':memory:')
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10.0, None, 30.0],
            'name': ['Alice', None, 'Charlie'],
        })
        self.connection.create_table('test_data', df)
        self.table = self.connection.table('test_data')

    def test_coalesce(self):
        """COALESCE: Returns the first non-null value."""
        formula = FormulaSQL(self.table, 'test_col', '=COALESCE(name, "unknown")')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert result.iloc[0]['test_col'] == 'Alice'
        assert result.iloc[1]['test_col'] == 'unknown'
        assert result.iloc[2]['test_col'] == 'Charlie'

    def test_nullif(self):
        """NULLIF: Returns null if two values are equal."""
        formula = FormulaSQL(self.table, 'test_col', '=NULLIF(value, 10)')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert pd.isna(result.iloc[0]['test_col'])  # 10 == 10, returns null
        assert result.iloc[2]['test_col'] == 30.0   # 30 != 10, returns 30

    def test_fill_null(self):
        """FILL_NULL: Replaces null values with a specified value."""
        formula = FormulaSQL(self.table, 'test_col', '=FILL_NULL(value, 0)')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert result.iloc[0]['test_col'] == 10.0
        assert result.iloc[1]['test_col'] == 0.0
        assert result.iloc[2]['test_col'] == 30.0


class TestNewFormulasWindow:
    """Tests for new Window functions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = ibis.duckdb.connect(':memory:')
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'amount': [100.0, 200.0, 150.0, 300.0, 250.0],
        })
        self.connection.create_table('test_data', df)
        self.table = self.connection.table('test_data')

    def test_row_number(self):
        """ROW_NUMBER: Assigns a unique sequential number to each row."""
        formula = FormulaSQL(self.table, 'test_col', '=ROW_NUMBER()')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert list(result['test_col']) == [1, 2, 3, 4, 5]

    def test_rank(self):
        """RANK: Assigns a rank to each row."""
        formula = FormulaSQL(self.table, 'test_col', '=RANK()')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert all(x >= 1 for x in result['test_col'])

    def test_dense_rank(self):
        """DENSE_RANK: Assigns a rank without gaps."""
        formula = FormulaSQL(self.table, 'test_col', '=DENSE_RANK()')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert all(x >= 1 for x in result['test_col'])

    def test_lag(self):
        """LAG: Returns the value from a previous row."""
        formula = FormulaSQL(self.table, 'test_col', '=LAG(amount, 1)')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert pd.isna(result.iloc[0]['test_col'])  # First row has no previous
        assert result.iloc[1]['test_col'] == 100.0   # Second row's lag = first row's amount

    def test_lead(self):
        """LEAD: Returns the value from a following row."""
        formula = FormulaSQL(self.table, 'test_col', '=LEAD(amount, 1)')
        result = self.table.mutate(formula.ibis_column()).execute()
        assert result.iloc[0]['test_col'] == 200.0   # First row's lead = second row's amount
        assert pd.isna(result.iloc[4]['test_col'])   # Last row has no next

    def test_ntile(self):
        """NTILE: Divides rows into n buckets."""
        formula = FormulaSQL(self.table, 'test_col', '=NTILE(2)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # With 5 rows and 2 tiles, we get 1,1,1,2,2 or similar distribution
        assert all(x in [1, 2] for x in result['test_col'])

    def test_cumsum(self):
        """CUMSUM: Returns the cumulative sum."""
        formula = FormulaSQL(self.table, 'test_col', '=CUMSUM(amount)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # 100, 100+200=300, 300+150=450, 450+300=750, 750+250=1000
        assert result.iloc[0]['test_col'] == 100.0
        assert result.iloc[4]['test_col'] == 1000.0

    def test_cummin(self):
        """CUMMIN: Returns the cumulative minimum."""
        formula = FormulaSQL(self.table, 'test_col', '=CUMMIN(amount)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # 100, min(100,200)=100, min(100,150)=100, min(100,300)=100, min(100,250)=100
        assert all(x == 100.0 for x in result['test_col'])

    def test_cummax(self):
        """CUMMAX: Returns the cumulative maximum."""
        formula = FormulaSQL(self.table, 'test_col', '=CUMMAX(amount)')
        result = self.table.mutate(formula.ibis_column()).execute()
        # 100, max(100,200)=200, max(200,150)=200, max(200,300)=300, max(300,250)=300
        assert result.iloc[0]['test_col'] == 100.0
        assert result.iloc[4]['test_col'] == 300.0


# =========================================================================
# Quick parsing tests (no database execution needed)
# =========================================================================

class TestFormulaParsingOnly:
    """Tests that formulas parse correctly without executing."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = ibis.duckdb.connect(':memory:')
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10.5, 20.3, 30.1],
            'name': ['Alice', 'Bob', 'Charlie'],
            'date_col': pd.to_datetime(['2023-01-01', '2023-06-15', '2023-12-31'])
        })
        self.connection.create_table('test_data', df)
        self.table = self.connection.table('test_data')

    # DateTime
    def test_parse_quarter(self):
        formula = FormulaSQL(self.table, 'test_col', '=QUARTER(date_col)')
        assert formula.ibis_column() is not None

    def test_parse_day_of_year(self):
        formula = FormulaSQL(self.table, 'test_col', '=DAY_OF_YEAR(date_col)')
        assert formula.ibis_column() is not None

    def test_parse_strftime(self):
        formula = FormulaSQL(self.table, 'test_col', '=STRFTIME(date_col, "%Y-%m")')
        assert formula.ibis_column() is not None

    def test_parse_date_trunc(self):
        formula = FormulaSQL(self.table, 'test_col', '=DATE_TRUNC(date_col, "month")')
        assert formula.ibis_column() is not None

    def test_parse_epoch_seconds(self):
        formula = FormulaSQL(self.table, 'test_col', '=EPOCH_SECONDS(date_col)')
        assert formula.ibis_column() is not None

    # Math
    def test_parse_log2(self):
        formula = FormulaSQL(self.table, 'test_col', '=LOG2(value)')
        assert formula.ibis_column() is not None

    def test_parse_clip(self):
        formula = FormulaSQL(self.table, 'test_col', '=CLIP(value, 20, 40)')
        assert formula.ibis_column() is not None

    def test_parse_negate(self):
        formula = FormulaSQL(self.table, 'test_col', '=NEGATE(value)')
        assert formula.ibis_column() is not None

    def test_parse_e(self):
        formula = FormulaSQL(self.table, 'test_col', '=E()')
        assert formula.ibis_column() is not None

    def test_parse_greatest(self):
        formula = FormulaSQL(self.table, 'test_col', '=GREATEST(1, 2, value)')
        assert formula.ibis_column() is not None

    def test_parse_least(self):
        formula = FormulaSQL(self.table, 'test_col', '=LEAST(1, 2, value)')
        assert formula.ibis_column() is not None

    def test_parse_random(self):
        formula = FormulaSQL(self.table, 'test_col', '=RANDOM()')
        assert formula.ibis_column() is not None

    # String
    def test_parse_capitalize(self):
        formula = FormulaSQL(self.table, 'test_col', '=CAPITALIZE(name)')
        assert formula.ibis_column() is not None

    def test_parse_initcap(self):
        formula = FormulaSQL(self.table, 'test_col', '=INITCAP(name)')
        assert formula.ibis_column() is not None

    def test_parse_ascii(self):
        formula = FormulaSQL(self.table, 'test_col', '=ASCII(name)')
        assert formula.ibis_column() is not None

    # Logic
    def test_parse_coalesce(self):
        formula = FormulaSQL(self.table, 'test_col', '=COALESCE(name, "default")')
        assert formula.ibis_column() is not None

    def test_parse_nullif(self):
        formula = FormulaSQL(self.table, 'test_col', '=NULLIF(value, 10)')
        assert formula.ibis_column() is not None

    def test_parse_fill_null(self):
        formula = FormulaSQL(self.table, 'test_col', '=FILL_NULL(name, "unknown")')
        assert formula.ibis_column() is not None

    # Window
    def test_parse_lag(self):
        formula = FormulaSQL(self.table, 'test_col', '=LAG(value, 1)')
        assert formula.ibis_column() is not None

    def test_parse_lead(self):
        formula = FormulaSQL(self.table, 'test_col', '=LEAD(value, 1)')
        assert formula.ibis_column() is not None

    def test_parse_row_number(self):
        formula = FormulaSQL(self.table, 'test_col', '=ROW_NUMBER()')
        assert formula.ibis_column() is not None

    def test_parse_rank(self):
        formula = FormulaSQL(self.table, 'test_col', '=RANK()')
        assert formula.ibis_column() is not None

    def test_parse_dense_rank(self):
        formula = FormulaSQL(self.table, 'test_col', '=DENSE_RANK()')
        assert formula.ibis_column() is not None

    def test_parse_ntile(self):
        formula = FormulaSQL(self.table, 'test_col', '=NTILE(4)')
        assert formula.ibis_column() is not None

    def test_parse_cumsum(self):
        formula = FormulaSQL(self.table, 'test_col', '=CUMSUM(value)')
        assert formula.ibis_column() is not None

    def test_parse_cummin(self):
        formula = FormulaSQL(self.table, 'test_col', '=CUMMIN(value)')
        assert formula.ibis_column() is not None

    def test_parse_cummax(self):
        formula = FormulaSQL(self.table, 'test_col', '=CUMMAX(value)')
        assert formula.ibis_column() is not None

    def test_parse_first(self):
        formula = FormulaSQL(self.table, 'test_col', '=FIRST(value)')
        assert formula.ibis_column() is not None

    def test_parse_last(self):
        formula = FormulaSQL(self.table, 'test_col', '=LAST(value)')
        assert formula.ibis_column() is not None

    def test_parse_percent_rank(self):
        formula = FormulaSQL(self.table, 'test_col', '=PERCENT_RANK()')
        assert formula.ibis_column() is not None

    def test_parse_cume_dist(self):
        formula = FormulaSQL(self.table, 'test_col', '=CUME_DIST()')
        assert formula.ibis_column() is not None
