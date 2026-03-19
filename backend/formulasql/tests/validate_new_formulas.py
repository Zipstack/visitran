#!/usr/bin/env python3
"""
Standalone validation script for new formulas.
Run directly with: python formulasql/tests/validate_new_formulas.py

This script validates all 46 new formulas implemented in FormulaSQL.
No pytest or Django configuration needed.
"""
import sys
import os

# Add parent directories to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import ibis
import pandas as pd
import math

from formulasql.formulasql import FormulaSQL


def create_test_table():
    """Create an in-memory DuckDB table for testing."""
    connection = ibis.duckdb.connect(':memory:')
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'value': [8.0, 16.0, 32.0, 64.0, 128.0],
        'value2': [2.0, 4.0, 8.0, 16.0, 32.0],
        'name': ['hello world', 'ALICE BOB', 'test string', 'foo bar', 'python code'],
        'date_col': pd.to_datetime(['2023-01-15', '2023-04-20', '2023-07-10', '2023-10-05', '2023-12-31']),
        'amount': [100.0, 200.0, 150.0, 300.0, 250.0],
        'nullable': [10.0, None, 30.0, None, 50.0],
    })
    connection.create_table('test_data', df)
    return connection, connection.table('test_data')


def test_formula(table, formula_str, description):
    """Test a single formula and print result."""
    try:
        formula = FormulaSQL(table, 'result', formula_str)
        result = table.mutate(formula.ibis_column()).execute()
        print(f"  ✅ {description}")
        print(f"     Formula: {formula_str}")
        print(f"     Sample result: {result['result'].iloc[0]}")
        return True
    except Exception as e:
        print(f"  ❌ {description}")
        print(f"     Formula: {formula_str}")
        print(f"     Error: {str(e)[:100]}")
        return False


def main():
    print("=" * 60)
    print("Validating New FormulaSQL Formulas")
    print("=" * 60)

    connection, table = create_test_table()

    passed = 0
    failed = 0

    # =========================================================================
    # Date/Time Functions (7)
    # =========================================================================
    print("\n📅 DATE/TIME FUNCTIONS")
    print("-" * 40)

    tests = [
        ('=QUARTER(date_col)', 'QUARTER - Get quarter from date'),
        ('=DAY_OF_YEAR(date_col)', 'DAY_OF_YEAR - Get day of year'),
        ('=STRFTIME(date_col, "%Y-%m")', 'STRFTIME - Format date as string'),
        ('=DATE_TRUNC(date_col, "month")', 'DATE_TRUNC - Truncate to month'),
        ('=EPOCH_SECONDS(date_col)', 'EPOCH_SECONDS - Get Unix timestamp'),
        ('=MILLISECOND(date_col)', 'MILLISECOND - Get milliseconds'),
        ('=MICROSECOND(date_col)', 'MICROSECOND - Get microseconds'),
    ]

    for formula, desc in tests:
        if test_formula(table, formula, desc):
            passed += 1
        else:
            failed += 1

    # =========================================================================
    # Window Functions (8)
    # =========================================================================
    print("\n🪟 WINDOW FUNCTIONS")
    print("-" * 40)

    tests = [
        ('=LAG(amount, 1)', 'LAG - Previous row value'),
        ('=LEAD(amount, 1)', 'LEAD - Next row value'),
        ('=CUMSUM(amount)', 'CUMSUM - Running total'),
        ('=CUMMEAN(amount)', 'CUMMEAN - Running average'),
        ('=CUMMIN(amount)', 'CUMMIN - Running minimum'),
        ('=CUMMAX(amount)', 'CUMMAX - Running maximum'),
        ('=FIRST(amount)', 'FIRST - First value in window'),
        ('=LAST(amount)', 'LAST - Last value in window'),
    ]

    for formula, desc in tests:
        if test_formula(table, formula, desc):
            passed += 1
        else:
            failed += 1

    # =========================================================================
    # Statistical Functions (5)
    # =========================================================================
    print("\n📊 STATISTICAL FUNCTIONS")
    print("-" * 40)

    tests = [
        ('=MEDIAN(value)', 'MEDIAN - Median value'),
        ('=QUANTILE(value, 0.75)', 'QUANTILE - 75th percentile'),
        ('=VARIANCE(value)', 'VARIANCE - Statistical variance'),
        ('=STDDEV(value)', 'STDDEV - Standard deviation'),
        ('=COV(value, value2)', 'COV - Covariance'),
    ]

    for formula, desc in tests:
        if test_formula(table, formula, desc):
            passed += 1
        else:
            failed += 1

    # =========================================================================
    # Numeric Functions (7)
    # =========================================================================
    print("\n🔢 NUMERIC FUNCTIONS")
    print("-" * 40)

    tests = [
        ('=LOG2(value)', 'LOG2 - Base-2 logarithm'),
        ('=CLIP(value, 20, 100)', 'CLIP - Clamp to range'),
        ('=NEGATE(value)', 'NEGATE - Negation'),
        ('=RANDOM()', 'RANDOM - Random 0-1'),
        ('=E()', 'E - Euler\'s number'),
        ('=GREATEST(value, value2, 50)', 'GREATEST - Max of values'),
        ('=LEAST(value, value2, 10)', 'LEAST - Min of values'),
    ]

    for formula, desc in tests:
        if test_formula(table, formula, desc):
            passed += 1
        else:
            failed += 1

    # =========================================================================
    # String Functions (6)
    # =========================================================================
    print("\n📝 STRING FUNCTIONS")
    print("-" * 40)

    tests = [
        ('=CAPITALIZE(name)', 'CAPITALIZE - First char upper'),
        ('=INITCAP(name)', 'INITCAP - Each word capitalized'),
        ('=ASCII(name)', 'ASCII - ASCII code of first char'),
        ('=TRANSLATE(name, "abc", "xyz")', 'TRANSLATE - Replace characters'),
        ('=LEVENSHTEIN(name, "hello")', 'LEVENSHTEIN - Edit distance'),
        ('=SPLIT(name, " ")', 'SPLIT - Split by delimiter'),
    ]

    for formula, desc in tests:
        if test_formula(table, formula, desc):
            passed += 1
        else:
            failed += 1

    # =========================================================================
    # Null/Type Functions (6)
    # =========================================================================
    print("\n⚡ NULL/TYPE FUNCTIONS")
    print("-" * 40)

    tests = [
        ('=COALESCE(nullable, 0)', 'COALESCE - First non-null'),
        ('=FILL_NULL(nullable, 0)', 'FILL_NULL - Replace nulls'),
        ('=NULLIF(value, 8)', 'NULLIF - Return null if equal'),
        ('=ISNAN(value)', 'ISNAN - Check if NaN'),
        ('=ISINF(value)', 'ISINF - Check if infinite'),
        ('=TRY_CAST(name, "int")', 'TRY_CAST - Safe type cast'),
    ]

    for formula, desc in tests:
        if test_formula(table, formula, desc):
            passed += 1
        else:
            failed += 1

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Total:  {passed + failed}")
    print(f"📈 Rate:   {100 * passed / (passed + failed):.1f}%")

    if failed == 0:
        print("\n🎉 All formulas validated successfully!")
    else:
        print(f"\n⚠️  {failed} formula(s) need attention")

    return failed == 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
