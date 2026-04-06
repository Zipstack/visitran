import unittest

import ibis
import pandas as pd
import pytest
from formulasql.formulasql import FormulaSQL


# Reference Table:
#                    name continent  population  area_km2
# 0               Andorra        EU       84000     468.0
# 1  United Arab Emirates        AS     4975593   82880.0
# 2         Afghanistan          AS    29121286  647500.0
# 3   Antigua and Barbuda        NA       86754     443.0
# 4              Anguilla        NA       13254     102.0

class TestFormulaSQLText:
    @pytest.fixture(autouse=True)
    def setup(self,mysql_sakila_db):
        db = mysql_sakila_db
        self.connection = ibis.sqlite.connect('formulasql/tests/db_data/geography.db')
        self.countries = self.connection.table('countries')

        # MySQL database for proper date functions
        # Database: sakila
        password = db.password
        user = db.user
        host = db.ip
        port = db.port
        self.connection_mysql = ibis.mysql.connect(host=host, port=port, user=user, password=password,
                                                   database='sakila')
        self.payment = self.connection_mysql.table('payment')



    def test_numbervalue(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=NUMBERVALUE("84000")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 84000)
        formula = FormulaSQL(self.countries, 'test_col1', '=NUMBERVALUE("TEST")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 0)

    def test_clean(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=CLEAN("Visitran \rsays\n\t hello world!")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran says hello world!')

    def test_code(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=CODE("Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 86)
        formula = FormulaSQL(self.countries, 'test_col1', '=CODE("Ʌisitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 581)

    def test_concatenate(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=CONCATENATE("Visitran", " says hello world!")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran says hello world!')
        formula = FormulaSQL(self.countries, 'test_col1', '=CONCATENATE(name, " has ", population, " people")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Andorra has 84000 people')

    def test_concat(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=CONCAT("Visitran", " says hello world!")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran says hello world!')
        formula = FormulaSQL(self.countries, 'test_col1', '=CONCAT(name, " has ", population, " people")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Andorra has 84000 people')

    def text_exact(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=EXACT("Visitran", "Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== True)
        formula = FormulaSQL(self.countries, 'test_col1', '=EXACT("Visitran", "visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)

    def test_find(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=FIND("ran", "Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 6)
        # Start position is not supported
        # formula = FormulaSQL(self.countries, 'test_col1', '=FIND("ran", "Visitran", 6)')
        # countries_x = self.countries.mutate(formula.ibis_column())
        # row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        # assert (row['test_col1']== 0)
        # formula = FormulaSQL(self.countries, 'test_col1', '=FIND("ran", "Visitran", 2)')
        # countries_x = self.countries.mutate(formula.ibis_column())
        # row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        # assert (row['test_col1']== 6)

    def test_fixed(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=FIXED(123.456, 2)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== '123.46')
        formula = FormulaSQL(self.countries, 'test_col1', '=FIXED(123.456)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== '123')

    def test_left(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=LEFT("Visitran", 3)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Vis')
        formula = FormulaSQL(self.countries, 'test_col1', '=LEFT("Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'V')

    def test_right(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=RIGHT("Visitran", 3)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'ran')
        formula = FormulaSQL(self.countries, 'test_col1', '=RIGHT("Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'n')

    def test_mid(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=MID("Visitran", 3, 3)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'sit')
        formula = FormulaSQL(self.countries, 'test_col1', '=MID("Visitran", 3, 100)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'sitran')

    def test_len(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=LEN("Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 8)

    def test_length(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=LENGTH("Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 8)

    def test_lower(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=LOWER("Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'visitran')

    def test_upper(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=UPPER("Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'VISITRAN')

    def test_proper(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=PROPER("visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran')
        formula = FormulaSQL(self.countries, 'test_col1', '=PROPER("visitran says hello")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran says hello')

    def test_rept(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=REPT("visitran", 3)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'visitranvisitranvisitran')

    def test_search(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=SEARCH("ran", "visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 6)
        formula = FormulaSQL(self.countries, 'test_col1', '=SEARCH("isi", "visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 2)
        formula = FormulaSQL(self.countries, 'test_col1', '=SEARCH("rax", "visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 0)

    def test_substitute_and_cast(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=CAST(SUBSTITUTE(" $123.55 ", "$", ""),"float")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 123.55)

    def test_substitute_and_trim(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=TRIM(SUBSTITUTE(" $123.55 ", "$", ""))')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== '123.55')
    def test_substitute(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=SUBSTITUTE("visitran", "ran", "ranran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'visitranran')
        formula = FormulaSQL(self.countries, 'test_col1', '=SUBSTITUTE("visitran", "vis", "Vis")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran')
        formula = FormulaSQL(self.countries, 'test_col1', '=SUBSTITUTE("visitran", "i", "I")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'vIsItran')
        formula = FormulaSQL(self.countries, 'test_col1', '=SUBSTITUTE("visitran", "x", "X")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'visitran')

    def test_trim(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=TRIM("   visitran   ")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'visitran')

    def test_contains(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=CONTAINS("visitran", "ran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== True)
        formula = FormulaSQL(self.countries, 'test_col1', '=CONTAINS("visitran", "ranran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)
        formula = FormulaSQL(self.countries, 'test_col1', '=CONTAINS("visitran", "ranranran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)

    def test_endswith(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=ENDSWITH("visitran", "ran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== True)
        formula = FormulaSQL(self.countries, 'test_col1', '=ENDSWITH("visitran", "ranran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)
        formula = FormulaSQL(self.countries, 'test_col1', '=ENDSWITH("visitran", "ranranran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)

    def test_startswith(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=STARTSWITH("visitran", "vis")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== True)
        formula = FormulaSQL(self.countries, 'test_col1', '=STARTSWITH("visitran", "ran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)
        formula = FormulaSQL(self.countries, 'test_col1', '=STARTSWITH("visitran", "ranran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)

    def test_like(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=LIKE("visitran", "vis%")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== True)
        formula = FormulaSQL(self.countries, 'test_col1', '=LIKE("visitran", "ran%")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)
        formula = FormulaSQL(self.countries, 'test_col1', '=LIKE("visitran", "ranran%")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)

    def test_ilike(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=ILIKE("visitran", "VIS%")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== True)
        formula = FormulaSQL(self.countries, 'test_col1', '=ILIKE("visitran", "RAN%")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)
        formula = FormulaSQL(self.countries, 'test_col1', '=ILIKE("visitran", "RANRAN%")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)

    def test_join(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=JOIN(":", "Hello", "World")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Hello:World')

    def test_lpad(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=LPAD("Visitran", 15,"*")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== '*******Visitran')

    def test_rpad(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=RPAD("Visitran", 15,"*")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran*******')

    def test_ltrim(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=LTRIM("     Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran')

    def test_rtrim(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=RTRIM("Visitran     ")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Visitran')

    def test_regex_extract(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=REGEX_EXTRACT("Visitran", "^(Vi)",0)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Vi')

    def test_regex_replace(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=REGEX_REPLACE("Visitran", "^(Vi)", "Hello")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'Hellositran')

    def test_regex_search(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=REGEX_SEARCH("Visitran", "^(Vi)")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== True)
        formula = FormulaSQL(self.countries, 'test_col1', '=REGEX_SEARCH("Visitran", "^(Vx)")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== False)

    def test_reverse(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=REVERSE("Visitran")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 'nartisiV')

    def test_timestamp(self):

        formula = FormulaSQL(self.payment, 'test_col1', '=TIMESTAMP("2019-06-05","%Y-%m-%d")')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== pd.Timestamp('2019-06-05 00:00:00+0000', tz='UTC'))


