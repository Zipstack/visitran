import datetime
import pytest

import ibis
import unittest
import pandas as pd

from formulasql.formulasql import FormulaSQL


# Reference Table:
#                    name continent  population  area_km2
# 0               Andorra        EU       84000     468.0
# 1  United Arab Emirates        AS     4975593   82880.0
# 2           Afghanistan        AS    29121286  647500.0
# 3   Antigua and Barbuda        NA       86754     443.0
# 4              Anguilla        NA       13254     102.0

class TestFormulaSQLDateTime:
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
    
    # def test_uniq(self):
    #     assert 1==1

    def test_date(self):
        formula = FormulaSQL(self.countries, 'test_col', '=DATE(2023,4,15)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        assert row['test_col']== datetime.date(2023, 4, 15)

    def test_day(self):
        formula = FormulaSQL(self.countries, 'test_col', '=DAY(DATE(2023,4,15))')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        assert (row['test_col'] == 15)

    def test_month(self):
        formula = FormulaSQL(self.countries, 'test_col', '=MONTH(DATE(2023,4,15))')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        assert (row['test_col']== 4)

    def test_year(self):
        formula = FormulaSQL(self.countries, 'test_col', '=YEAR(DATE(2023,4,15))')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        assert (row['test_col']== 2023)

    def test_days(self):
        formula = FormulaSQL(self.payment, 'test_col1', '=DAYS(DATE(2023,4,30),DATE(2023,3,15))')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 46)

    def test_edate(self):
        formula = FormulaSQL(self.payment, 'test_col1', '=EDATE(DATE(2023,4,30),-1)')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert row['test_col1']==datetime.date(2023, 3, 30)
        formula = FormulaSQL(self.payment, 'test_col1', '=EDATE(DATE(2023,4,30),1)')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== datetime.date(2023, 5, 30))

    def test_time(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=TIME(12,30,10)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== datetime.time(12, 30, 10))

    def test_datetime(self):
        formula = FormulaSQL(self.payment, 'test_col1', '=DATETIME(2023,4,30,13,40,20)')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== pd.Timestamp(2023, 4, 30, 13, 40, 20))

    def test_hour(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=HOUR(TIME(12,30,10))')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 12)
        formula = FormulaSQL(self.payment, 'test_col1', '=HOUR(DATETIME(2023,4,30,13,40,20))')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 13)

    def test_minute(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=MINUTE(TIME(12,30,10))')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 30)
        formula = FormulaSQL(self.payment, 'test_col1', '=MINUTE(DATETIME(2023,4,30,13,40,20))')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 40)

    def test_second(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=SECOND(TIME(12,30,10))')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 10)
        formula = FormulaSQL(self.payment, 'test_col1', '=SECOND(DATETIME(2023,4,30,13,40,20))')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 20)

    def test_isoweeknum(self):
        formula = FormulaSQL(self.payment, 'test_col1', '=ISOWEEKNUM(DATETIME(2023,4,30,13,40,20))')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 18)

    def test_now(self):
        formula = FormulaSQL(self.payment, 'test_col1', '=NOW()')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1'] is not None)

    def test_today(self):
        formula = FormulaSQL(self.payment, 'test_col1', '=TODAY()')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1'] is not None)

    def test_weekday(self):
        formula = FormulaSQL(self.payment, 'test_col1', '=WEEKDAY(DATETIME(2023,4,29,13,40,20))')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 6)

    def test_datediff(self):
        formula = FormulaSQL(self.payment, 'test_col1',
                             '=DATEDIFF(DATE(2023,6,29),DATE(2022,6,29),"D")')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 365)
        formula = FormulaSQL(self.payment, 'test_col1',
                             '=DATEDIFF(DATE(2023,6,29),DATE(2022,6,29),"M")')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 12)
        formula = FormulaSQL(self.payment, 'test_col1',
                             '=DATEDIFF(DATE(2023,6,29),DATE(2022,6,29),"Y")')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']==1)

    def test_datetimediff(self):
        formula = FormulaSQL(self.payment, 'test_col1',
                             '=DATETIMEDIFF(DATETIME(2023,6,29,0,0,0),DATETIME(2022,6,29,0,0,0),"D")')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 365)
        formula = FormulaSQL(self.payment, 'test_col1',
                             '=DATETIMEDIFF(DATETIME(2023,6,29,0,0,0),DATETIME(2022,6,29,0,0,0),"M")')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 12)
        formula = FormulaSQL(self.payment, 'test_col1',
                             '=DATETIMEDIFF(DATETIME(2023,6,29,0,0,0),DATETIME(2022,6,29,0,0,0),"Y")')
        payment_x = self.payment.mutate(formula.ibis_column())
        row = payment_x['payment_id', 'customer_id', 'amount', 'payment_date', 'test_col1'].head().execute().iloc[0]
        assert (row['test_col1']== 1)
