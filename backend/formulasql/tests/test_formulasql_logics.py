import unittest
import datetime

import pytest
import ibis

from formulasql.formulasql import FormulaSQL


# Reference Table:
#                    name continent  population  area_km2
# 0               Andorra        EU       84000     468.0
# 1  United Arab Emirates        AS     4975593   82880.0
# 2           Afghanistan        AS    29121286  647500.0
# 3   Antigua and Barbuda        NA       86754     443.0
# 4              Anguilla        NA       13254     102.0

class TestFormulaSQLLogics:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = ibis.sqlite.connect('formulasql/tests/db_data/geography.db')
        self.countries = self.connection.table('countries')

    def test_if(self):
        formula = FormulaSQL(self.countries, 'test_col', '=IF(continent="AS", "Yes", "No")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        # We know the first row is Andorra, which is in Europe
        assert(row['test_col']== "No")
        # We know the second row is United Arab Emirates, which is in Asia
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[1]
        assert(row['test_col']== "Yes")

    def test_ifs(self):
        formula = FormulaSQL(self.countries, 'test_col',
                             '=IFS("None",continent="AS","Asia",continent="EU","Europe",continent="NA","North America")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        # We know the first row is Andorra, which is in Europe
        assert(row['test_col']== "Europe")
        # We know the second row is United Arab Emirates, which is in Asia
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[1]
        assert(row['test_col']== "Asia")
        # We know the fourth row is Antigua and Barbuda, which is in North America
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[3]
        assert(row['test_col']== "North America")

    def test_choose(self):
        # Add a couple of constant columns to the table
        countries_x = self.countries.mutate(ibis.literal(3).name('month1'))
        countries_x = countries_x.mutate(ibis.literal(5).name('month2'))
        formula = FormulaSQL(countries_x, 'month_name1',
                             '=CHOOSE(month1,"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")')
        countries_x = countries_x.mutate(formula.ibis_column())
        formula = FormulaSQL(countries_x, 'month_name2',
                             '=CHOOSE(month2,"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")')
        countries_x = countries_x.mutate(formula.ibis_column())
        row = countries_x['name', 'month_name1', 'month_name2'].head().execute().iloc[0]
        assert(row['month_name1']== "Mar")
        assert(row['month_name2']== "May")
        assert(row['month_name1']!= "Jan")

    def test_switch(self):
        formula = FormulaSQL(self.countries, 'test_col',
                             '=SWITCH(None,continent,"AS","Asia","EU","Europe","NA","North America")')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        # We know the first row is Andorra, which is in Europe
        assert(row['test_col']== "Europe")
        assert(row['test_col']!= "Asia")
        # We know the second row is United Arab Emirates, which is in Asia
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[1]
        assert(row['test_col']== "Asia")
        assert(row['test_col']!= "Europe")
        # We know the fourth row is Antigua and Barbuda, which is in North America
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[3]
        assert(row['test_col']== "North America")
        assert(row['test_col']!= "Asia")

    def test_ifna(self):

        formula = FormulaSQL(self.countries, 'test_col', '=IFNA(population, 3)')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        assert (row['test_col'] == 84000)

        countries_x = self.countries.mutate(ibis.literal(None, type="int64").name('pops'))
        formula = FormulaSQL(countries_x, 'test_col', '=IFNA(pops, 22)')
        new_col = formula.ibis_column()
        countries_x = countries_x.mutate(new_col)
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        assert (row['test_col'] == 22)

        countries_x = self.countries.mutate(ibis.literal('2024-02-02', type="date").name('date_1'))
        formula = FormulaSQL(countries_x, 'test_col', '=IFNA(date_1, "Found")')
        new_col = formula.ibis_column()
        countries_x = countries_x.mutate(new_col)
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        assert (row['test_col'] == datetime.date(2024, 2, 2))

        countries_x = self.countries.mutate(ibis.literal(None, type="date").name('date_1'))
        formula = FormulaSQL(countries_x, 'test_col', '=IFNA(date_1, "1991-01-01")')
        new_col = formula.ibis_column()
        countries_x = countries_x.mutate(new_col)
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        assert (row['test_col'] == datetime.date(1991, 1, 1))

        # Create a know cell with None value
        countries_x = self.countries.mutate(ibis.literal(None, type="string").name('test_col'))
        formula = FormulaSQL(countries_x, 'test_col2',
                             '=IFNA(test_col,"Europe")')
        countries_x = countries_x.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col', 'test_col2'].head().execute().iloc[0]
        # We know the first row is Andorra, which is in Europe
        assert(row['test_col2']=='Europe')
        # We know the second row is United Arab Emirates, which is in Asia
        row = countries_x['continent', 'population', 'area_km2', 'test_col', 'test_col2'].head().execute().iloc[1]
        assert(row['test_col'] is None)
        assert(row['test_col2'] == 'Europe')

    def test_isblank(self):
        countries_x = self.countries.mutate(ibis.literal(None, type="date").name('blank1'))
        formula = FormulaSQL(countries_x, 'test_col', '=ISBLANK(blank1)')
        countries_x = countries_x.mutate(formula.ibis_column())
        formula = FormulaSQL(countries_x, 'test_col2', '=ISBLANK(continent)')
        countries_x = countries_x.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col', 'test_col2'].head().execute().iloc[0]
        assert(row['test_col'] == True)
        assert(row['test_col2'] == False)

    def test_iseven(self):
        countries_x = self.countries.mutate(ibis.literal(2).name('val1'))
        countries_x = countries_x.mutate(ibis.literal(3).name('val2'))
        formula = FormulaSQL(countries_x, 'test_col', '=ISEVEN(val1)')
        countries_x = countries_x.mutate(formula.ibis_column())
        formula = FormulaSQL(countries_x, 'test_col2', '=ISEVEN(val2)')
        countries_x = countries_x.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col', 'test_col2'].head().execute().iloc[0]
        assert(row['test_col']== True)
        assert(row['test_col2']== False)

    def test_isodd(self):
        countries_x = self.countries.mutate(ibis.literal(2).name('val1'))
        countries_x = countries_x.mutate(ibis.literal(3).name('val2'))
        formula = FormulaSQL(countries_x, 'test_col', '=ISODD(val1)')
        countries_x = countries_x.mutate(formula.ibis_column())
        formula = FormulaSQL(countries_x, 'test_col2', '=ISODD(val2)')
        countries_x = countries_x.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col', 'test_col2'].head().execute().iloc[0]
        assert(row['test_col']== False)
        assert(row['test_col2']==True)

    def test_isna(self):
        # Create a know cell with None value
        formula = FormulaSQL(self.countries, 'test_col',
                             '=IFS(None,continent="AS","Asia",continent="EUX","Europe",continent="NA","North America")')
        countries_x = self.countries.mutate(formula.ibis_column())
        formula = FormulaSQL(countries_x, 'test_col2', '=ISNA(test_col)')
        countries_x = countries_x.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col', 'test_col2'].head().execute().iloc[0]
        # We know the first row is Andorra, which is in Europe
        assert(row['test_col2']== True)
        # We know the second row is United Arab Emirates, which is in Asia
        row = countries_x['continent', 'population', 'area_km2', 'test_col', 'test_col2'].head().execute().iloc[1]
        assert(row['test_col']!= False)

    def test_istext(self):
        countries_x = self.countries.mutate(ibis.literal('2').name('val1'))
        countries_x = countries_x.mutate(ibis.literal('Three').name('val2'))
        formula = FormulaSQL(countries_x, 'test_col1', '=ISTEXT(val1)')
        countries_x = countries_x.mutate(formula.ibis_column())
        formula = FormulaSQL(countries_x, 'test_col2', '=ISTEXT(val2)')
        countries_x = countries_x.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col1', 'test_col2'].head().execute().iloc[0]
        assert(row['test_col1']==False)
        assert(row['test_col2']==True)

    def test_isnumber(self):
        countries_x = self.countries.mutate(ibis.literal('2').name('val1'))
        countries_x = countries_x.mutate(ibis.literal('Three').name('val2'))
        formula = FormulaSQL(countries_x, 'test_col1', '=ISNUMBER(val1)')
        countries_x = countries_x.mutate(formula.ibis_column())
        formula = FormulaSQL(countries_x, 'test_col2', '=ISNUMBER(val2)')
        countries_x = countries_x.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col1', 'test_col2'].head().execute().iloc[0]
        assert(row['test_col1']== True)
        assert(row['test_col2']==False)

    def test_istrue(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=TRUE()')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert(row['test_col1']==True)
        assert(row['test_col1']!= False)

    def test_isfalse(self):
        formula = FormulaSQL(self.countries, 'test_col1', '=FALSE()')
        countries_x = self.countries.mutate(formula.ibis_column())
        row = countries_x['continent', 'population', 'area_km2', 'test_col1'].head().execute().iloc[0]
        assert(row['test_col1']== False)
        assert(row['test_col1']!= True)

    def test_between(self):
        formula = FormulaSQL(self.countries, 'test_col', '=BETWEEN(iso_numeric,4,20)')
        created_column = formula.ibis_column()
        countries_x = self.countries.mutate(created_column)
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[0]
        # iso_numeric is 20 in first row, so it will be True
        assert(row['test_col']== True)
        # iso_numeric is 784 in second row, so it will be False
        row = countries_x['name', 'continent', 'population', 'area_km2', 'test_col'].head().execute().iloc[1]
        assert(row['test_col']== False)
