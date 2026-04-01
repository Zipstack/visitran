import ibis

from formulasql.formulasql import FormulaSQL

if __name__ == "__main__":
    # Connect to a sample database
    connection = ibis.sqlite.connect("sample/geography.db")
    countries = connection.table("countries")
    print(countries["name", "continent", "population", "area_km2"].head().execute())

    formula = FormulaSQL(countries, "density", "=population / area_km2")
    countries = countries.mutate(formula.ibis_column())
    print(countries["name", "continent", "population", "area_km2", "density"].head().execute())

    formula = FormulaSQL(countries, "isAsia", '=IF(continent="AS", "Yes", "No")')
    countries = countries.mutate(formula.ibis_column())
    print(countries["name", "continent", "population", "area_km2", "isAsia"].head().execute())

    formula = FormulaSQL(
        countries, "full_name", '=IFS(continent="AS","Asia",continent="EU","Europe",continent="NA","North America")'
    )
    countries = countries.mutate(formula.ibis_column())
    print(countries["name", "continent", "population", "area_km2", "full_name"].head().execute())

    countries = countries.mutate(ibis.literal(3).name("month1"))
    formula = FormulaSQL(
        countries,
        "month_name",
        '=CHOOSE(month1,"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")',
    )
    countries = countries.mutate(formula.ibis_column())
    print(countries["name", "continent", "population", "area_km2", "month1", "month_name"].head().execute())

    formula = FormulaSQL(countries, "full_name", '=SWITCH(continent,"AS","Asia","EU","Europe","NA","North America")')
    countries = countries.mutate(formula.ibis_column())
    print(countries["name", "continent", "population", "area_km2", "full_name"].head().execute())

    # Making sure we have one 'None' value in the table
    formula = FormulaSQL(
        countries, "full_name2", '=IFS(continent="AS","Asia",continent="EUX","Europe",continent="NA","North America")'
    )
    countries = countries.mutate(formula.ibis_column())
    print(countries["name", "continent", "population", "area_km2", "full_name2"].head().execute())

    formula = FormulaSQL(countries, "full_name3", '=IFNA(full_name2,"Europe")')
    countries = countries.mutate(formula.ibis_column())
    print(countries["name", "continent", "population", "area_km2", "full_name2", "full_name3"].head().execute())
