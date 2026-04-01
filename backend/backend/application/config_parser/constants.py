AGGREGATE_DICT = {
    "COUNT": "count",
    "COUNT_DISTINCT": "nunique",
    "AVG": "mean",
    "MEAN": "mean",
    "SUM": "sum",
    "MIN": "min",
    "MAX": "max",
    "STD": "std",
    "STDDEV": "std",
    "VARIANCE": "var",
}


AGGREGATE_DETAILS = {
    "COUNT": "Return a count of the number of rows returned",
    "COUNT_DISTINCT": "Return a count of distinct/unique values in the column",
    "AVG": "Return the average value of the argument",
    "MEAN": "Return the mean value of the argument",
    "SUM": "Return the sum value of the argument",
    "MIN": "Return the minimum value of the argument",
    "MAX": "Return the maximum value of the argument",
    "STD": "Return the std value of the argument",
    "STDDEV": "Return the stddev value of the argument",
    "VARIANCE": "Return the variance of the argument",
}


FORMULA_DICT = {
    "ABS": {
        "key": "ABS",
        "value": "ABS",
        "description": (
            "Returns the absolute value (i.e. the modulus) of a supplied number. \nExample: ABS(-5) returns 5."
        ),
        "function_type": "Math",
    },
    "ACOS": {
        "key": "ACOS",
        "value": "ACOS",
        "description": (
            "Returns the Arccosine of a number \n Example: ACOS(0.5) returns 60 degrees"
            " (approximately 1.047 radians)."
        ),
        "function_type": "Math",
    },
    "AND": {
        "key": "AND",
        "value": "AND",
        "description": (
            "Tests a number of user-defined conditions and returns TRUE if ALL of the"
            " conditions evaluate to TRUE, or FALSE otherwise. \n Example: AND(TRUE,"
            " FALSE, TRUE) returns FALSE."
        ),
        "function_type": "Logic",
    },
    "ASIN": {
        "key": "ASIN",
        "value": "ASIN",
        "description": "Returns the Arcsine of a number. \n Example:  ASIN(0.5) returns 0.524 (approximately).",
        "function_type": "Math",
    },
    "ATAN": {
        "key": "ATAN",
        "value": "ATAN",
        "description": (
            "Returns the Arctangent of a given number. \n Example:  ATAN(1) returns 0.785 (approximately)."
        ),
        "function_type": "Math",
    },
    "ATAN2": {
        "key": "ATAN2",
        "value": "ATAN2",
        "description": (
            "Returns the Arctangent of a given pair of x and y coordinates. \n Example:"
            "  ATAN2(4, 2) returns 1.107 (approximately)."
        ),
        "function_type": "Math",
    },
    "AVERAGE": {
        "key": "AVERAGE",
        "value": "AVERAGE",
        "description": ("Returns the Average of a list of supplied numbers. \n Example:  AVERAGE(1, 3, 5) returns 3."),
        "function_type": "Statistics",
    },
    "BITAND": {
        "key": "BITAND",
        "value": "BITAND",
        "description": "Returns a Bitwise 'And' of two numbers . \n Example: BITAND(5-101, 3-011) returns 1-001.",
        "function_type": "Math",
    },
    "BITLSHIFT": {
        "key": "BITLSHIFT",
        "value": "BITLSHIFT",
        "description": (
            "Returns a number shifted left by a specified number of bits. \n Example: "
            " BITLSHIFT(5-00000101, 2-00000010) returns 20-00010100."
        ),
        "function_type": "Math",
    },
    "BITOR": {
        "key": "BITOR",
        "value": "BITOR",
        "description": "Returns a Bitwise 'Or' of two numbers. \n Example:  BITOR(5-0101, 2-0010) returns 7-0111.",
        "function_type": "Math",
    },
    "BITRSHIFT": {
        "key": "BITRSHIFT",
        "value": "BITRSHIFT",
        "description": (
            "Returns a number shifted right by a specified number of bits. \n Example: "
            " BITRSHIFT(5-00000101, 2-00000010) returns 1-00000001."
        ),
        "function_type": "Math",
    },
    "BITXOR": {
        "key": "BITXOR",
        "value": "BITXOR",
        "description": (
            "Returns a Bitwise 'Exclusive Or' of two numbers. \n Example: BITXOR(5-0101, 2-0010) returns 7-0111"
        ),
        "function_type": "Math",
    },
    "CAST": {
        "key": "CAST",
        "value": "CAST",
        "description": (
            "The CAST function is used to explicitly convert data types."
            " It allows you to convert an expression from one data type to another.  "
            " \n Example: CAST(column_name,INT)"
        ),
        "function_type": "Math",
    },
    "CEILING": {
        "key": "CEILING",
        "value": "CEILING",
        "description": (
            "Rounds a number away from zero (i.e. rounds a positive number up and a"
            " negative number down), to a multiple of significance. \n Example: "
            " CEILING(2.3) returns 3."
        ),
        "function_type": "Math",
    },
    "CHOOSE": {
        "key": "CHOOSE",
        "value": "CHOOSE",
        "description": (
            "Returns one of a list of values, depending on the value of a supplied"
            " index number. \n Example:  CHOOSE(3, 'Apple', 'Banana', 'Cherry') returns"
            " 'Cherry'."
        ),
        "function_type": "Logic",
    },
    "CLEAN": {
        "key": "CLEAN",
        "value": "CLEAN",
        "description": (
            "Removes all non-printable characters from a supplied text string. \n"
            " Example:  CLEAN('Hello!^@#$') returns 'Hello!'."
        ),
        "function_type": "Text",
    },
    "COALESCE": {
        "key": "COALESCE",
        "value": "COALESCE",
        "description": (
            "The COALESCE function returns the first non-null expression in a list of ."
            " \n Example: COALESCE(column1,'DefaultValue')"
        ),
        "function_type": "Math",
    },
    "CODE": {
        "key": "CODE",
        "value": "CODE",
        "description": (
            "Returns the numeric code for the first character of a supplied string. \n"
            " Example:  CODE('A') returns 65."
        ),
        "function_type": "Text",
    },
    "CONCAT": {
        "key": "CONCAT",
        "value": "CONCAT",
        "description": (
            "Joins together two or more text strings.  \n Example:  CONCAT('Hello', '"
            " ', 'World!') returns 'Hello World!'."
        ),
        "function_type": "Text",
    },
    "CONCATENATE": {
        "key": "CONCATENATE",
        "value": "CONCATENATE",
        "description": (
            "Joins together two or more text strings. \n Example:  CONCATENATE('Hello',"
            " ' ', 'World!') returns 'Hello World!'."
        ),
        "function_type": "Text",
    },
    "COS": {
        "key": "COS",
        "value": "COS",
        "description": "Returns the Cosine of a given angle. \n Example: COS(1) returns 0.5403 (approximately).",
        "function_type": "Math",
    },
    "COT": {
        "key": "COT",
        "value": "COT",
        "description": "Returns the cotangent of an angle. \n Example:  COT(1) returns 0.6421 (approximately).",
        "function_type": "Math",
    },
    "DATE": {
        "key": "DATE",
        "value": "DATE",
        "description": (
            "Returns a date, from a user-supplied year, month and day. \n Example: "
            " DATE(2023, 6, 29) returns June 29, 2023."
        ),
        "function_type": "Date",
    },
    "DAY": {
        "key": "DAY",
        "value": "DAY",
        "description": (
            "Returns the day (of the month) from a user-supplied date. \n Example: "
            " DAY(DATE(2023, 6, 29)) returns 29."
        ),
        "function_type": "Date",
    },
    "DAYS": {
        "key": "DAYS",
        "value": "DAYS",
        "description": (
            "Returns the number of days between 2 dates. \n Example:  DAYS(DATE(2023,"
            " 6, 29), DATE(2023, 7, 5)) returns 6."
        ),
        "function_type": "Date",
    },
    "DEGREES": {
        "key": "DEGREES",
        "value": "DEGREES",
        "description": "Converts Radians to Degrees. \n Example:  DEGREES(1.5708) returns 90.",
        "function_type": "Math",
    },
    "DELTA": {
        "key": "DELTA",
        "value": "DELTA",
        "description": "Tests whether two supplied numbers are equal. \n Example:  DELTA(5, 5) returns 1.",
        "function_type": "Math",
    },
    "EDATE": {
        "key": "EDATE",
        "value": "EDATE",
        "description": (
            "Returns a date that is the specified number of months before or after an"
            " initial supplied start date. \n Example:  EDATE(DATE(2023, 6, 29), 3)"
            " returns September 29, 2023."
        ),
        "function_type": "Date",
    },
    "EVEN": {
        "key": "EVEN",
        "value": "EVEN",
        "description": (
            "Rounds a number away from zero (i.e. rounds a positive number up and a"
            " negative number down), to the next even number. \n Example:  EVEN(5.5)"
            " returns 6. "
        ),
        "function_type": "Math",
    },
    "EXACT": {
        "key": "EXACT",
        "value": "EXACT",
        "description": (
            "Tests if two supplied text strings are exactly the same and if so, returns"
            " TRUE; Otherwise, returns FALSE. (case-sensitive). \n Example: "
            " EXACT('apple', 'Apple') returns FALSE."
        ),
        "function_type": "Text",
    },
    "EXP": {
        "key": "EXP",
        "value": "EXP",
        "description": "Returns e raised to a given power. \n Example:  EXP(1) returns 2.7183 (approximately).",
        "function_type": "Math",
    },
    "FIND": {
        "key": "FIND",
        "value": "FIND",
        "description": (
            "Returns the position of a supplied character or text string from within a"
            " supplied text string (case-sensitive). \n Example:  FIND('world', 'Hello,"
            " world!') returns 8."
        ),
        "function_type": "Text",
    },
    "FIXED": {
        "key": "FIXED",
        "value": "FIXED",
        "description": (
            "Rounds a supplied number to a specified number of decimal places, and then"
            " converts this into text. \n Example:  FIXED(3.14159, 2) returns '3.14'."
        ),
        "function_type": "Text",
    },
    "FLOOR": {
        "key": "FLOOR",
        "value": "FLOOR",
        "description": (
            "Rounds a number towards zero, (i.e. rounds a positive number down and a"
            " negative number up), to a multiple of significance. \n Example: "
            " FLOOR(4.7, 1) returns 4."
        ),
        "function_type": "Math",
    },
    "GESTEP": {
        "key": "GESTEP",
        "value": "GESTEP",
        "description": "Tests whether a number is greater than a supplied threshold value ",
        "function_type": "Math",
    },
    "HOUR": {
        "key": "HOUR",
        "value": "HOUR",
        "description": ("Returns the hour part of a user-supplied time. \n Example:  HOUR(TIME(9, 30, 0)) returns 9."),
        "function_type": "Date",
    },
    "IF": {
        "key": "IF",
        "value": "IF",
        "description": (
            "Tests a user-defined condition and returns one result if the condition is"
            " TRUE, and another result if the condition is FALSE. \n Example:  IF(A1 >"
            " 10, 'Large', 'Small') returns 'Large' if the value in column A1 is greater"
            " than 10, otherwise 'Small'. "
        ),
        "function_type": "Logic",
    },
    "IFNA": {
        "key": "IFNA",
        "value": "IFNA",
        "description": (
            "Tests if an expression returns the #N/A error and if so, returns an"
            " alternative specified value; Otherwise the function returns the value of"
            " the supplied expression. \n Example:  IFNA(VLOOKUP(A1, B1:C10, 2, FALSE),"
            " 'Not found') returns 'Not found' if the VLOOKUP result is #N/A."
        ),
        "function_type": "Logic",
    },
    "IFS": {
        "key": "IFS",
        "value": "IFS",
        "description": (
            "Tests a number of supplied conditions and returns a result corresponding"
            " to the first condition that evaluates to TRUE. \n Example:  IFS(A1 > 10,"
            " 'Large', A1 > 5, 'Medium', A1 > 0, 'Small', NULL) returns 'Large' if A1 is"
            " greater than 10, 'Medium' if A1 is greater than 5, and 'Small' if A1 is"
            " greater than 0 and returns NULL if not matched in any of the conditions"
        ),
        "function_type": "Logic",
    },
    "INT": {
        "key": "INT",
        "value": "INT",
        "description": "Rounds a number down to the next integer. \n Example:  INT(3.8) returns 3.",
        "function_type": "Math",
    },
    "ISBLANK": {
        "key": "ISBLANK",
        "value": "ISBLANK",
        "description": (
            "Tests if a given column_name is blank (empty), and if so, returns TRUE;"
            " Otherwise, returns FALSE. \n Example:  ISBLANK(A1) returns TRUE if column"
            " A1 is empty."
        ),
        "function_type": "Logic",
    },
    "ISEVEN": {
        "key": "ISEVEN",
        "value": "ISEVEN",
        "description": (
            "Tests if a supplied number (or expression) is an even number, and if so,"
            " returns TRUE; Otherwise, returns FALSE. \n Example:  ISEVEN(6) returns"
            " TRUE."
        ),
        "function_type": "Logic",
    },
    "ISNA": {
        "key": "ISNA",
        "value": "ISNA",
        "description": (
            "Tests if an initial supplied value (or expression) or column returns the #N/A"
            "error and if so, returns TRUE; Otherwise returns FALSE. \n Example:"
            "ISNA(column_name) returns TRUE if column contains #N/A."
        ),
        "function_type": "Logic",
    },
    "ISNUMBER": {
        "key": "ISNUMBER",
        "value": "ISNUMBER",
        "description": (
            "Tests if a supplied value is a number, and if so, returns TRUE; Otherwise,"
            " returns FALSE. \n Example:  ISNUMBER(A1) returns TRUE if column A1 contains"
            " a number."
        ),
        "function_type": "Logic",
    },
    "ISODD": {
        "key": "ISODD",
        "value": "ISODD",
        "description": (
            "Tests if a supplied number (or expression) is an odd number, and if so,"
            " returns TRUE; Otherwise, returns FALSE. \n Example:  ISODD(7) returns"
            " TRUE."
        ),
        "function_type": "Logic",
    },
    "ISOWEEKNUM": {
        "key": "ISOWEEKNUM",
        "value": "ISOWEEKNUM",
        "description": (
            "Returns the ISO week number of the year for a given date (New in Excel"
            " 2013). \n Example:  ISOWEEKNUM(DATE(2023, 6, 29)) returns 26."
        ),
        "function_type": "Date",
    },
    "ISTEXT": {
        "key": "ISTEXT",
        "value": "ISTEXT",
        "description": (
            "Tests if a supplied value is text, and if so, returns TRUE; Otherwise,"
            " returns FALSE. \n Example:  ISTEXT(A1) returns TRUE if column A1 contains"
            " text."
        ),
        "function_type": "Logic",
    },
    "LEFT": {
        "key": "LEFT",
        "value": "LEFT",
        "description": (
            "Returns a specified number of characters from the start of a supplied text"
            " string. \n Example:  ISTEXT(A1) returns TRUE if column A1 contains text."
        ),
        "function_type": "Text",
    },
    "LEN": {
        "key": "LEN",
        "value": "LEN",
        "description": "Returns the length of a supplied text string. \n Example:  LEN('Hello') returns 5.",
        "function_type": "Text",
    },
    "LN": {
        "key": "LN",
        "value": "LN",
        "description": (
            "Returns the natural logarithm of a given number. \n Example:  LN(10) returns approximately 2.3026."
        ),
        "function_type": "Math",
    },
    "LOG": {
        "key": "LOG",
        "value": "LOG",
        "description": (
            "Returns the logarithm of a given number, to a specified base. \n Example: LOG(100, 10) returns 2."
        ),
        "function_type": "Math",
    },
    "LOG10": {
        "key": "LOG10",
        "value": "LOG10",
        "description": "Returns the base 10 logarithm of a given number. \n Example:  LOG10(100) returns 2.",
        "function_type": "Math",
    },
    "LOWER": {
        "key": "LOWER",
        "value": "LOWER",
        "description": (
            "Converts all characters in a supplied text string to lower case. \n"
            " Example:  LOWER('HELLO') returns 'hello'."
        ),
        "function_type": "Text",
    },
    "MAX": {
        "key": "MAX",
        "value": "MAX",
        "description": (
            "Returns the largest value from a list of supplied numbers. \n Example: "
            " MAX(A1,A2,A3,A4,A5) returns the maximum value in columns from A1 to A5."
        ),
        "function_type": "Math",
    },
    "MID": {
        "key": "MID",
        "value": "MID",
        "description": (
            "Returns a specified number of characters from the middle of a supplied"
            " text string. \n Example:  MID('Hello, world!', 8, 5) returns 'world'."
        ),
        "function_type": "Text",
    },
    "MIN": {
        "key": "MIN",
        "value": "MIN",
        "description": (
            "Returns the smallest value from a list of supplied numbers. \n Example: "
            " MIN(A1,A2,A3,A4,A5) returns the minimum value in column from A1 to A5."
        ),
        "function_type": "Math",
    },
    "MINUTE": {
        "key": "MINUTE",
        "value": "MINUTE",
        "description": (
            "Returns the minute part of a user-supplied time. \n Example: MINUTE(TIME(9, 30, 0)) returns 30."
        ),
        "function_type": "Date",
    },
    "MOD": {
        "key": "MOD",
        "value": "MOD",
        "description": (
            "Returns the remainder from a division between two supplied numbers. \n Example:  MOD(10, 3) returns 1."
        ),
        "function_type": "Math",
    },
    "MONTH": {
        "key": "MONTH",
        "value": "MONTH",
        "description": (
            "Returns the month from a user-supplied date. \n Example:  MONTH(DATE(2023, 6, 29)) returns 6."
        ),
        "function_type": "Date",
    },
    "N": {
        "key": "N",
        "value": "N",
        "description": (
            "Converts a non-number value to a number, a date to a serial number, the"
            " logical value TRUE to 1 and all other values to 0. \n Example:  N('10')"
            " returns 10."
        ),
        "function_type": "Conversion",
    },
    "NOT": {
        "key": "NOT",
        "value": "NOT",
        "description": (
            "Returns a logical value that is the opposite of a user supplied logical"
            " value or expression. \n Example:  NOT(TRUE) returns FALSE."
        ),
        "function_type": "Logic",
    },
    "NOW": {
        "key": "NOW",
        "value": "NOW",
        "description": "Returns the current date & time. \n Example:  NOW() returns the current date and time.",
        "function_type": "Date",
    },
    "NUMBERVALUE": {
        "key": "NUMBERVALUE",
        "value": "NUMBERVALUE",
        "description": (
            "Converts text to a number, in a locale-independent way. \n Example: "
            " NUMBERVALUE('123.45', '.', ',') returns 123.45."
        ),
        "function_type": "Conversion",
    },
    "ODD": {
        "key": "ODD",
        "value": "ODD",
        "description": (
            "Rounds a number away from zero (i.e. rounds a positive number up and a"
            " negative number down), to the next odd number. \n Example:  ODD(7.5)"
            " returns 9."
        ),
        "function_type": "Math",
    },
    "OR": {
        "key": "OR",
        "value": "OR",
        "description": (
            "Tests a number of user-defined conditions and returns TRUE if ANY of the"
            " conditions evaluate to TRUE, or FALSE otherwise. \n Example:  OR(TRUE,"
            " FALSE, TRUE) returns TRUE."
        ),
        "function_type": "Logic",
    },
    "PI": {
        "key": "PI",
        "value": "PI",
        "description": (
            "Returns the constant value of pi. \n Example:  PI() returns the value of"
            " pi (approximately 3.14159265358979)."
        ),
        "function_type": "Math",
    },
    "POWER": {
        "key": "POWER",
        "value": "POWER",
        "description": (
            "Returns the result of a given number raised to a supplied power. \n Example:  POWER(2, 3) returns 8."
        ),
        "function_type": "Math",
    },
    "PRODUCT": {
        "key": "PRODUCT",
        "value": "PRODUCT",
        "description": ("Returns the product of a supplied list of numbers. \n Example:  PRODUCT(2, 3, 4) returns 24."),
        "function_type": "Math",
    },
    "PROPER": {
        "key": "PROPER",
        "value": "PROPER",
        "description": (
            "Converts all characters in a supplied text string to proper case (i.e."
            " letters that do not follow another letter are upper case and all other"
            " characters are lower case). \n Example:  PROPER('hello world') returns"
            " 'Hello World'."
        ),
        "function_type": "Text",
    },
    "QUOTIENT": {
        "key": "QUOTIENT",
        "value": "QUOTIENT",
        "description": (
            "Returns the integer portion of a division between two supplied numbers. \n"
            " Example:  QUOTIENT(10, 3) returns 3."
        ),
        "function_type": "Math",
    },
    "RADIANS": {
        "key": "RADIANS",
        "value": "RADIANS",
        "description": ("Converts Degrees to Radians. \n Example:  RADIANS(45) returns 0.785 (approximately)."),
        "function_type": "Math",
    },
    "REPT": {
        "key": "REPT",
        "value": "REPT",
        "description": (
            "Returns a string consisting of a supplied text string, repeated a"
            " specified number of times. \n Example:  REPT('Hello', 3) returns"
            " 'HelloHelloHello'."
        ),
        "function_type": "Text",
    },
    "RIGHT": {
        "key": "RIGHT",
        "value": "RIGHT",
        "description": (
            "Returns a specified number of characters from the end of a supplied text"
            " string. \n Example:  RIGHT('Hello, world!', 6) returns 'world!''."
        ),
        "function_type": "Text",
    },
    "ROUND": {
        "key": "ROUND",
        "value": "ROUND",
        "description": (
            "Rounds a number up or down, to a given number of digits. \n Example:  ROUND(3.14159, 2) returns 3.14."
        ),
        "function_type": "Math",
    },
    "ROUNDDOWN": {
        "key": "ROUNDDOWN",
        "value": "ROUNDDOWN",
        "description": (
            "Rounds a number towards zero, (i.e. rounds a positive number down and a"
            " negative number up), to a given number of digits. \n Example: "
            " ROUNDDOWN(3.14159, 2) returns 3.14."
        ),
        "function_type": "Math",
    },
    "ROUNDUP": {
        "key": "ROUNDUP",
        "value": "ROUNDUP",
        "description": (
            "Rounds a number away from zero (i.e. rounds a positive number up and a"
            " negative number down), to a given number of digits. \n Example: "
            " ROUNDUP(3.14159, 2) returns 3.15."
        ),
        "function_type": "Math",
    },
    "SEARCH": {
        "key": "SEARCH",
        "value": "SEARCH",
        "description": (
            "Returns the position of a supplied character or text string from within a"
            " supplied text string (non-case-sensitive). \n Example:  SEARCH('world',"
            " 'Hello, world!') returns 8."
        ),
        "function_type": "Text",
    },
    "SECOND": {
        "key": "SECOND",
        "value": "SECOND",
        "description": (
            "Returns the seconds part of a user-supplied time. \n Example: SECOND(TIME(9, 30, 15)) returns 15."
        ),
        "function_type": "Date",
    },
    "SIGN": {
        "key": "SIGN",
        "value": "SIGN",
        "description": "Returns the sign (+1, -1 or 0) of a supplied number. \n Example:  SIGN(-5) returns -1.",
        "function_type": "Math",
    },
    "SIN": {
        "key": "SIN",
        "value": "SIN",
        "description": "Returns the Sine of a given angle. \n Example:  SIN(1) returns 0.8415 (approximately).",
        "function_type": "Math",
    },
    "SQRT": {
        "key": "SQRT",
        "value": "SQRT",
        "description": "Returns the positive square root of a given number. \n Example:  SQRT(25) returns 5.",
        "function_type": "Math",
    },
    "SQRTPI": {
        "key": "SQRTPI",
        "value": "SQRTPI",
        "description": (
            "Returns the square root of a supplied number multiplied by pi. \n Example:"
            "  SQRTPI(10) returns 5.6419 (approximately)."
        ),
        "function_type": "Math",
    },
    "SUBSTITUTE": {
        "key": "SUBSTITUTE",
        "value": "SUBSTITUTE",
        "description": (
            "Substitutes all occurrences of a search text string, within an original"
            " text string, with the supplied replacement text. \n Example: "
            " SUBSTITUTE('Hello, world!', 'world', 'universe') returns 'Hello,"
            " universe!'."
        ),
        "function_type": "Text",
    },
    "SUM": {
        "key": "SUM",
        "value": "SUM",
        "description": "Returns the sum of a supplied list of numbers. \n Example:  SUM(1, 2, 3) returns 6.",
        "function_type": "Math",
    },
    "SUMSQ": {
        "key": "SUMSQ",
        "value": "SUMSQ",
        "description": (
            "Returns the sum of the squares of a supplied list of numbers. \n Example:  SUMSQ(1, 2, 3) returns 14."
        ),
        "function_type": "Math",
    },
    "SWITCH": {
        "key": "SWITCH",
        "value": "SWITCH",
        "description": (
            "Compares a number of supplied values to a supplied test expression and"
            " returns a result corresponding to the first value that matches the test"
            " expression. \n Example:  SWITCH(A1, 1, 'One', 2, 'Two', 3, 'Three',"
            " 'Other') returns 'One' if A1 is 1, 'Two' if A1 is 2, 'Three' if A1 is 3,"
            " and 'Other' otherwise."
        ),
        "function_type": "Logic",
    },
    "TAN": {
        "key": "TAN",
        "value": "TAN",
        "description": "Returns the Tangent of a given angle. \n Example:  TAN(1) returns 1.5574 (approximately).",
        "function_type": "Math",
    },
    "TIME": {
        "key": "TIME",
        "value": "TIME",
        "description": (
            "Returns a time, from a user-supplied hour, minute and second. \n Example: "
            " TIME(9, 30, 0) returns the time 09:30:00."
        ),
        "function_type": "Date",
    },
    "TODAY": {
        "key": "TODAY",
        "value": "TODAY",
        "description": "Returns today's date. \n Example:  TIME(9, 30, 0) returns the time 09:30:00.",
        "function_type": "Date",
    },
    "TRIM": {
        "key": "TRIM",
        "value": "TRIM",
        "description": (
            "Removes duplicate spaces, and spaces at the start and end of a text"
            " string. \n Example:  TRIM(' Hello ') returns 'Hello'."
        ),
        "function_type": "Text",
    },
    "UPPER": {
        "key": "UPPER",
        "value": "UPPER",
        "description": (
            "Converts all characters in a supplied text string to upper case. \n"
            " Example:  UPPER('hello') returns 'HELLO'."
        ),
        "function_type": "Text",
    },
    "WEEKDAY": {
        "key": "WEEKDAY",
        "value": "WEEKDAY",
        "description": (
            "Returns an integer representing the day of the week for a supplied date."
            " \n Example: WEEKDAY(DATE(2023, 6, 29)) returns 5."
        ),
        "function_type": "Date",
    },
    "WEEKNUM": {
        "key": "WEEKNUM",
        "value": "WEEKNUM",
        "description": (
            "Returns an integer representing the week number (from 1 to 53) of the year"
            " from a user-supplied date. \n Example: WEEKNUM(DATE(2023, 6, 29)) returns 26."
        ),
        "function_type": "Date",
    },
    "XOR": {
        "key": "XOR",
        "value": "XOR",
        "description": (
            "Returns a logical Exclusive Or of all arguments. \n Example:  XOR(TRUE, FALSE, TRUE) returns TRUE."
        ),
        "function_type": "Logic",
    },
    "YEAR": {
        "key": "YEAR",
        "value": "YEAR",
        "description": (
            "Returns the year from a user-supplied date. \n Example:  YEAR(DATE(2023, 6, 29)) returns 2023."
        ),
        "function_type": "Date",
    },
    "FALSE": {
        "key": "FALSE",
        "value": "FALSE",
        "description": (
            "Simply returns the logical value FALSE. \n Example:  FALSE() returns the logical value FALSE."
        ),
        "function_type": "Logic",
    },
    "TRUE": {
        "key": "TRUE",
        "value": "TRUE",
        "description": ("Simply returns the logical value TRUE. \n Example:  TRUE() returns the logical value TRUE."),
        "function_type": "Logic",
    },
    "DUPLICATE": {
        "key": "DUPLICATE",
        "value": "DUPLICATE",
        "description": (
            "Returns shallow copy of existing column \n Example:  DUPLICATE(column_name) returns column_name."
        ),
        "function_type": "Logic",
    },
    "ISIN": {
        "key": "ISIN",
        "value": "ISIN",
        "description": (
            "Returns True/False \n Example:  ISIN(column_name,1,2,3) returns True "
            "\nif  column_name is among given rest of value(1,2,3), \nFalse otherwise."
        ),
        "function_type": "Logic",
    },
    "NOTIN": {
        "key": "NOTIN",
        "value": "NOTIN",
        "description": (
            "Returns True/False \n Example:  NOTIN(column_name,1,2,3) returns True "
            "\nif column_name is not among given rest of value(1,2,3), \nFalse otherwise."
        ),
        "function_type": "Logic",
    },
    "BETWEEN": {
        "key": "BETWEEN",
        "value": "BETWEEN",
        "description": (
            "Returns True/False \n Example:  BETWEEN(column_name,lower_val,upper_val) returns True "
            "\nif column_name is in given range of value(lower_val,upper_val) where upper_val is inclusive, \nFalse otherwise."
        ),
        "function_type": "Logic",
    },
    "DIFFERENCE": {
        "key": "DIFFERENCE",
        "value": "DIFFERENCE",
        "description": "Returns the difference of a supplied numbers. \n Example:  DIFFERENCE(1, 2) returns -1.",
        "function_type": "Math",
    },
    # =========================================================================
    # Window Functions (NEW)
    # =========================================================================
    "LAG": {
        "key": "LAG",
        "value": "LAG",
        "description": (
            "Returns the value from a previous row within a partition. \n"
            "Example: LAG(sales, 1) returns the previous row's sales value."
        ),
        "function_type": "Window",
    },
    "LEAD": {
        "key": "LEAD",
        "value": "LEAD",
        "description": (
            "Returns the value from a subsequent row within a partition. \n"
            "Example: LEAD(sales, 1) returns the next row's sales value."
        ),
        "function_type": "Window",
    },
    "CUMSUM": {
        "key": "CUMSUM",
        "value": "CUMSUM",
        "description": (
            "Returns the cumulative sum of values. \n" "Example: CUMSUM(sales) returns running total of sales."
        ),
        "function_type": "Window",
    },
    "CUMMEAN": {
        "key": "CUMMEAN",
        "value": "CUMMEAN",
        "description": (
            "Returns the cumulative mean of values. \n" "Example: CUMMEAN(score) returns running average of scores."
        ),
        "function_type": "Window",
    },
    "CUMMIN": {
        "key": "CUMMIN",
        "value": "CUMMIN",
        "description": (
            "Returns the cumulative minimum value. \n" "Example: CUMMIN(price) returns the minimum price seen so far."
        ),
        "function_type": "Window",
    },
    "CUMMAX": {
        "key": "CUMMAX",
        "value": "CUMMAX",
        "description": (
            "Returns the cumulative maximum value. \n" "Example: CUMMAX(price) returns the maximum price seen so far."
        ),
        "function_type": "Window",
    },
    "FIRST": {
        "key": "FIRST",
        "value": "FIRST",
        "description": (
            "Returns the first value in a window. \n" "Example: FIRST(name) returns the first name in the partition."
        ),
        "function_type": "Window",
    },
    "LAST": {
        "key": "LAST",
        "value": "LAST",
        "description": (
            "Returns the last value in a window. \n" "Example: LAST(name) returns the last name in the partition."
        ),
        "function_type": "Window",
    },
    "ROW_NUMBER": {
        "key": "ROW_NUMBER",
        "value": "ROW_NUMBER",
        "description": (
            "Returns sequential row numbers starting from 0 within a window partition. \n"
            "Example: ROW_NUMBER() returns 0, 1, 2, 3, ... for each row. \n"
            "Example: ROW_NUMBER(order_id) returns row numbers ordered by order_id."
        ),
        "function_type": "Window",
    },
    "RANK": {
        "key": "RANK",
        "value": "RANK",
        "description": (
            "Returns rank with gaps for ties based on the column value. \n"
            "Example: RANK(score) returns 1, 2, 2, 4 for values 100, 90, 90, 80."
        ),
        "function_type": "Window",
    },
    "DENSE_RANK": {
        "key": "DENSE_RANK",
        "value": "DENSE_RANK",
        "description": (
            "Returns rank without gaps for ties based on the column value. \n"
            "Example: DENSE_RANK(score) returns 1, 2, 2, 3 for values 100, 90, 90, 80."
        ),
        "function_type": "Window",
    },
    # =========================================================================
    # Date/Time Functions (NEW)
    # =========================================================================
    "QUARTER": {
        "key": "QUARTER",
        "value": "QUARTER",
        "description": ("Returns the quarter (1-4) from a date. \n" "Example: QUARTER(DATE(2023, 6, 29)) returns 2."),
        "function_type": "Date",
    },
    "DAY_OF_YEAR": {
        "key": "DAY_OF_YEAR",
        "value": "DAY_OF_YEAR",
        "description": (
            "Returns the day of the year (1-366) from a date. \n" "Example: DAY_OF_YEAR(DATE(2023, 2, 1)) returns 32."
        ),
        "function_type": "Date",
    },
    "EPOCH_SECONDS": {
        "key": "EPOCH_SECONDS",
        "value": "EPOCH_SECONDS",
        "description": (
            "Returns the Unix timestamp (seconds since 1970-01-01). \n"
            "Example: EPOCH_SECONDS(NOW()) returns the current Unix timestamp."
        ),
        "function_type": "Date",
    },
    "STRFTIME": {
        "key": "STRFTIME",
        "value": "STRFTIME",
        "description": (
            "Formats a date/time according to a format string. \n"
            "Example: STRFTIME(date_col, '%Y-%m-%d') returns '2023-06-29'."
        ),
        "function_type": "Date",
    },
    "MILLISECOND": {
        "key": "MILLISECOND",
        "value": "MILLISECOND",
        "description": (
            "Returns the millisecond component from a timestamp. \n"
            "Example: MILLISECOND(timestamp_col) returns 0-999."
        ),
        "function_type": "Date",
    },
    "MICROSECOND": {
        "key": "MICROSECOND",
        "value": "MICROSECOND",
        "description": (
            "Returns the microsecond component from a timestamp. \n"
            "Example: MICROSECOND(timestamp_col) returns 0-999999."
        ),
        "function_type": "Date",
    },
    "DATE_TRUNC": {
        "key": "DATE_TRUNC",
        "value": "DATE_TRUNC",
        "description": (
            "Truncates a timestamp to the specified unit. \n"
            "Example: DATE_TRUNC(timestamp, 'month') truncates to month start."
        ),
        "function_type": "Date",
    },
    # =========================================================================
    # Statistical Functions (NEW)
    # =========================================================================
    "MEDIAN": {
        "key": "MEDIAN",
        "value": "MEDIAN",
        "description": (
            "Returns the median (middle) value of a column. \n" "Example: MEDIAN(salary) returns the middle salary."
        ),
        "function_type": "Statistics",
    },
    "QUANTILE": {
        "key": "QUANTILE",
        "value": "QUANTILE",
        "description": (
            "Returns the value at a given quantile (0-1). \n"
            "Example: QUANTILE(score, 0.75) returns the 75th percentile."
        ),
        "function_type": "Statistics",
    },
    "VARIANCE": {
        "key": "VARIANCE",
        "value": "VARIANCE",
        "description": ("Returns the variance of a column. \n" "Example: VARIANCE(price) returns the price variance."),
        "function_type": "Statistics",
    },
    "STDDEV": {
        "key": "STDDEV",
        "value": "STDDEV",
        "description": (
            "Returns the standard deviation of a column. \n"
            "Example: STDDEV(score) returns the score standard deviation."
        ),
        "function_type": "Statistics",
    },
    "COV": {
        "key": "COV",
        "value": "COV",
        "description": (
            "Returns the covariance between two columns. \n" "Example: COV(x, y) returns the covariance of x and y."
        ),
        "function_type": "Statistics",
    },
    # =========================================================================
    # Numeric Functions (NEW)
    # =========================================================================
    "LOG2": {
        "key": "LOG2",
        "value": "LOG2",
        "description": ("Returns the base-2 logarithm of a number. \n" "Example: LOG2(8) returns 3."),
        "function_type": "Math",
    },
    "CLIP": {
        "key": "CLIP",
        "value": "CLIP",
        "description": (
            "Clips values to be within a specified range. \n"
            "Example: CLIP(value, 0, 100) limits values between 0 and 100."
        ),
        "function_type": "Math",
    },
    "NEGATE": {
        "key": "NEGATE",
        "value": "NEGATE",
        "description": ("Returns the negation of a number. \n" "Example: NEGATE(5) returns -5."),
        "function_type": "Math",
    },
    "RANDOM": {
        "key": "RANDOM",
        "value": "RANDOM",
        "description": ("Returns a random float between 0 and 1. \n" "Example: RANDOM() returns 0.7234..."),
        "function_type": "Math",
    },
    "E": {
        "key": "E",
        "value": "E",
        "description": ("Returns Euler's number (approximately 2.71828). \n" "Example: E() returns 2.718281828..."),
        "function_type": "Math",
    },
    "GREATEST": {
        "key": "GREATEST",
        "value": "GREATEST",
        "description": (
            "Returns the greatest value among the arguments. \n"
            "Example: GREATEST(a, b, c) returns the maximum of a, b, c."
        ),
        "function_type": "Math",
    },
    "LEAST": {
        "key": "LEAST",
        "value": "LEAST",
        "description": (
            "Returns the least value among the arguments. \n" "Example: LEAST(a, b, c) returns the minimum of a, b, c."
        ),
        "function_type": "Math",
    },
    # =========================================================================
    # Regex Functions
    # =========================================================================
    "REGEX_REPLACE": {
        "key": "REGEX_REPLACE",
        "value": "REGEX_REPLACE",
        "description": (
            "Replaces all matches of a regex pattern with a replacement string. \n"
            "Example: REGEX_REPLACE(phone, '[^0-9]', '') removes all non-digits."
        ),
        "function_type": "Text",
    },
    "REGEX_EXTRACT": {
        "key": "REGEX_EXTRACT",
        "value": "REGEX_EXTRACT",
        "description": (
            "Extracts the matching group at specified index from a regex pattern. \n"
            "Example: REGEX_EXTRACT(email, '(.+)@(.+)', 1) extracts username."
        ),
        "function_type": "Text",
    },
    "REGEX_SEARCH": {
        "key": "REGEX_SEARCH",
        "value": "REGEX_SEARCH",
        "description": (
            "Returns TRUE if the regex pattern matches, FALSE otherwise. \n"
            "Example: REGEX_SEARCH(text, '^[0-9]+$') checks if text is numeric."
        ),
        "function_type": "Text",
    },
    # =========================================================================
    # String Functions (NEW)
    # =========================================================================
    "CAPITALIZE": {
        "key": "CAPITALIZE",
        "value": "CAPITALIZE",
        "description": (
            "Capitalizes the first letter of each word. \n" "Example: CAPITALIZE('hello world') returns 'Hello World'."
        ),
        "function_type": "Text",
    },
    "TRANSLATE": {
        "key": "TRANSLATE",
        "value": "TRANSLATE",
        "description": (
            "Replaces characters in a string based on a mapping. \n"
            "Example: TRANSLATE('abc', 'abc', '123') returns '123'."
        ),
        "function_type": "Text",
    },
    "LEVENSHTEIN": {
        "key": "LEVENSHTEIN",
        "value": "LEVENSHTEIN",
        "description": (
            "Returns the Levenshtein distance (edit distance) between two strings. \n"
            "Example: LEVENSHTEIN('kitten', 'sitting') returns 3."
        ),
        "function_type": "Text",
    },
    "SPLIT": {
        "key": "SPLIT",
        "value": "SPLIT",
        "description": (
            "Splits a string by a delimiter and returns an array. \n"
            "Example: SPLIT('a,b,c', ',') returns ['a', 'b', 'c']."
        ),
        "function_type": "Text",
    },
    "SPLIT_PART": {
        "key": "SPLIT_PART",
        "value": "SPLIT_PART",
        "description": (
            "Extracts the nth part from a string split by a delimiter. \n"
            "Position is 1-indexed (1 = first part, 2 = second part, etc.). \n"
            "Example: SPLIT_PART('a,b,c', ',', 2) returns 'b'."
        ),
        "function_type": "Text",
    },
    "ASCII": {
        "key": "ASCII",
        "value": "ASCII",
        "description": ("Returns the ASCII code of the first character. \n" "Example: ASCII('A') returns 65."),
        "function_type": "Text",
    },
    "INITCAP": {
        "key": "INITCAP",
        "value": "INITCAP",
        "description": (
            "Capitalizes the first letter of each word (alias for PROPER). \n"
            "Example: INITCAP('hello world') returns 'Hello World'."
        ),
        "function_type": "Text",
    },
    "LPAD": {
        "key": "LPAD",
        "value": "LPAD",
        "description": (
            "Left-pads a string with a specified character to a target length. \n"
            "Example: LPAD('5', 3, '0') returns '005'."
        ),
        "function_type": "Text",
    },
    "RPAD": {
        "key": "RPAD",
        "value": "RPAD",
        "description": (
            "Right-pads a string with a specified character to a target length. \n"
            "Example: RPAD('5', 3, '0') returns '500'."
        ),
        "function_type": "Text",
    },
    # =========================================================================
    # Null/Type Functions (NEW)
    # =========================================================================
    "FILL_NULL": {
        "key": "FILL_NULL",
        "value": "FILL_NULL",
        "description": (
            "Replaces null values with a specified value. \n" "Example: FILL_NULL(column, 0) replaces nulls with 0."
        ),
        "function_type": "Logic",
    },
    "NULLIF": {
        "key": "NULLIF",
        "value": "NULLIF",
        "description": (
            "Returns null if the two arguments are equal. \n" "Example: NULLIF(a, 0) returns null if a equals 0."
        ),
        "function_type": "Logic",
    },
    "ISNAN": {
        "key": "ISNAN",
        "value": "ISNAN",
        "description": (
            "Returns true if the value is NaN (Not a Number). \n" "Example: ISNAN(value) returns TRUE if value is NaN."
        ),
        "function_type": "Logic",
    },
    "ISINF": {
        "key": "ISINF",
        "value": "ISINF",
        "description": (
            "Returns true if the value is infinite. \n" "Example: ISINF(value) returns TRUE if value is infinity."
        ),
        "function_type": "Logic",
    },
    "TRY_CAST": {
        "key": "TRY_CAST",
        "value": "TRY_CAST",
        "description": (
            "Attempts to cast a value, returning null on failure. \n" "Example: TRY_CAST('abc', 'int') returns null."
        ),
        "function_type": "Logic",
    },
}
