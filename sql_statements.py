sql_create_cases_table = """
  IF OBJECT_ID(N'dbo.Cases', N'U') IS NULL
  BEGIN
      CREATE TABLE Cases(
          [DATE] DATE,
          PROVINCE VARCHAR(25),
          REGION VARCHAR(25),
          AGEGROUP VARCHAR(25),
          SEX VARCHAR(1),
          CASES INT
        )
    END
    """

sql_create_mort_table = """
    IF OBJECT_ID(N'dbo.Mort', N'U') IS NULL
    BEGIN
        CREATE TABLE Mort(
            [DATE] DATE,
            REGION VARCHAR(25),
            AGEGROUP VARCHAR(25),
            SEX VARCHAR(1),
            DEATHS INT
        )
    END"""

sql_create_muni_table = """
    IF OBJECT_ID(N'dbo.Muni', N'U') IS NULL
    BEGIN
        CREATE TABLE Muni(
            NIS5 VARCHAR(5),
            [DATE] DATE,
            TX_DESCR_NL VARCHAR(50),
            PROVINCE VARCHAR(25),
            REGION VARCHAR(25),
            CASES INT
        )
    END"""

sql_create_vaccins_table = """
    IF OBJECT_ID(N'dbo.Vaccins', N'U') IS NULL
    BEGIN
        CREATE TABLE Vaccins(
            [DATE] DATE,
            REGION VARCHAR(25),
            AGEGROUP VARCHAR(25),
            SEX VARCHAR(1),
            BRAND VARCHAR(50),
            DOSE VARCHAR(1),
            COUNT INT
        )
    END"""

sql_create_logging_table = """
    IF OBJECT_ID(N'dbo.Logging', N'U') IS NULL
    BEGIN
        CREATE TABLE Logging(
            DATE DATETIME,
            LOGGING TEXT,
            ROWS_AFFECTED INT
        )
    END"""

sql_create_population_table = """
    IF OBJECT_ID(N'dbo.Population', N'U') IS NULL
    BEGIN
        CREATE TABLE Population(
            REFNIS VARCHAR(5),
            MUNI VARCHAR(50),
            PROVINCE VARCHAR(25),
            REGION VARCHAR(25),
            SEX VARCHAR(1),
            NATIONALITY VARCHAR(25),
            AGE INT,
            POPULATION INT,
            YEAR INT
        )
    END"""
