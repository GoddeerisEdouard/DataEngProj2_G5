sql_create_cases_table = """
  IF OBJECT_ID(N'dbo.Cases', N'U') IS NULL
  BEGIN
      CREATE TABLE Cases(
           [DATE] DATE,
           PROVINCE TEXT,
           REGION TEXT,
           AGEGROUP TEXT,
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
            REGION TEXT,
            AGEGROUP TEXT,
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
            MUNI TEXT,
            PROVINCE TEXT,
            REGION TEXT,
            CASES INT
        )
    END"""

sql_create_vaccins_table = """
    IF OBJECT_ID(N'dbo.Vaccins', N'U') IS NULL
    BEGIN
        CREATE TABLE Vaccins(
            [DATE] DATE,
            REGION TEXT,
            AGEGROUP TEXT,
            SEX VARCHAR(1),
            BRAND TEXT,
            DOSE VARCHAR(1),
            COUNT INT
        )
    END"""

sql_create_logging_table = """
    IF OBJECT_ID(N'dbo.Logging', N'U') IS NULL
    BEGIN
        CREATE TABLE Logging(
            LOGGING TEXT
        )
    END"""
