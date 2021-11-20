sql_create_cases_table = """
  IF OBJECT_ID(N'dbo.Cases', N'U') IS NULL
  BEGIN
      CREATE TABLE Cases(
          ID INT identity(1, 1) primary key,
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
            ID INT identity(1, 1) primary key,
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
            ID INT identity(1, 1) primary key,
            NIS5 VARCHAR(5),
            [DATE] DATE,
            MUNI VARCHAR(50),
            PROVINCE VARCHAR(25),
            REGION VARCHAR(25),
            CASES INT
        )
    END"""

sql_create_vaccins_table = """
    IF OBJECT_ID(N'dbo.Vaccins', N'U') IS NULL
    BEGIN
        CREATE TABLE Vaccins(
            ID INT identity(1, 1) primary key,
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
            ID INT identity(1, 1) primary key,
            DATE DATETIME,
            LOGGING TEXT,
            ROWS_AFFECTED INT
        )
    END"""

sql_create_education_muni_table = """
    IF OBJECT_ID(N'dbo.Education_Muni', N'U') IS NULL
    BEGIN
        CREATE TABLE Education_Muni(
            ID INT identity(1, 1) primary key,
            PROVINCE VARCHAR(25),
            REGION VARCHAR(25),
            REFNIS VARCHAR(5),
            MUNI VARCHAR(50),
            EDUCATION VARCHAR(10),
            POPULATION INT,
            POPULATION25 INT
        )
    END"""

sql_create_education_age_origin_table = """
    IF OBJECT_ID(N'dbo.Education_Age_Origin', N'U') IS NULL
    BEGIN
        CREATE TABLE Education_Age_Origin(
            ID INT identity(1, 1) primary key,
            PROVINCE VARCHAR(25),
            REGION VARCHAR(25),
            SEX VARCHAR(1),
            AGEGROUP VARCHAR(25),
            ORIGIN VARCHAR(10),
            EDUCATION VARCHAR(10),
            POPULATION INT
        )
    END"""

sql_create_income_table = """
    IF OBJECT_ID(N'dbo.Income', N'U') IS NULL
    BEGIN
        CREATE TABLE Income(
            ID INT identity(1, 1) primary key,
            YEAR INT,
            REFNIS VARCHAR(5),
            INCOME BIGINT,
            POPULATION INT,
            MUNI VARCHAR(50),
            PROVINCE VARCHAR(25),
            REGION VARCHAR(25)
        )
    END"""