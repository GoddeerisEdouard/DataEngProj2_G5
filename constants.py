from collections import defaultdict
provinces = defaultdict(lambda : None ,{
    "Antwerpen" : "Antwerpen",
    "BrabantWallon" : "Waals-Brabant",
    "Brussels" : "Brussel",
    "Hainaut" : "Henegouwen",
    "Liège" : "Luik",
    "Limburg" : "Limburg",
    "Luxembourg" : "Luxemburg",
    "Namur" : "Namen",
    "OostVlaanderen" : "Oost-Vlaanderen",
    "VlaamsBrabant" : "Vlaams-Brabant",
    "WestVlaanderen" : "West-Vlaanderen"
})

regions = defaultdict(lambda : None, {
    "Brussels" : "Brussel",
    "Flanders" : "Vlaanderen",
    "Wallonia" : "Wallonië",
    "Ostbelgien" : "Oost-België"
})

request_value_to_column_value = {
    "Brussels Hoofdstedelijk Gewest" : "Brussels",
    "Vlaams Gewest" : "Flanders",
    "Waals Gewest" : "Wallonia"
}

code_to_prov_reg_name = {
    "BE1" : "Brussel",
    "BE2" : "Vlaanderen",
    "BE3" : "Wallonië",
    "BE10" : "Brussel",
    "BE21" : "Antwerpen",
    "BE22" : "Limburg",
    "BE23" : "Oost-Vlaanderen",
    "BE24" : "Vlaams-Brabant",
    "BE25" : "West-Vlaanderen",
    "BE31" : "Waals-Brabant",
    "BE32" : "Henegouwen",
    "BE33" : "Luik",
    "BE34" : "Luxemburg",
    "BE35" : "Namen"
}

request_key_to_column_name = {
    "TX_RGN_DESCR_NL" : "REGION",
    "TX_PROV_DESCR_NL" : "PROVINCE",
    "CD_SEX" : "SEX",
    "CD_AGE_CL" : "AGEGROUP",
    "MS_TOT_NET_INC" : "INCOME",
    "CD_REG" : "REGION",
    "CD_PROV" : "PROVINCE"
}

db_column_name_to_request_key = {
    "Muni" : {
        "MUNI" : "TX_DESCR_NL"
    },
    "Education_Muni" : {
        "REGION" : "CD_REG",
        "PROVINCE" : "CD_PROV",
        "REFNIS" : "CD_MUNTY_REFNIS",
        "MUNI" : "TX_MUNTY_DESCR_NL",
        "EDUCATION" : "CD_ISCED_CL",
        "POPULATION" : "MS_POPULATION",
        "POPULATION25" : "MS_POPULATION_25"
    },
    "Education_Age_Origin" : {
        "REGION" : "CD_REG",
        "PROVINCE" : "CD_PROV",
        "SEX" : "CD_SEX",
        "AGEGROUP" : "CD_AGE_CL",
        "ORIGIN" : "CD_DSCNT_CL",
        "EDUCATION" : "CD_ISCED_CL",
        "POPULATION" : "MS_POPULATION"
    },
    "Income" : {
        "YEAR" : "CD_YEAR",
        "REFNIS" : "CD_MUNTY_REFNIS",
        "INCOME" : "MS_TOT_NET_INC",
        "POPULATION" : "MS_TOT_RESIDENTS",
        "MUNI" : "TX_MUNTY_DESCR_NL",
        "PROVINCE" : "TX_PROV_DESCR_NL",
        "REGION" : "TX_RGN_DESCR_NL"
    }
}