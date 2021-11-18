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

request_key_to_column_name = {
    "TX_RGN_DESCR_NL" : "REGION",
    "TX_PROV_DESCR_NL" : "PROVINCE",
    "CD_SEX" : "SEX",
    "CD_AGE_CL" : "AGEGROUP"
}

db_column_name_to_request_key = {
    "Muni" : {
        "MUNI" : "TX_DESCR_NL"
    },
    "Population" : {
        "REFNIS" : "CD_REFNIS",
        "MUNI" : "TX_DESCR_NL",
        "PROVINCE" : "TX_PROV_DESCR_NL",
        "REGION" : "TX_RGN_DESCR_NL",
        "SEX" : "CD_SEX",
        "NATIONALITY" : "TX_NATLTY_NL",
        "AGE" : "CD_AGE",
        "POPULATION" : "MS_POPULATION"
    },
    "Education_Muni" : {
        "REFNIS" : "CD_MUNTY_REFNIS",
        "MUNI" : "TX_MUNTY_DESCR_NL",
        "EDUCATION" : "CD_ISCED_CL",
        "POPULATION" : "MS_POPULATION",
        "POPULATION25" : "MS_POPULATION_25"
    },
    "Education_Prov_Reg" : {
        "SEX" : "CD_SEX",
        "AGEGROUP" : "CD_AGE_CL",
        "ORIGIN" : "CD_DSCNT_CL",
        "EDUCATION" : "CD_ISCED_CL",
        "POPULATION" : "MS_POPULATION"
    }
}