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
    "WestVlaanderen" : "West-Vlaanderen",
    None : None
})

regions = defaultdict(lambda : None, {
    "Brussels Hoofdstedelijk Gewest" : "Brussel",
    "Brussels" : "Brussel",
    "Vlaams Gewest" : "Vlaanderen",
    "Flanders" : "Vlaanderen",
    "Waals Gewest" : "Wallonië",
    "Wallonia" : "Wallonië",
    "Ostbelgien" : "Oost-België",
    None : None
})