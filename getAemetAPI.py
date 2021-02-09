
def getAPI(pwd):
    if pwd == "yourPassword":
        return "your AEMET API"
    else:
        raise Exception("No tienes acceso a la API Key")
