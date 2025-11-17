import hashlib
from datetime import datetime, timezone
from dateutil import parser as dateparser

# -------------------- LIMPIEZA DE DATOS --------------------

# generador de clave única para archivo en /landing
def file_sha256(path, block_size=65536):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            h.update(block)
    return h.hexdigest()

# normaliza fechas a formatos ISO
def normalize_pub_date(d):
    
    if d is None:
        return None
    d = str(d).strip()
    if d == "":
        return None
    
    try:
        # Si solo dígitos y 4 chars => año
        if d.isdigit() and len(d) == 4:
            return d  
        # Si coincide con YYYY-MM
        if len(d) == 7 and d[4] == '-':
            return d
        # parse completo
        dt = dateparser.parse(d, fuzzy=True)
        return dt.date().isoformat()  
    except Exception:
        return None

# mapeos simplificados para idiomas y monedas, dominios
LANG_ALIAS = {
    "eng": "en", "en-US": "en", "en_US": "en", "english": "en",
    "spa": "es", "es-ES": "es", "spanish": "es",
    "fr": "fr", "fra": "fr", "fre": "fr",
    None: None
}
CURRENCY_ALIAS = {
    "USD": "USD", "US$": "USD", "$": "USD",
    "EUR": "EUR", "€": "EUR",
    "GBP": "GBP", "£": "GBP",
    None: None
}

# normaliza lenguajes(sin espacios, minusculas y len=2 por key:value según dominios)
def normalize_language(l):
    if not l:
        return None
    s = str(l).strip()
    s_low = s.lower()
    
    for k, v in LANG_ALIAS.items():
        if k and k.lower() == s_low:
            return v    
    if len(s) >= 2:
        return s[:2].lower()
    return s

# normaliza moneda(sin espacios y mayusculas por key:value según dominios)
def normalize_currency(c):
    if not c:
        return None
    s = str(c).strip()
    return CURRENCY_ALIAS.get(s, s.upper())

# castea str a int simple con try/catch
def safe_int(x):
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None

# supervivencia de registros con ISBN duplicado -> por menor nulabilidad
def select_most_complete(df):
    # Cuenta valores no nulos por fila
    df["_notnull_count"] = df.notna().sum(axis=1)
    df_sorted = df.sort_values("_notnull_count", ascending=False)
    df_dedup = df_sorted.drop_duplicates(subset=["isbn13"], keep="first").copy()
    df_dedup.drop(columns=["_notnull_count"], inplace=True)
    return df_dedup

# timestap de la ingesta
INGEST_TS = datetime.now(timezone.utc).isoformat()