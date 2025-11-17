import os
import glob
import json
import hashlib
import pandas as pd
from utils_quality import *
from utils_isbn import *

#--------------------------------------------------------------------------------------------------------------

# configuración de rutas sistema y web
LANDING_DIR = "../landing"
DOCS_DIR = "../docs"
STANDARD_DIR = "../standard"

os.makedirs(DOCS_DIR, exist_ok=True)
os.makedirs(STANDARD_DIR, exist_ok=True)

# leer archivos desde landing/ y anotar metadatos 
landing_files = glob.glob(os.path.join(LANDING_DIR, "*"))
file_metadata = []

json_records = []
csv_records = []

for fpath in landing_files:
    fname = os.path.basename(fpath)
    stat = os.stat(fpath)
    fmeta = {
        "file_name": fname,
        "path": f"/landing/{fpath[11:len(fpath)]}",
        "size_bytes": stat.st_size,
        "sha256": file_sha256(fpath),
        "ingest_timestamp": INGEST_TS,
    }
    file_metadata.append(fmeta)

    # carga contenido dependiendo de la extensión del archivo
    ext = os.path.splitext(fname)[1].lower()
    try:
        if ext in [".json"]:
            with open(fpath, "r", encoding="utf-8") as fh:
                json_full = json.load(fh)
                data = json_full['data']
                # acepta listas u objeto único para archivo JSON
                if isinstance(data, list):
                    json_records.extend(data)
                else:
                    json_records.append(data)
        elif ext in [".csv"]:
            df = pd.read_csv(fpath)
            csv_records.append((fname, df))
        else:
            pass
    except Exception as e:
        print(f"Warning: fallo al leer {fname}: {e}")

# normalizar y construir book_source_detail 
if json_records:
    df_json = pd.DataFrame(json_records)
else:
    df_json = pd.DataFrame(columns=["title","author","rating","ratings_count","book_url","isbn10","isbn13"])

# Concatenar todos los CSVs de Google Books (si hay más de uno)
if csv_records:
    dfs = []
    for fname, df in csv_records:
        df = df.copy()
        df["__source_file"] = fname
        dfs.append(df)
    df_csv = pd.concat(dfs, ignore_index=True, sort=False)
else:
    df_csv = pd.DataFrame(columns=[
        "gb_id","title","subtitle","authors","publisher","pub_date","language","categories",
        "isbn13","isbn10","price_amount","price_currency","__source_file"
    ])

# uniformizar nombres de columnas mínimas asegurando tipos str
for c in ["title","author","isbn10","isbn13"]:
    if c in df_json.columns:
        df_json[c] = df_json[c].astype(object)
for c in ["title","authors","isbn10","isbn13","pub_date","language","price_amount","price_currency"]:
    if c in df_csv.columns:
        df_csv[c] = df_csv[c].astype(object)

# Crear columna 'source' que identifique de donde viene cada fila
df_json["__source"] = "goodreads"
df_csv["__source"] = df_csv.get("__source_file", "google_books")

# Antes de merge limpiar ISBN evaluando tipos
def fast_clean_isbn(s): 
    if pd.isna(s): 
        return None 
    s = str(s).replace("-", "").strip() 
    return s if s != "" else None

for df in [df_json, df_csv]:
    if "isbn10" in df.columns:
        df["isbn10"] = df["isbn10"].apply(fast_clean_isbn)
    else:
        df["isbn10"] = None
    if "isbn13" in df.columns:
        df["isbn13"] = df["isbn13"].apply(fast_clean_isbn)
    else:
        df["isbn13"] = None

# garantizar la trazabilidad, crea identificadores únicos para cada fila de origen
df_json = df_json.reset_index().rename(columns={"index": "src_row_id"})
df_json["src_id"] = df_json["__source"] + ":" + df_json["src_row_id"].astype(str)

df_csv = df_csv.reset_index().rename(columns={"index": "src_row_id"})
df_csv["src_id"] = df_csv["__source"] + ":" + df_csv["src_row_id"].astype(str)

# Estrategia de merge por clave principal ISBN13, clave alternativa ISBN10, clave alternativa aproximada por título + autor
merged = pd.merge(df_json, df_csv, how="left", on="isbn13", suffixes=("_gr", "_gb"))

# para filas donde falta el ISBN13 para merge, probar merge con el ISBN10
missing_gb_mask = merged["gb_id"].isna() & merged["isbn10_gr"].notna()
if missing_gb_mask.any():
    
    gb_by_isbn10 = df_csv.dropna(subset=["isbn10"]).drop_duplicates(subset=["isbn10"]).set_index("isbn10")
    def pick_gb_by_isbn10(row):
        isbn10 = row.get("isbn10_gr")
        if pd.isna(isbn10):
            return None
        try:
            rec = gb_by_isbn10.loc[isbn10]
            # si rec es un DataFrame, si hay duplicados, elige el primero
            if isinstance(rec, pd.DataFrame):
                rec = rec.iloc[0]
            return rec.to_dict()
        except KeyError:
            return None
    fill_rows = []
    for i, row in merged[missing_gb_mask].iterrows():
        rec = pick_gb_by_isbn10(row)
        if rec:
            for k, v in rec.items():
                merged.at[i, k] = v

# Para resultados no coincidentes, probar coincidencia aproximada por título + autor
def fuzzy_match_gb(row):
    if pd.notna(row.get("gb_id")):
        return None
    t = str(row.get("title_gr", "")).lower().strip()
    a = str(row.get("author", "")).lower().strip()
    if t == "":
        return None

    for j, cand in df_csv.iterrows():
        t2 = str(cand.get("title", "")).lower()
        a2 = str(cand.get("authors", "")).lower()
        if t in t2 or t2 in t:           
            if a and a != "nan":
                
                if any(tok for tok in a.split() if tok in a2):
                    return cand.to_dict()
            else:
                return cand.to_dict()
    return None

for i, row in merged[merged["gb_id"].isna()].iterrows():
    rec = fuzzy_match_gb(row)
    if rec:
        for k, v in rec.items():
            merged.at[i, k] = v

# Ahora crea book_source_detail, una fila por fuente(Goodreads + campos coincidentes de Google Books)
book_source_detail = merged[[
    "src_id", "__source", "title_gr", "author", "rating", "ratings_count", "book_url",
    "isbn10_gr", "isbn13",
    "gb_id", "title", "subtitle", "authors", "publisher", "pub_date", "language", "categories",
    "isbn13", "isbn10", "price_amount", "price_currency"
]].copy()

# provenance por campo, devuelve la procedencia del campo canónico
def pick_prov(row, field_gr, field_gb):   
    v_gr = row.get(field_gr)
    v_gb = row.get(field_gb)
    if pd.notna(v_gb):
        return "google_books"
    if pd.notna(v_gr):
        return "goodreads"
    return None

book_source_detail["prov_title"]   = book_source_detail.apply(lambda r: pick_prov(r, "title_gr", "title"), axis=1)
book_source_detail["prov_authors"] = book_source_detail.apply(lambda r: pick_prov(r, "author", "authors"), axis=1)
book_source_detail["prov_price"]   = book_source_detail.apply(lambda r: pick_prov(r, "price_amount", "price_amount"), axis=1)

# normaliza campos no numericos
book_source_detail["pub_date_iso"] = book_source_detail["pub_date"].apply(normalize_pub_date)
book_source_detail["language_norm"] = book_source_detail["language"].apply(normalize_language)
book_source_detail["price_currency_norm"] = book_source_detail["price_currency"].apply(normalize_currency)
# normaliza campos numericos
book_source_detail["ratings_count"] = book_source_detail["ratings_count"].apply(safe_int)
book_source_detail["price_amount"] = pd.to_numeric(book_source_detail["price_amount"], errors="coerce")

# anotar metadatos de ingestión en cada fila
book_source_detail["ingest_ts"] = INGEST_TS
book_source_detail["source_files"] = book_source_detail.apply(
    lambda r: [meta["file_name"] for meta in file_metadata], axis=1
)

# Deduplicación y modelo canónico (dim_book)
def make_synthetic_id(row):
    s = (str(row.get("title_gr","")) + "|" + str(row.get("author",""))).lower()
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]

# crear campo de candidato a canonical_id
book_source_detail = book_source_detail.loc[:, ~book_source_detail.columns.duplicated()]
book_source_detail["canonical_isbn13"] = book_source_detail["isbn13"].apply(get_isbn13)
book_source_detail["canonical_id"] = book_source_detail["canonical_isbn13"].copy()

# rellena canonical_id con id sintetico si hace falta
for i, r in book_source_detail.iterrows():
    if not r["canonical_id"] or str(r["canonical_id"]).lower() in ["nan","none"]:
        book_source_detail.at[i, "canonical_id"] = "synth:" + make_synthetic_id(r)

book_source_detail = select_most_complete(book_source_detail)

# preferencia de valores no nulos; busqueda de los valores más completos
def choose_most_complete(series):   
    vals = [v for v in series if pd.notna(v)]
    if not vals:
        return None
    vals = sorted(vals, key=lambda x: (len(str(x)), str(x)), reverse=True)
    return vals[0]

# normalización extra por opción más específica y más reciente
def choose_pub_date(series):
    normalized = [(normalize_pub_date(v), v) for v in series]
    
    scored = []
    for iso, orig in normalized:
        if iso is None:
            score = (0, None)
        elif len(iso) == 10:
            score = (3, iso)
        elif len(iso) == 7:
            score = (2, iso)
        elif len(iso) == 4:
            score = (1, iso)
        else:
            score = (0, iso)
        scored.append((score, iso))
    scored = [s for s in scored if s[0][1] is not None]
    if not scored:
        return None
    # en caso de empate, seleccionar la fecha más reciente
    scored = sorted(scored, key=lambda x: (x[0][0], x[1]), reverse=True)
    return scored[0][1]

dim_rows = []
for cid, group in book_source_detail.groupby("canonical_id"):
    # identificar si alguna fila tiene isbn13
    isbn13_vals = [v for v in group["isbn13"] if pd.notna(v)]
    isbn13_final = isbn13_vals[0] if isbn13_vals else None

    title_final = choose_most_complete(group["title_gr"].tolist() + group["title"].tolist())
    subtitle_final = choose_most_complete(group["subtitle"])
    authors_final = choose_most_complete(group["authors"].tolist() + group["author"].tolist())
    publisher_final = choose_most_complete(group["publisher"])
    pub_date_final = choose_pub_date(group["pub_date"].tolist())
    language_final = choose_most_complete(group["language_norm"])
    categories_final = choose_most_complete(group["categories"])
    isbn10_final = choose_most_complete(group["isbn10"].tolist() + group["isbn10_gr"].tolist())
    price_amount_final = group["price_amount"].dropna().max() if group["price_amount"].notna().any() else None
    price_currency_final = choose_most_complete(group["price_currency_norm"])
    gb_id_final = choose_most_complete(group["gb_id"])

    # ratings info obtenida de goodreads
    ratings_count_final = group["ratings_count"].dropna().max() if group["ratings_count"].notna().any() else None
    rating_final = group["rating"].dropna().max() if group["rating"].notna().any() else None

    sources = list(group["src_id"].unique())


    dim_rows.append({
        "canonical_id": cid,
        "isbn13": isbn13_final,
        "isbn10": isbn10_final,
        "gb_id": gb_id_final,
        "title": title_final,
        "subtitle": subtitle_final,
        "authors": authors_final,
        "publisher": publisher_final,
        "pub_date": pub_date_final,
        "language": language_final,
        "categories": categories_final,
        "price_amount": price_amount_final,
        "price_currency": price_currency_final,
        "rating": rating_final,
        "ratings_count": ratings_count_final,
        "source_ids": sources,
        "ingest_ts": INGEST_TS
    })

# aserciones bloqueantes(filtro de calidad)

# ≥90% de filas deben tener título
title_ratio = book_source_detail["title_gr"].notna().mean()
assert title_ratio >= 0.90, \
    f"ERROR: Solo {title_ratio*100:.1f}% de títulos presentes (<90%)."

# unicidad de isbn13 en canónico
dup_isbn13 = book_source_detail["isbn13"].dropna().duplicated().any()
assert not dup_isbn13, \
    "ERROR: Se detectaron isbn13 duplicados en book_source_detail. Revisar merge."

# rangos básicos de precio
if "price_amount" in book_source_detail.columns:
    bad_price = book_source_detail["price_amount"].dropna().lt(0).any()
    assert not bad_price, "ERROR: Existen precios negativos, lo cual viola reglas de calidad."


dim_book = pd.DataFrame(dim_rows)

# quality metrics
metrics = {}
metrics["ingest_timestamp"] = INGEST_TS
metrics["files_read"] = [ {k:v for k,v in m.items()} for m in file_metadata ]
metrics["counts"] = {
    "goodreads_rows": len(df_json),
    "google_books_rows": len(df_csv),
    "merged_rows": len(book_source_detail),
    "canonical_rows_emitted": len(dim_book)
}

# missings por campo importante en dim_book
missing = {}
for col in ["isbn13","isbn10","title","authors","pub_date","language","price_amount"]:
    missing[col] = int(dim_book[col].isna().sum()) if col in dim_book.columns else None
metrics["missing_counts"] = missing
metrics["title_coverage_ratio"] = float(title_ratio)
metrics["isbn13_unique"] = not dup_isbn13
metrics["bad_price_values"] = bool(bad_price if 'bad_price' in locals() else False)

# duplicados detectados
metrics["duplicates_detected_by_isbn13"] = int(book_source_detail["isbn13"].duplicated(keep=False).sum()) if "isbn13" in book_source_detail.columns else 0

# lista de canonical_ids sintéticos(sin isbn13)
metrics["synthetic_ids_count"] = int(dim_book["isbn13"].isna().sum())
metrics["examples_synthetic_ids"] = dim_book.loc[dim_book["isbn13"].isna(), "canonical_id"].head(5).tolist()

# guardar quality metrics
with open(os.path.join(DOCS_DIR, "quality_metrics.json"), "w", encoding="utf-8") as fh:
    json.dump(metrics, fh, indent=2, ensure_ascii=False)

# Guardar Parquet outputs
# parquet de book_source_detail
bsd = book_source_detail.copy()
bsd["source_files"] = bsd["source_files"].apply(json.dumps)
bsd.to_parquet(os.path.join(STANDARD_DIR, "book_source_detail.parquet"), index=False, engine="pyarrow")

# parquet de dim_book 
dim_book.to_parquet(os.path.join(STANDARD_DIR, "dim_book.parquet"), index=False, engine="pyarrow")

# Generar schema.md
schema_lines = []

schema_lines.append("# Schema: dim_book\n")
schema_lines.append("Este documento describe el esquema final del dataset `dim_book`, incluyendo:\n")
schema_lines.append("- Definición de campos (tipo, nullability, formato, ejemplo, reglas)\n")
schema_lines.append("- Prioridades y fuentes (Goodreads vs Google Books)\n")
schema_lines.append("- Reglas de deduplicación\n")
schema_lines.append("- Reglas de supervivencia aplicadas durante la integración\n\n")

schema_lines.append("## 1. Campos del dataset\n")

field_schema = [
    {
        "name": "isbn13",
        "type": "string",
        "nullable": False,
        "format": "ISBN-13 (13 dígitos, validados por utils_isbn)",
        "example": "9781491957660",
        "rules": "Clave primaria del libro. Se elige siempre el ISBN-13 válido. Si no existe, la fila se descarta."
    },
    {
        "name": "title",
        "type": "string",
        "nullable": False,
        "format": "Texto UTF-8",
        "example": "Data Science from Scratch",
        "rules": "Se selecciona usando survival rule: priority Google Books > Goodreads. Debe existir ≥90% cobertura."
    },
    {
        "name": "authors",
        "type": "string",
        "nullable": True,
        "format": "Lista de autores separada por coma",
        "example": "Joel Grus",
        "rules": "Survival rule: Google Books > Goodreads."
    },
    {
        "name": "publisher",
        "type": "string",
        "nullable": True,
        "format": "Texto libre",
        "example": "O'Reilly Media",
        "rules": "Tomado exclusivamente de Google Books."
    },
    {
        "name": "pub_date",
        "type": "string",
        "nullable": True,
        "format": "Fecha ISO-8601 (YYYY, YYYY-MM o YYYY-MM-DD)",
        "example": "2019-04-14",
        "rules": "Fecha original de Google Books, no normalizada más allá de ISO."
    },
    {
        "name": "language",
        "type": "string",
        "nullable": True,
        "format": "Código BCP-47",
        "example": "en",
        "rules": "Se usa el código de idioma proporcionado por Google Books."
    },
    {
        "name": "categories",
        "type": "string",
        "nullable": True,
        "format": "Lista separada por coma",
        "example": "Data Science, Machine Learning",
        "rules": "Categorías directamente de Google Books."
    },
    {
        "name": "price_amount",
        "type": "float",
        "nullable": True,
        "format": "Número decimal positivo",
        "example": "34.50",
        "rules": "Debe ser ≥0. Seleccionado desde Google Books si existe."
    },
    {
        "name": "price_currency",
        "type": "string",
        "nullable": True,
        "format": "Código ISO-4217",
        "example": "USD",
        "rules": "Moneda proporcionada por Google Books."
    },
    {
        "name": "prov_title",
        "type": "string",
        "nullable": True,
        "format": "goodreads | google_books",
        "example": "google_books",
        "rules": "Indica de dónde procede el valor final de `title`."
    },
    {
        "name": "prov_authors",
        "type": "string",
        "nullable": True,
        "format": "goodreads | google_books",
        "example": "google_books",
        "rules": "Indica de dónde procede el valor final de `authors`."
    },
    {
        "name": "prov_price",
        "type": "string",
        "nullable": True,
        "format": "google_books",
        "example": "google_books",
        "rules": "Indica si el precio proviene de Google Books."
    },
]

# Generación de tabla
schema_lines.append("| Campo | Tipo | Nullable | Formato | Ejemplo | Reglas |\n")
schema_lines.append("|-------|------|----------|---------|---------|--------|\n")

for f in field_schema:
    schema_lines.append(
        f"| {f['name']} | {f['type']} | {f['nullable']} | {f['format']} | {f['example']} | {f['rules']} |\n"
    )

schema_lines.append("\n")

# FUENTES Y PRIORIDADES
schema_lines.append("## 2. Fuentes y prioridades\n")
schema_lines.append("""
| Campo | Fuente primaria | Fuente secundaria | Regla de prioridad |
|-------|-----------------|-------------------|--------------------|
| title | Google Books | Goodreads | Google Books prevalece si existe. |
| authors | Google Books | Goodreads | Google Books prevalece. |
| isbn13 | Goodreads/Google Books | — | Validación obligatoria. Sin ISBN-13 no se carga. |
| publisher | Google Books | — | Solo GB. |
| pub_date | Google Books | — | Fecha original. |
| language | Google Books | — | BCP-47. |
| categories | Google Books | — | Lista GB. |
| price_amount | Google Books | — | Solo GB. |
| price_currency | Google Books | — | ISO-4217. |
""")

# REGLAS DE DEDUPLICACIÓN
schema_lines.append("## 3. Reglas de deduplicación\n")
schema_lines.append("""
- La clave de deduplicación es **isbn13**.
- Si existen múltiples registros con el mismo isbn13:
  - Se agrupan mediante `.groupby("isbn13")`.
  - Se resuelven conflictos vía *survival rules* descritas abajo.
- Si un registro no tiene isbn13 válido → se descarta.
""")

# REGLAS DE SUPERVIVENCIA
schema_lines.append("## 4. Reglas de supervivencia\n")
schema_lines.append("""
### Lógica general
Para cada grupo (por isbn13), se aplica:

1. **Preferencia Google Books > Goodreads**  
   Siempre que Google Books tenga un valor no nulo, se usa ese.

2. **Criterio de mayor completitud**  
   Para campos de texto donde ambas fuentes existen:
   - Se elige el más largo (mayor información).
   - En caso de empate, Google Books gana.

3. **Requisitos mínimos**
   - ≥90% de títulos debe estar presente en la tabla.
   - isbn13 debe ser único y válido.
   - price_amount ≥ 0.

### Campos con survival rule explícita
- **title** → GB > GR  
- **authors** → GB > GR  
- **isbn10** → GB > GR  
- **price_amount** → GB  
- **publisher / pub_date / language / categories** → solo GB  
""")

# Guardar schema
schema_path = os.path.join("../docs", "schema.md")
with open(schema_path, "w", encoding="utf-8") as f:
    f.writelines(schema_lines)

# Report summary 
summary = {
    "output_files": {
        "dim_book_parquet": os.path.join("/standard/", "dim_book.parquet"),
        "book_source_detail_parquet": os.path.join("/standard/", "book_source_detail.parquet"),
        "quality_metrics": os.path.join("/docs/", "quality_metrics.json"),
        "schema_md": os.path.join("/docs/", "schema.md")
    },
    "counts": metrics["counts"],
    "ingest_ts": INGEST_TS
}
with open(os.path.join(DOCS_DIR, "ingest_summary.json"), "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2, ensure_ascii=False)

print("\n---------------------------INGESTION COMPLETADA-------------------------")
print("\nArchivos escritos en /docs y /standard")
print("Resumen:", json.dumps(summary, indent=2))
