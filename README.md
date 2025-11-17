# Books Pipeline — Extracción, Enriquecimiento y Estandarización de Libros

Pipeline ETL completo para obtener libros desde Goodreads (mediante Selenium + BeautifulSoup), enriquecerlos con Google Books API y producir un modelo canónico consolidado.  
Incluye control de calidad, metadatos de ingestión y esquema documentado.

---

## 📚 1. Objetivo del proyecto

Este pipeline:

1. **Extrae** ≥12–15 libros desde Goodreads, usando paginación real.
2. **Respeta buenas prácticas de scraping**:
   - Selenium para carga dinámica.
   - BeautifulSoup para parsing.
   - Pausas (`time.sleep`) configuradas para no saturar la web.
   - Selectores CSS anotados.
   - User-Agent propio.
3. **Enriquece** los libros con Google Books:
   - Búsqueda por **ISBN-13 → ISBN-10 → título+autor** (en ese orden).
   - Campos normalizados (idioma BCP-47, moneda ISO-4217, fechas ISO).
4. **Integra** los datos en un modelo canónico:
   - Reglas de deduplicación por ISBN-13 o ID sintético.
   - Surrogate key con SHA-1 cuando no existe ISBN.
   - Reglas de supervivencia (título más completo, fecha más específica, precio validado…).
5. **Genera outputs estándar**:
   - `/standard/dim_book.parquet`
   - `/standard/book_source_detail.parquet`
   - `/docs/quality_metrics.json`
   - `/docs/schema.md` (esquema extendido)
   - `/docs/ingest_summary.json`

---
---

## 🚀 2. Instalación y ejecución

```bash
python -m venv .venv
.venv\Scripts\activate  
pip install -r requirements.txt
python src/scrape_goodreads.py
python src/enrich_googlebooks.py
python src/integrate_pipeline.py
```
---
---

## 🧩 3. Dependencias principales

 - Incluidas en requirements.txt:
 - Scraping
 - selenium
 - beautifulsoup4
 - requests
 - Procesamiento
 - pandas
 - numpy
 - python-dateutil
 - pyarrow
 - Infraestructura / red
 - urllib3
 - certifi
 - idna

---
---

## 🔍 4. Metadatos documentados

  **4.1 En scrape_goodreads.py**

   - Selectores CSS anotados en comentarios.
   - User-Agent configurado para evitar bloqueos.
   - Paginación automática hasta obtener al menos 12–15 libros.
   - Pausas anti-baneo:
   - time.sleep(1.5–3.5) entre páginas.
   - implicitly_wait + WebDriverWait.

  **4.2 Archivos generados**

   - Todos los CSV usan:
   - Codificación: UTF-8
   - Separador: ,
   - Cabecera incluida
   - Tipos normalizados
   - Fechas ISO‐8601

  **4.3 Provenance y trazabilidad**

   - Cada fila tiene:
   - src_id único (goodreads:<row> / google_books:<row>).
   - source_files: lista JSON de archivos que contribuyeron.
   - ingest_ts ISO-8601.

---
---

## 🧠 5. Decisiones clave del diseño

  # Scraping suave

   - Selenium para cargar JS
   - BeautifulSoup para parsear
   - Pausas humanas (sleep)
   - Sin paralelismo para no castigar Goodreads

  # Normalización semántica

   - Fechas ISO (YYYY, YYYY-MM, YYYY-MM-DD)
   - Idioma BCP-47 estándar (ej. en, es)
   - Moneda ISO-4217 (ej. USD)
   - ISBN limpiado (- removidos)

  # Deduplicación fuerte

   - Preferencia por isbn13.
   - Si falta → uso de isbn10.
   - Si falta → fuzzy matching título+autor.
   - Si no existe ISBN → creación de canonical_id = "synth:<sha1_16>"

  # Reglas de supervivencia (Modelo canónico)

   - Título: el más largo/no-nulo.
   - Fecha: la más específica y más reciente.
   - Precio: el valor no nulo más alto disponible.
   - Autores: cadena más completa.
   - Origen: lista de src_id acumulada.

  # Control de calidad

   - Se calcula en quality_metrics.json:
   - Nº de filas leídas por origen
   - Campos faltantes por columna clave
   - Identificación de ISBN duplicados
   - Nº de IDs sintéticos generados
   - Ejemplos de synthetic IDs
   - Reglas de bloqueo si la calidad cae (≥90% títulos, unicidad ISBN, etc.)

---
---

## 📑 6. Archivos producidos

**landing/goodreads_books.json** -> Datos crudos desde Goodreads
**landing/googlebooks_books.csv** -> Datos enriquecidos desde Google Books
**standard/book_source_detail.parquet** -> Tabla detallada de origen, con trazabilidad
**standard/dim_book.parquet** -> Modelo canónico final
**docs/quality_metrics.json** -> Métricas de calidad
**docs/schema.md** -> Esquema completo del modelo
**docs/ingest_summary.json** -> Resumen global de ingestión

---

