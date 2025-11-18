# Schema: dim_book
Este documento describe el esquema final del dataset `dim_book`, incluyendo:
- Definición de campos (tipo, nullability, formato, ejemplo, reglas)
- Prioridades y fuentes (Goodreads vs Google Books)
- Reglas de deduplicación
- Reglas de supervivencia aplicadas durante la integración

## 1. Campos del dataset
| Campo | Tipo | Nullable | Formato | Ejemplo | Reglas |
|-------|------|----------|---------|---------|--------|
| isbn13 | string | False | ISBN-13 (13 dígitos, validados por utils_isbn) | 9781491957660 | Clave primaria del libro. Se elige siempre el ISBN-13 válido. Si no existe, la fila se descarta. |
| title | string | False | Texto UTF-8 | Data Science from Scratch | Se selecciona usando survival rule: priority Google Books > Goodreads. Debe existir ≥90% cobertura. |
| authors | string | True | Lista de autores separada por coma | Joel Grus | Survival rule: Google Books > Goodreads. |
| publisher | string | True | Texto libre | O'Reilly Media | Tomado exclusivamente de Google Books. |
| pub_date | string | True | Fecha ISO-8601 (YYYY, YYYY-MM o YYYY-MM-DD) | 2019-04-14 | Fecha original de Google Books, no normalizada más allá de ISO. |
| language | string | True | Código BCP-47 | en | Se usa el código de idioma proporcionado por Google Books. |
| categories | string | True | Lista separada por coma | Data Science, Machine Learning | Categorías directamente de Google Books. |
| price_amount | float | True | Número decimal positivo | 34.50 | Debe ser ≥0. Seleccionado desde Google Books si existe. |
| price_currency | string | True | Código ISO-4217 | USD | Moneda proporcionada por Google Books. |
| prov_title | string | True | goodreads | google_books | google_books | Indica de dónde procede el valor final de `title`. |
| prov_authors | string | True | goodreads | google_books | google_books | Indica de dónde procede el valor final de `authors`. |
| prov_price | string | True | google_books | google_books | Indica si el precio proviene de Google Books. |

## 2. Fuentes y prioridades

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
## 3. Reglas de deduplicación

- La clave de deduplicación es **isbn13**.
- Si existen múltiples registros con el mismo isbn13:
  - Se agrupan mediante `.groupby("isbn13")`.
  - Se resuelven conflictos vía *survival rules* descritas abajo.
- Si un registro no tiene isbn13 válido → se descarta.
## 4. Reglas de supervivencia

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
