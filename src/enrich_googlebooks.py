import requests
import json
import csv
import urllib.parse
import os
from utils_quality import *
from utils_isbn import *

#--------------------------------------------------------------------------------------------------------------

# configuración de rutas sistema y web
LANDING_DIR = "../landing"
INPUT_JSON = os.path.join(LANDING_DIR, "goodreads_books.json")
OUTPUT_CSV = os.path.join(LANDING_DIR, "googlebooks_books.csv")

# función para obtener info de libros usando la API de googlebooks
def search_google_books(isbn10, isbn13, title, author):
    """
    Busca el libro en Google Books y devuelve un registro normalizado.
    Prioriza ISBN-13 > ISBN-10 > título+autor.
    """

    # búsqueda por ISBN preferente
    if isbn13:
        url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn13}"
    elif isbn10:
        url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn10}"
    else:
        # búsqueda por título + autor
        q_title = urllib.parse.quote(title or "")
        q_author = urllib.parse.quote(author or "")
        url = f"https://www.googleapis.com/books/v1/volumes?q=intitle:{q_title}+inauthor:{q_author}"

    r = requests.get(url)
    data = r.json()

    if "items" not in data:
        return None

    info = data["items"][0].get("volumeInfo", {})
    sale = data["items"][0].get("saleInfo", {})

    # Extraer ISBNs normalizados
    isbn_10, isbn_13 = None, None
    for id_obj in info.get("industryIdentifiers", []):
        if id_obj["type"] == "ISBN_10":
            isbn_10 = id_obj["identifier"]
        elif id_obj["type"] == "ISBN_13":
            isbn_13 = normalize_isbn13(id_obj["identifier"])

    # extraer precio si existe
    price_amount = None
    price_currency = None
    if sale.get("listPrice"):
        price_amount = sale["listPrice"].get("amount")
        price_currency = sale["listPrice"].get("currencyCode")  

    # Normalización final
    return {
        "gb_id": data["items"][0].get("id"),
        "title": info.get("title"),
        "subtitle": info.get("subtitle"),
        "authors": ", ".join(info.get("authors", [])),
        "publisher": info.get("publisher"),

        # fechas normalizadas ISO (manteniendo precisión de Google Books)
        "pub_date": normalize_pub_date(info.get("publishedDate")),

        # idioma BCP-47
        "language": normalize_language(info.get("language")),

        "categories": ", ".join(info.get("categories", [])),

        # ISBNs normalizados
        "isbn13": isbn_13,
        "isbn10": isbn_10,

        # moneda ISO-4217
        "price_amount": price_amount,
        "price_currency": price_currency,
    }

# función que ejecuta busqueda en googlebooks usando el archivo JSON scrapeado como referencia
def enrich_books():
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
        books = data['data']

    enriched = []

    for b in books:

        gdata = search_google_books(
            isbn10=b.get("isbn10"),
            isbn13=b.get("isbn13"),
            title=b.get("title"),
            author=b.get("author")
        )

        if gdata:
            enriched.append(gdata)

    # guardar CSV UTF-8 con esquema claro
    fieldnames = [
        "gb_id",
        "title", "subtitle", "authors", "publisher",
        "pub_date", "language", "categories",
        "isbn13", "isbn10",
        "price_amount", "price_currency"
    ]

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched)

    print("CSV googlebooks_books.csv generado en /landing")


if __name__ == "__main__":
    enrich_books()