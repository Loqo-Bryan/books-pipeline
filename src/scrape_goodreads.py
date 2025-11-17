from bs4 import BeautifulSoup
import json
import re
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from utils_isbn import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

#--------------------------------------------------------------------------------------------------------------

# configuración de rutas sistema y web
load_dotenv("../.env.example")
BASE_URL = os.getenv('GOODREADS_URL')
QUERY = os.getenv('QUERY_GOODREADS')
SEARCH_URL = f"{BASE_URL}{QUERY}"

OUT_DIR = os.getenv('LANDING_DIR')
print(OUT_DIR)
OUTPUT_JSON = os.path.join(OUT_DIR, "goodreads_books.json")

os.makedirs(OUT_DIR, exist_ok=True)

# configuración de Selenium
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/116.0.5845.140 Safari/537.36"
)
driver = webdriver.Chrome(options=options)  # webdriver.Firefox(options=options) para usar firefox
driver.get(SEARCH_URL)

# función para scrapear y parsear info limitada
def scrape_full_data():
    
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2) 

    # Esperar hasta que se carguen los elementos de los libros con 20s de espera
    try:
        WebDriverWait(driver, 20).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "a.bookTitle")
        )
    except:
        print("No se cargaron los resultados en el tiempo esperado.")
        driver.quit()
        exit()

    # Obtener HTML renderizado
    html = driver.page_source

    # parsear HTML
    soup = BeautifulSoup(html, "html.parser")
    
    results = []
    rows = soup.select("tr[itemtype='http://schema.org/Book']")[:15]  
    
    # Obtener información de los libros
    for row in rows:
        title_tag = row.select_one("a.bookTitle span")
        author_tag = row.select_one("a.authorName span")
        rating_tag = row.select_one("span.minirating")
        url_tag = row.select_one("a.bookTitle")

        if not title_tag:
            continue

        # info básica
        title = title_tag.text.strip()
        author = author_tag.text.strip() if author_tag else None
        minirating = rating_tag.text.strip() if rating_tag else ""
        
        # rating y número de votos
        rating_match = re.search(r"([0-9.]+)\s+avg rating", minirating)
        count_match = re.search(r"—\s+([\d,]+)\s+ratings", minirating)

        rating = float(rating_match.group(1)) if rating_match else None
        ratings_count = int(count_match.group(1).replace(",", "")) if count_match else None
        
        book_url = BASE_URL + url_tag["href"]

        # Extraer ISBN
        isbn10, isbn13 = extract_isbn(book_url)
        time.sleep(1)   
        
        results.append({
            "title": title,
            "author": author,
            "rating": rating,
            "ratings_count": ratings_count,
            "book_url": book_url,
            "isbn10": isbn10,
            "isbn13": isbn13
        })

    # diccionario con datos obtenidos del scrapeo
    scrape_data = {'books': results, 'html': html}

    return scrape_data

# funcion que gestiona el scrapeo por paginación de goodreads
def scrape_goodreads_limit(min_books=15):
    
    all_books = []
    page = 1
    url = SEARCH_URL

    # Bucle para scrapear según min_books(mínimo de libros en argumento)
    while len(all_books) < min_books:
        print(f"[INFO] Scrapeando página {page}: {url}")

        data = scrape_full_data()
        books = data['books']
        html = data['html']

        if not books:
            print("[WARN] No se encontraron libros en esta página.")
            break

        all_books.extend(books)

        # Buscar botón siguiente página
        soup = BeautifulSoup(html, "html.parser")
        next_page = soup.select_one("a.next_page")

        if not next_page or "disabled" in next_page.get("class", []):
            print("[INFO] No hay más páginas.")
            break

        next_href = next_page.get("href")
        url = BASE_URL + next_href
        page += 1

        time.sleep(1)

    return all_books[:min_books]

#--------------------------------------------------------------------------------------------------------------

# EJECUCIÓN PRINCIPAL

print("[INFO] Iniciando scraping...")

books = scrape_goodreads_limit()

# metadatos del scraping
metadata = {
    "scraped_at": datetime.now(timezone.utc).isoformat(),
    "records": len(books),
    "source": "Goodreads",
    "query": "data science",

    "selectors": {
        "row": "tr[itemtype='http://schema.org/Book']",
        "title": "a.bookTitle span",
        "author": "a.authorName span",
        "rating": "span.minirating",
    },

    "pauses": {
        "page_load_wait_seconds": 2,
        "isbn_request_pause_seconds": 1,
        "pagination_pause_seconds": 1,
        "explanation": (
            "Se aplican pausas con time.sleep() para respetar el rendimiento "
            "de Goodreads, evitar comportamiento agresivo de scraping y "
            "reducir el riesgo de bloqueos o rate limiting."
        )
    },

    "selenium": {
        "headless": True,
        "user_agent": "Chrome 120 custom UA",
        "page_wait_strategy": "WebDriverWait(driver, 20).until(a.bookTitle)"
    },

    "notes": "Renderizado por Selenium; incluye pausas entre peticiones."
}

output_data = {
    "metadata": metadata,
    "data": books
}

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(output_data, f, indent=2, ensure_ascii=False)

driver.quit()

print("[OK] Archivo goodreads_books.json generado en /landing")