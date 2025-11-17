import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

# ---------------------------------------- ISBN TOOLS ----------------------------------------

# extraer el ISBN desde la pagina individual del libro
def extract_isbn(book_url):
    
    r = requests.get(book_url)
    soup = BeautifulSoup(r.text, "html.parser")
    
    isbn10, isbn13 = None, None
    
    # Buscar cualquier cadena ISBN en la sección BookDataBox
    data_box = soup.find("div", {"id": "bookDataBox"})
    if data_box:
        text = data_box.get_text(" ", strip=True)
        match_10 = re.search(r"ISBN(?:\-10)?:?\s?(\d{10})", text)
        match_13 = re.search(r"ISBN(?:\-13)?:?\s?(\d{13})", text)
        if match_10: isbn10 = match_10.group(1)
        if match_13: isbn13 = match_13.group(1)
    
    return isbn10, isbn13

# extraer el ISBN de un dataframe evaluando tipos
def get_isbn13(val):
    # Si es lista o Series, tomar solo el primer elemento
    if isinstance(val, (list, pd.Series)) and len(val) > 0:
        return val[0]
    # Si es un valor escalar no nulo
    elif not pd.isna(val):
        return val
    # Si es nulo u objeto vacío
    else:
        return None

# valida el ISBN por no espacios, len=13 y solo numerico
def normalize_isbn13(isbn):
    if not isbn:
        return None
    isbn_digits = isbn.replace("-", "").strip()
    return isbn_digits if isbn_digits.isdigit() and len(isbn_digits) == 13 else None