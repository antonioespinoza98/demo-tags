import re
from pathlib import Path
import polars as pl
from pypdf import PdfReader
from pdf2image import convert_from_path
import pytesseract


POPLER_PATH = r"C:\Program Files\poppler-25.11.0\Library\bin"  
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def extract_text_from_pdf(path: str) -> str:
    """Return all text from a PDF using pypdf (no OCR)."""
    reader = PdfReader(path)
    pages_text = []
    for page in reader.pages:
        page_text = page.extract_text() or ""
        pages_text.append(page_text)
    return "\n".join(pages_text).strip()


def extract_text_with_ocr(path: str) -> str:
    """Convert PDF pages to images and run OCR."""
    kwargs = {}
    if POPLER_PATH:
        kwargs["poppler_path"] = POPLER_PATH

    images = convert_from_path(path, **kwargs)
    text_chunks = []
    for img in images:
        ocr_text = pytesseract.image_to_string(img)  
        text_chunks.append(ocr_text)
    return "\n".join(text_chunks).strip()


STOP_MARKERS = [
    r"\nmanufactured by",
    r"\nmade in",
    r"\nwarning",
    r"\nwww\.",
    r"\nkeep out of reach",
    r"\nuso t[oó]pico",
    r"\nmodo de empleo",
    r"\nmode d'emploi",
    r"\nkey ingredients",
    r"\ningredientes principales",
]


def clean_ingredients_string(s: str) -> str:
    """Normalize whitespace and trailing punctuation."""
    s = re.sub(r"\s+", " ", s)
    return s.strip(" \t\n\r;,.")  


def extract_inline_ingredients(text: str) -> str | None:
    """
    Extract ingredients from running text like:
    'Ingredients: ...' or 'INGREDIENTES: ...'
    (works for English, Spanish, FR mixed stuff).
    """
    if not text:
        return None

    patterns = [
        r"\bingredients?\b\s*[:\-]\s*(.+)",                  
        r"\bingredients/ingr[ée]dients\b\s*[:\-]\s*(.+)",    
        r"\bingredientes\b\s*[:\-]\s*(.+)",                  
        r"\bingredientes/ingredients\b\s*[:\-]\s*(.+)",      
    ]

    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            continue

        after = m.group(1)

        cut_pos = len(after)
        for sm in STOP_MARKERS:
            ms = re.search(sm, after, flags=re.IGNORECASE)
            if ms and ms.start() < cut_pos:
                cut_pos = ms.start()

        ingredients = clean_ingredients_string(after[:cut_pos])
        if ingredients:
            return ingredients

    return None

def _extract_name_from_line(line: str) -> str | None:
    """Best-effort extraction of ingredient name from one table row line."""
    if not line.strip():
        return None

    line = re.sub(r"^\s*\d+[\.\-]?\s+", "", line)

    m = re.search(r"\s\d{2,7}-\d{2}-\d\b", line)
    if m:
        return line[:m.start()].strip()

    m = re.search(r"\s\d+[.,]\d+", line)
    if m:
        return line[:m.start()].strip()

    m = re.search(r"\sc\.s\.p\.", line, flags=re.IGNORECASE)
    if m:
        return line[:m.start()].strip()

    # Fallback: full line
    return line.strip()


def extract_table_ingredients(text: str) -> str | None:
    if not text:
        return None

    headers = [
        r"INGREDIENTES/INGREDIENTS\s*\(INCI\)",
        r"INGREDIENTE\s+INCI",             
        r"Lista de ingredientes",          
        r"No\.\s*INCI name\s*CAS No\.",    
    ]

    lower_text = text.lower()
    all_names: list[str] = []

    for hpat in headers:
        m = re.search(hpat, text, flags=re.IGNORECASE)
        if not m:
            continue

        start = m.end()
        block = text[start:]

        stop_pats = [
            r"\nTotal\b",
            r"\nTotal %",          
            r"\nRangos de concentración",  
            r"\nREGULACI[ÓO]N COSMETICA",
            r"\nAN[ÁA]LISIS",      
            r"\n2\.\s*Fragrance/Perfume",  
        ]
        cut_pos = len(block)
        for sp in stop_pats:
            ms = re.search(sp, block, flags=re.IGNORECASE)
            if ms and ms.start() < cut_pos:
                cut_pos = ms.start()
        block = block[:cut_pos]

        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            if re.search(hpat, line, flags=re.IGNORECASE):
                continue
            if re.match(r"^%?\s*INCI\b", line, flags=re.IGNORECASE):
                continue

            name = _extract_name_from_line(line)
            if not name:
                continue

            if len(name) < 3:
                continue

            all_names.append(name)

    if not all_names:
        return None

    seen = set()
    unique = []
    for n in all_names:
        if n not in seen:
            seen.add(n)
            unique.append(n)

    return ", ".join(unique)


def extract_ingredients_from_pdf(path: str) -> str | None:

    text = extract_text_from_pdf(path)

    if len(text) < 40:
        text = extract_text_with_ocr(path)

    ingredients = extract_inline_ingredients(text)
    if ingredients:
        return ingredients

    ingredients = extract_table_ingredients(text)
    if ingredients:
        return ingredients

    return None


def build_ingredients_df(pdf_paths: list[str | Path]) -> pl.DataFrame:
    records = []
    for pdf in pdf_paths:
        pdf = Path(pdf)
        print(f"Procesando: {pdf.name}")
        ingredients = extract_ingredients_from_pdf(str(pdf))
        records.append(
            {
                "file_name": pdf.name,
                "ingredients": ingredients,
            }
        )
    return pl.DataFrame(records)


if __name__ == "__main__":
    pdfs = [
        "./pdfs/213089 TK COLOR STAY COND1000.pdf",
        "./pdfs/033465- OO BUTTERFLY 14 ml Etiqueta (v.0419).pdf",
        "./pdfs/2445311_30116710_2.pdf",
        "./pdfs/AAFF_ESTUCHE_LACABINE_AMPOLLAS_FACIALES_SUR_EUROPA_x10_EYE_CONTOUR_ES_EN_IT_PT.pdf",
        "./pdfs/COM-PURIFYING SCRUB-16JAN24.pdf",
        "./pdfs/FORM GO BUTTERFLY Cuantitativa (0419).pdf",
        "./pdfs/FORM_LACABINE AMPOLLA EYE CONTOUR.pdf",
        "./pdfs/FORMULA TK COLOR STAY CONDITIONER.pdf",
        "./pdfs/MO-Hair-Scalp-2024-PurifyingScrub-125ml-GL.pdf",
        "./pdfs/TBH Tone softener.pdf",
    ]

    df = build_ingredients_df(pdfs)
    print(df)
