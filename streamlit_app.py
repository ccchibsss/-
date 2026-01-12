from __future__ import annotations
import io
import re
import sys
import json
import os
import argparse
import requests
import pandas as pd
from difflib import SequenceMatcher
from functools import lru_cache
from collections import Counter
from typing import Optional, Dict, Set, List
import concurrent.futures

# Попытка подключить streamlit/altair для визуализации
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None
try:
    import altair as alt  # type: ignore
except Exception:
    alt = None

# Попытка подключить pymorphy2
try:
    import pymorphy2  # type: ignore
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

ADDITIONS_FILE = "additional_brands.json"

# Полный словарь брендов и моделей (часть, можно расширять)
car_brands_models: Dict[str, str] = {
    "BMW": "БМВ",
    "1 Series": "1 Серия", "2 Series": "2 Серия", "3 Series": "3 Серия",
    # ... (оставьте полный словарь, как в вашем скрипте)
}

# --- Обработка морфологии и транслитерации ---
@lru_cache(maxsize=10000)
def decline_word_cached(word: str) -> str:
    if not word or morph is None:
        return word
    try:
        p = morph.parse(word)[0]
        inf = p.inflect({"nomn"})
        return inf.word if inf else p.word
    except Exception:
        return word

LAT_TO_CYR_RULES = [
    ("shch", "щ"), ("sch", "щ"), ("sht", "шт"),
    ("oye", "ое"), ("oyu", "ою"), ("iya", "ия"), ("iye", "ие"),
    ("aye", "ая"), ("ayu", "аю"), ("eyu", "ею"), ("iu", "ю"),
    ("ia", "ия"), ("yo", "ё"), ("yu", "ю"), ("ya", "я"),
    ("zh", "ж"), ("kh", "х"), ("ts", "ц"), ("ch", "ч"),
    ("sh", "ш"), ("ye", "е"), ("ja", "я"), ("ju", "ю"),
    ("je", "е"),
    ("a", "а"), ("b", "б"), ("v", "в"), ("g", "г"), ("d", "д"),
    ("e", "е"), ("z", "з"), ("i", "и"), ("k", "к"), ("l", "л"),
    ("m", "м"), ("n", "н"), ("o", "о"), ("p", "п"), ("r", "р"),
    ("s", "с"), ("t", "т"), ("u", "у"), ("f", "ф"), ("y", "ы"),
    ("j", "й"), ("'", "ь"), ('"', "ъ"), ("x", "кс"), ("q", "к"), ("w", "в"),
]
_LAT_RULES_SORTED = sorted(LAT_TO_CYR_RULES, key=lambda x: -len(x[0]))

def latin_to_cyrillic(text: str) -> str:
    if not isinstance(text, str) or not text:
        return text
    def translit_word(word: str) -> str:
        lower = word.lower()
        i = 0
        out = []
        while i < len(lower):
            matched = False
            for lat, cyr in _LAT_RULES_SORTED:
                if lower.startswith(lat, i):
                    out.append(cyr)
                    i += len(lat)
                    matched = True
                    break
            if not matched:
                out.append(lower[i])
                i += 1
        out_s = "".join(out)
        if word.isupper():
            return out_s.upper()
        if word[0].isupper():
            return out_s.capitalize()
        return out_s
    parts = re.split(r'(\s+)', text)
    res = []
    for p in parts:
        if re.search(r'[A-Za-z]', p):
            pieces = re.split(r'([^A-Za-z]+)', p)
            for s in pieces:
                res.append(translit_word(s) if re.search(r'[A-Za-z]', s) else s)
        else:
            res.append(p)
    return "".join(res)

def contains_latin(text: str) -> bool:
    return bool(re.search(r'[A-Za-z]', str(text)))

def contains_cyrillic(text: str) -> bool:
    return bool(re.search(r'[\u0400-\u04FF]', str(text)))

def build_final_struct(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict:
    final_map = {**base_map, **(additions or {})}
    if not final_map:
        return {"pattern": None, "map": {}, "len_max": 0}
    keys_sorted = sorted(final_map.keys(), key=len, reverse=True)
    escaped = [re.escape(k) for k in keys_sorted if k.strip()]
    pattern = re.compile(r'(?<!\w)(?:' + "|".join(escaped) + r')(?!\w)', flags=re.IGNORECASE)
    mapping: Dict[str, tuple] = {}
    for k in keys_sorted:
        ru = final_map.get(k) or k
        ru_decl = decline_word_cached(ru)
        mapping[k.lower()] = (k, ru_decl)
    return {"pattern": pattern, "map": mapping, "len_max": max((len(k) for k in final_map.keys()), default=0)}

def process_text_fast_core(text: str, final_struct: dict, translit_allowed: bool=True) -> str:
    if not isinstance(text, str) or not final_struct:
        return text
    if translit_allowed and contains_latin(text) and not contains_cyrillic(text):
        cyr = latin_to_cyrillic(text)
        return f"{text} ({decline_word_cached(cyr)})"
    pattern = final_struct.get("pattern")
    mapping = final_struct.get("map", {})
    if not pattern:
        return text
    def repl(m):
        f = m.group(0)
        info = mapping.get(f.lower())
        if info:
            return f"{f} ({info[1]})"
        return f
    return pattern.sub(repl, str(text))

def process_texts_parallel(texts: List[str], final_struct: dict, translit_allowed: bool=True) -> List[str]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(process_text_fast_core, text, final_struct, translit_allowed)
            for text in texts
        ]
        return [f.result() for f in futures]

# --- Вспомогательные функции ---

def load_external_data(url: Optional[str]) -> pd.DataFrame:
    if not url:
        return pd.DataFrame()
    try:
        if url.startswith("http://") or url.startswith("https://"):
            response = requests.get(url)
            response.raise_for_status()
            content = io.BytesIO(response.content)
            if url.lower().endswith(('.xls', '.xlsx')):
                return pd.read_excel(content)
            else:
                return pd.read_csv(content)
        else:
            if os.path.exists(url):
                if url.lower().endswith(('.xls', '.xlsx')):
                    return pd.read_excel(url)
                else:
                    return pd.read_csv(url)
    except Exception as e:
        print(f"Ошибка загрузки внешних данных: {e}")
    return pd.DataFrame()

def extract_words_from_series(series: pd.Series) -> set:
    words = set()
    for text in series.dropna():
        for word in re.findall(r'\b\w+\b', str(text)):
            words.add(word.lower())
    return words

def prepare_additions_fast(base_keys: set, candidates: set, threshold: float=0.85) -> Dict[str, str]:
    additions = {}
    for candidate in candidates:
        best_match = None
        highest_ratio = 0.0
        for key in base_keys:
            ratio = SequenceMatcher(None, candidate, key).ratio()
            if ratio > highest_ratio:
                highest_ratio = ratio
                best_match = key
        if highest_ratio >= threshold and best_match:
            additions[best_match] = candidate
    return additions

def save_additions():
    try:
        with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(added_pairs, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения дополнений: {e}")

# --- Основные переменные ---
@lru_cache(maxsize=10000)
def decline_word_cached(word: str) -> str:
    if not word or morph is None:
        return word
    try:
        p = morph.parse(word)[0]
        inf = p.inflect({"nomn"})
        return inf.word if inf else p.word
    except Exception:
        return word

# --- Веб-интерфейс (Streamlit) ---
def run_streamlit_app() -> None:
    if st is None:
        print("Streamlit не доступен.")
        return
    st.set_page_config(page_title="Автообработка (ускорено)", layout="wide")
    st.title("Распознавание брендов/моделей — ускорённый модуль")
    st.markdown("Загрузите CSV/XLSX, выберите столбец — скрипт автоматически подсветит и добавит переводы.")

    sidebar = st.sidebar
    threshold = sidebar.slider("Порог похожести для автодобавления", 0.6, 0.99, 0.85, 0.01)
    translit_allowed = sidebar.checkbox("Автотранслитерация (латиница→кириллица)", value=True)

    new_k = sidebar.text_input("Ключ (англ)")
    new_v = sidebar.text_input("Русское название")
    if sidebar.button("Добавить в словарь"):
        if new_k and new_v:
            car_brands_models[new_k] = new_v
            save_additions()
            st.success(f"Добавлено: {new_k} → {new_v}")
        else:
            st.error("Поля обязательны")

    uploaded = st.file_uploader("Загрузите CSV/XLSX", type=["csv", "xls", "xlsx"])
    external_url = st.text_input("URL внешнего источника (CSV/XLSX) — необязательно")
    if not uploaded:
        st.info("Загрузите файл выше, чтобы начать.")
        return

    try:
        if uploaded.name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(uploaded)
        else:
            df = pd.read_csv(uploaded)
    except Exception as e:
        st.error(f"Не удалось прочитать файл: {e}")
        return

    col = st.selectbox("Столбец для обработки", df.columns.tolist())

    if st.button("Обработать"):
        external_df = load_external_data(external_url) if external_url else pd.DataFrame()
        series = df[col]
        dataset_words = extract_words_from_series(series)
        external_words = extract_words_from_series(external_df.stack()) if not external_df.empty else set()
        base_keys = set(car_brands_models.keys())
        candidates = (dataset_words | external_words) - base_keys

        # Обновление словаря кандидатами
        additions = prepare_additions_fast(base_keys, candidates, threshold=threshold)
        if additions:
            car_brands_models.update(additions)
            save_additions()

        final_struct = build_final_struct(car_brands_models, additions)
        # Обработка текста параллельно
        texts = df[col].astype(str).tolist()
        processed_texts = process_texts_parallel(texts, final_struct, translit_allowed)
        df[f"{col}_trans"] = processed_texts

        # Визуализация подсветки
        patt = final_struct.get("pattern")
        mapping = final_struct.get("map", {})
        def highlight_html(text: str) -> str:
            if not text or not patt:
                return text or ""
            def rep(m):
                f = m.group(0)
                info = mapping.get(f.lower())
                if info:
                    return f"<mark style='background:#fffd8a'>{f} ({info[1]})</mark>"
                return f
            return patt.sub(rep, str(text))
        rows = []
        preview = df.head(200)
        for idx in preview.index:
            orig = preview.at[idx, col]
            highlighted = highlight_html(orig)
            rows.append(f"<tr><td style='padding:6px;border:1px solid #ddd'><code>{str(orig)}</code></td>"
                        f"<td style='padding:6px;border:1px solid #ddd'>{highlighted}</td></tr>")
        table_html = "<table style='width:100%;border-collapse:collapse'><thead><tr><th>Оригинал</th><th>Подсветка</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
        st.markdown(table_html, unsafe_allow_html=True)

        export = st.radio("Формат экспорта", ("CSV", "Excel"))
        if export == "Excel":
            buf = io.BytesIO()
            df.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button("Скачать Excel", buf, file_name="result.xlsx")
        else:
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button("Скачать CSV", csv_bytes, file_name="result.csv")


# --- CLI обработка ---
def process_file_cli(input_path: str, column: str, external_url: Optional[str], output_path: Optional[str]) -> None:
    try:
        if input_path.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(input_path)
        else:
            df = pd.read_csv(input_path)
    except Exception as e:
        print("Ошибка чтения файла:", e)
        return
    if column not in df.columns:
        print("Столбец не найден. Доступные:", df.columns.tolist())
        return
    external_df = load_external_data(external_url) if external_url else pd.DataFrame()
    series = df[column]
    dataset_words = extract_words_from_series(series)
    external_words = extract_words_from_series(external_df.stack()) if not external_df.empty else set()
    base_keys = set(car_brands_models.keys())
    candidates = (dataset_words | external_words) - base_keys
    additions = prepare_additions_fast(base_keys, candidates, threshold=0.85)
    if additions:
        car_brands_models.update(additions)
        save_additions()
    final_struct = build_final_struct(car_brands_models, additions)
    df[column] = df[column].fillna("").astype(str).apply(lambda v: process_text_fast(v, final_struct, translit_allowed=True))
    if not output_path:
        output_path = "result.xlsx" if input_path.lower().endswith(('.xls', '.xlsx')) else "result.csv"
    try:
        if output_path.lower().endswith(('.xls', '.xlsx')):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)
        print("Результат сохранён в", output_path)
    except Exception as e:
        print("Ошибка сохранения:", e)

# --- Основной запуск ---
def main():
    if st:
        run_streamlit_app()
        return
    parser = argparse.ArgumentParser(description="Обработка названий автомобилей (ускорённая)")
    parser.add_argument("--input", "-i", help="Входной файл CSV/XLSX")
    parser.add_argument("--column", "-c", help="Имя столбца для обработки")
    parser.add_argument("--external", "-e", help="URL внешнего CSV/XLSX")
    parser.add_argument("--output", "-o", help="Путь для сохранения результата")
    parser.add_argument("--list", action="store_true", help="Вывести список ключей словаря")
    args = parser.parse_args()
    if args.list:
        print("Всего ключей в словаре:", len(car_brands_models))
        for k in sorted(car_brands_models.keys()):
            print(k, "->", car_brands_models[k])
        return
    if not args.input or not args.column:
        print("Укажите --input и --column, или используйте --list для просмотра словаря.")
        print("Пример: python script.py --input data.csv --column description --output result.csv")
        return
    process_file_cli(args.input, args.column, args.external, args.output)

if __name__ == "__main__":
    main()
