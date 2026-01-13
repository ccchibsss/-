#!/usr/bin/env python3
# Полный исправленный код с улучшенной функцией prepare_additions_fast

from __future__ import annotations
import io
import os
import re
import json
import requests
import pandas as pd
from difflib import SequenceMatcher
from functools import lru_cache
from collections import Counter
from typing import Optional, Dict, Set, List, Any

try:
    import streamlit as st  # type: ignore
except Exception:
    st = None

try:
    import pymorphy2  # type: ignore
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

CSV_ENCODING = "utf-8-sig"
ADDITIONS_FILE = "additional_brands.json"

# Базовые данные
car_brands_models: Dict[str, str] = {
    "BMW": "БМВ",
    "1 Series": "1 Серия",
    "2 Series": "2 Серия",
    "3 Series": "3 Серия",
    # добавьте свои данные
}

# Объявляем глобальную переменную для новых пар
added_pairs: Dict[str, str] = {}

# Загрузка пользовательских добавлений
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                car_brands_models.update({str(k): str(v) for k, v in loaded.items()})
    except Exception:
        pass

@lru_cache(maxsize=10000)
def decline_word_cached(word: str) -> str:
    if not word or not morph:
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
        out_str = "".join(out)
        if word.isupper():
            return out_str.upper()
        if word[0].isupper():
            return out_str.capitalize()
        return out_str
    parts = re.split(r'(\s+)', text)
    res = []
    for p in parts:
        if re.search(r'[A-Za-z]', p):
            sub_parts = re.split(r'([^A-Za-z]+)', p)
            for s in sub_parts:
                if re.search(r'[A-Za-z]', s):
                    res.append(translit_word(s))
                else:
                    res.append(s)
        else:
            res.append(p)
    return "".join(res)

def contains_latin(text: str) -> bool:
    return bool(re.search(r'[A-Za-z]', str(text)))

def contains_cyrillic(text: str) -> bool:
    return bool(re.search(r'[\u0400-\u04FF]', str(text)))

def build_final_struct(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict:
    final_map = {**base_map}
    if additions:
        final_map.update(additions)
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

def format_custom(text: str, final_struct: Dict) -> str:
    if not isinstance(text, str) or not final_struct:
        return text
    years_match = re.search(r'(\d{4}\s*[~\-\–]\s*\d{4})', text)
    years = years_match.group(1) if years_match else ""
    text_no_years = text.replace(years, "").strip()
    pattern_pairs = re.findall(r'\([^\)]+\)|[A-Za-z0-9\-\.]+|[А-Яа-я0-9\-\.]+', text_no_years)
    brand = ""
    model = ""
    if pattern_pairs:
        brand_candidate = pattern_pairs[0]
        model_candidate = pattern_pairs[1] if len(pattern_pairs) > 1 else ""
        brand = re.sub(r'^[\(\s]+|[\)\s]+$', '', brand_candidate)
        model = re.sub(r'^[\(\s]+|[\)\s]+$', '', model_candidate)
    mp = final_struct.get("map", {})
    ru_brand = ""
    ru_model = ""
    if brand:
        val = mp.get(brand.lower())
        if val:
            ru_brand = val[1] if isinstance(val, (list, tuple)) and len(val) > 1 else ""
    if model:
        val = mp.get(model.lower())
        if val:
            ru_model = val[1] if isinstance(val, (list, tuple)) and len(val) > 1 else ""
    parts = []
    if brand:
        parts.append(brand)
    if model:
        parts.append(model)
    main = " ".join(parts).strip() or text.strip()
    extras = []
    if ru_brand:
        extras.append(ru_brand)
    if ru_model:
        extras.append(ru_model)
    if years:
        main = f"{main} {years}" if main else years
        extras.append(years)
    if extras:
        return f"{main} ({' '.join(extras).strip()})"
    return main

def translate_full_string(text: str, final_struct: Dict) -> str:
    parts = [part.strip() for part in str(text).split('/')]
    translated_parts = [format_custom(part, final_struct) for part in parts]
    return " / ".join(translated_parts)

def process_text_fast(text: str, final_struct: Dict, translit_allowed: bool = True) -> str:
    if not isinstance(text, str) or not final_struct:
        return text
    pattern_delim = re.compile(r'([/;])')
    if any(d in text for d in ['/', ';']):
        parts = pattern_delim.split(text)
        result_parts = []
        for part in parts:
            if part in ['/', ';']:
                result_parts.append(part)
            else:
                key_lower = part.lower()
                if key_lower in final_struct["map"]:
                    val = final_struct["map"][key_lower][1]
                    result_parts.append(val)
                else:
                    if translit_allowed and contains_latin(part) and not contains_cyrillic(part):
                        cyr = latin_to_cyrillic(part)
                        result_parts.append(f"{part} ({decline_word_cached(cyr)})")
                    else:
                        result_parts.append(part)
        return "".join(result_parts)
    else:
        key_lower = text.lower()
        if key_lower in final_struct["map"]:
            return final_struct["map"][key_lower][1]
        else:
            if translit_allowed and contains_latin(text) and not contains_cyrillic(text):
                cyr = latin_to_cyrillic(text)
                return f"{text} ({decline_word_cached(cyr)})"
            else:
                return format_custom(text, final_struct)

def load_external_data(url: str) -> pd.DataFrame:
    if not url:
        return pd.DataFrame()
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "").lower()
        if "text/csv" in ct or url.lower().endswith(".csv"):
            txt = resp.content.decode(CSV_ENCODING, errors="ignore")
            return pd.read_csv(io.StringIO(txt))
        try:
            return pd.read_excel(io.BytesIO(resp.content))
        except Exception:
            return pd.read_csv(io.StringIO(resp.content.decode(CSV_ENCODING, errors="ignore")))
    except Exception:
        return pd.DataFrame()

def extract_words_from_series(series: pd.Series) -> Set[str]:
    if series is None:
        return set()
    all_text = series.dropna().astype(str).str.cat(sep=' ')
    return set(re.findall(r'[A-Za-zА-Яа-я0-9\-_/\.]+', all_text))

def save_additions():
    try:
        with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): str(v) for k, v in {**car_brands_models, **added_pairs}.items()}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# Новая функция: сравнивает по сочетаниям латиницы и буквам
def prepare_additions_fast(base_keys: set, candidates: set, threshold: float = 0.85) -> Dict[str, str]:
    additions = {}
    for candidate in candidates:
        for base in base_keys:
            similarity_full = SequenceMatcher(None, candidate, base).ratio()
            if similarity_full >= threshold:
                additions[candidate] = base
                break
            else:
                candidate_letters = sorted(candidate.lower())
                base_letters = sorted(base.lower())
                similarity_letters = SequenceMatcher(None, "".join(candidate_letters), "".join(base_letters)).ratio()
                if similarity_letters >= threshold:
                    additions[candidate] = base
                    break
    return additions

# --- UI и CLI ---

def run_streamlit_app() -> None:
    if st is None:
        return
    st.set_page_config(page_title="Автообработка", layout="wide")
    st.title("Распознавание брендов/моделей — улучшенная визуализация")
    st.markdown("Загрузите CSV/XLSX, выберите столбец — скрипт автоматически подсветит совпадения.")

    sidebar = st.sidebar
    threshold = sidebar.slider("Порог для автодобавления", 0.6, 0.99, 0.85, 0.01)
    translit_allowed = sidebar.checkbox("Автотранслитерация (латиница → кириллица)", value=True)

    sidebar.header("Загрузить словарь (опционально)")
    dict_file = sidebar.file_uploader("Файл словаря (json/csv/xlsx)", type=["json", "csv", "xls", "xlsx"])
    dict_url = sidebar.text_input("URL словаря (json/csv/xlsx) — опционально")
    if sidebar.button("Загрузить"):
        loaded = {}
        if dict_file is not None:
            try:
                loaded = load_dictionary(fileobj=dict_file)
            except Exception as e:
                st.error(f"Не удалось загрузить файл словаря: {e}")
        elif dict_url:
            loaded = load_dictionary(source=dict_url)
        if loaded:
            for k, v in loaded.items():
                car_brands_models[k] = v
            added_pairs.update(loaded)
            save_additions()
            st.success(f"Загружено {len(loaded)} пар из словаря")
        else:
            st.info("Словарь не загружен или пустой")

    sidebar.header("Добавить пару вручную")
    new_k = sidebar.text_input("Ключ (англ)")
    new_v = sidebar.text_input("Русское название")
    if sidebar.button("Добавить"):
        if new_k and new_v:
            car_brands_models[new_k] = new_v
            added_pairs[new_k] = new_v
            save_additions()
            st.success(f"Добавлено: {new_k} → {new_v}")
        else:
            st.error("Поля обязательны")

    uploaded = st.file_uploader("Выберите CSV/XLSX", type=["csv", "xls", "xlsx"])
    external_url = sidebar.text_input("URL внешних данных (CSV/XLSX) — необязательно")
    if not uploaded:
        st.info("Загрузите файл выше.")
        return

    try:
        if uploaded.name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(uploaded)
        else:
            try:
                df = pd.read_csv(uploaded, encoding=CSV_ENCODING)
            except Exception:
                raw = uploaded.getvalue()
                txt = raw.decode(CSV_ENCODING, errors="ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
                df = pd.read_csv(io.StringIO(txt))
    except Exception as e:
        st.error(f"Ошибка чтения файла: {e}")
        return

    st.success(f"Файл: {uploaded.name} ({df.shape[0]} строк, {df.shape[1]} колонок)")
    st.dataframe(df.head(5))
    col = st.selectbox("Выберите столбец для обработки", df.columns.tolist())

    if st.button("Обработать"):
        ext_df = load_external_data(external_url) if external_url else pd.DataFrame()

        series = df[col]
        dataset_words = extract_words_from_series(series)
        external_words = extract_words_from_series(ext_df.stack()) if not ext_df.empty else set()
        base_keys = set(car_brands_models.keys())
        candidates = (dataset_words | external_words) - base_keys

        # Автоматическое добавление с новой логикой
        additions = prepare_additions_fast(base_keys, candidates, threshold=threshold)
        if additions:
            for k, v in additions.items():
                car_brands_models[k] = v
            added_pairs.update(additions)
            save_additions()

        final_struct = build_final_struct(car_brands_models, additions)
        pattern = final_struct.get("pattern")
        mapping = final_struct.get("map", {})

        # Создаем новые колонки: исходное и обработанное
        df["Исходное"] = df[col]
        df["Обработанное"] = df[col].astype(str).apply(lambda v: process_text_fast(v, final_struct, translit_allowed=translit_allowed))

        # Создаем HTML таблицу с подсветкой исходных данных
        def create_html_table(df: pd.DataFrame, pattern: re.Pattern, mapping: Dict) -> str:
            html_rows = ""
            for idx in df.index[:200]:
                original = df.at[idx, "Исходное"]
                def replacer(match):
                    f = match.group(0)
                    info = mapping.get(f.lower())
                    if info:
                        return f"<mark style='background:#fffd8a'>{f} ({info[1]})</mark>"
                    return f
                highlighted_value = pattern.sub(replacer, str(original))
                if "<mark" in highlighted_value:
                    style = "background-color:#ffff99"
                    icon = "🔍"
                else:
                    style = ""
                    icon = "⚪"
                html_rows += (
                    f"<tr>"
                    f"<td style='padding:6px;border:1px solid #ddd; {style}' title='Исходное: {original}'><code>{original}</code></td>"
                    f"<td style='padding:6px;border:1px solid #ddd'>{icon} {highlighted_value}</td>"
                    f"</tr>"
                )
            html_table = (
                "<table style='width:100%;border-collapse:collapse'>"
                "<thead><tr><th>Исходное</th><th>Подсветка</th></tr></thead>"
                "<tbody>" + html_rows + "</tbody></table>"
            )
            return html_table

        html_table = create_html_table(df, pattern, mapping)
        st.markdown(html_table, unsafe_allow_html=True)

        # Таблица с двумя колонками: исходное и обработанное
        with st.expander("Полная таблица с исходным и обработанным"):
            display_df = df[["Исходное", "Обработанное"]]
            st.dataframe(display_df)

        # Экспорт
        export_format = st.radio("Формат экспорта", ("CSV", "Excel"))
        if export_format == "Excel":
            buf = io.BytesIO()
            df.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button("Скачать Excel", buf, file_name="result.xlsx")
        else:
            csv_bytes = df.to_csv(index=False, encoding=CSV_ENCODING).encode(CSV_ENCODING)
            st.download_button("Скачать CSV", csv_bytes, file_name="result.csv", mime="text/csv")


def main():
    if st:
        run_streamlit_app()
        return
    import argparse
    parser = argparse.ArgumentParser(description="Автообработка")
    parser.add_argument("--input", "-i", help="Входной файл CSV/XLSX")
    parser.add_argument("--column", "-c", help="Имя столбца")
    parser.add_argument("--external", "-e", help="URL внешних данных")
    parser.add_argument("--output", "-o", help="Путь для сохранения")
    parser.add_argument("--list", action="store_true", help="Показать список словаря")
    parser.add_argument("--dict", "-d", help="Файл или URL словаря")
    args = parser.parse_args()

    if args.list:
        print("Всего ключей в словаре:", len(car_brands_models))
        for k in sorted(car_brands_models):
            print(k, "→", car_brands_models[k])
        return
    if not args.input or not args.column:
        print("Укажите --input и --column, или --list")
        return

    # Обработка файла из командной строки
    try:
        if args.input.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(args.input)
        else:
            df = pd.read_csv(args.input, encoding=CSV_ENCODING)
    except Exception as e:
        print("Ошибка чтения файла:", e)
        return

    if args.column not in df.columns:
        print("Столбец не найден:", args.column)
        return

    # Загрузка словаря
    if args.dict:
        loaded = load_dictionary(source=args.dict)
        if loaded:
            for k, v in loaded.items():
                car_brands_models[k] = v
            added_pairs.update(loaded)
            save_additions()

    # Обработка внешних данных
    ext_df = load_external_data(args.external) if args.external else pd.DataFrame()

    # Подготовка
    series = df[args.column]
    dataset_words = extract_words_from_series(series)
    external_words = extract_words_from_series(ext_df.stack()) if not ext_df.empty else set()
    base_keys = set(car_brands_models.keys())
    candidates = (dataset_words | external_words) - base_keys

    # Автоматическое добавление с улучшенной логикой
    additions = prepare_additions_fast(base_keys, candidates, threshold=0.85)
    for k, v in additions.items():
        car_brands_models[k] = v
    added_pairs.update(additions)
    save_additions()

    # Создаем структуру
    final_struct = build_final_struct(car_brands_models, additions)
    pattern = final_struct.get("pattern")
    mapping = final_struct.get("map", {})

    # Обработка данных
    df["Исходное"] = df[args.column]
    df["Обработанное"] = df[args.column].astype(str).apply(lambda v: process_text_fast(v, final_struct, translit_allowed=True))

    # Экспорт
    output_path = args.output or ("result.xlsx" if args.input.lower().endswith(('.xls', '.xlsx')) else "result.csv")
    try:
        if output_path.lower().endswith(('.xls', '.xlsx')):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False, encoding=CSV_ENCODING)
        print("Результат сохранен:", output_path)
    except Exception as e:
        print("Ошибка сохранения:", e)


if __name__ == "__main__":
    main()
