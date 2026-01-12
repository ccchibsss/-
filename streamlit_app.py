import io
import os
import re
import json
import numpy as np
from functools import lru_cache
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

try:
    import streamlit as st
except ImportError:
    st = None

try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except ImportError:
    morph = None

# Ваш словарь брендов и моделей
car_brands_models: dict = {
    "BMW": "БМВ",
    "1 Series": "1 Серия",
    "2 Series": "2 Серия",
    "3 Series": "3 Серия",
    "4 Series": "4 Серия",
    "5 Series": "5 Серия",
    "6 Series": "6 Серия",
    "7 Series": "7 Серия",
    "8 Series": "8 Серия",
    "X1": "Икс 1",
    "X2": "Икс 2",
    "X3": "Икс 3",
    "X4": "Икс 4",
    "X5": "Икс 5",
    "X6": "Икс 6",
    "X7": "Икс 7",
    "Z4": "Зет 4",
    "M3": "Эм 3",
    "M5": "Эм 5",
    "M Series": "Эм Серия",
    "Mercedes-Benz": "Мерседес-Бенц",
    "Mercedes": "Мерседес",
}

ADDITIONS_FILE = "additional_brands.json"

def load_user_additions(filepath=ADDITIONS_FILE) -> dict:
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
        except Exception as e:
            logging.warning(f"Ошибка загрузки пользовательских данных: {e}")
    return {}

def save_user_additions(data: dict, filepath=ADDITIONS_FILE) -> None:
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Ошибка сохранения пользовательских данных: {e}")

def load_brands_from_file(filepath: str) -> dict:
    try:
        if filepath.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        elif filepath.lower().endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            print("Некupported формат файла для словаря.")
            return {}
        result = {}
        for _, row in df.iterrows():
            if len(row) >= 2:
                key = str(row[0]).strip()
                val = str(row[1]).strip()
                if key and val:
                    result[key] = val
        return result
    except Exception as e:
        print(f"Ошибка при загрузке файла словаря: {e}")
        return {}

added_pairs: dict = {}

@lru_cache(maxsize=10000)
def decline_word_cached(word: str) -> str:
    if not word or not morph:
        return word
    try:
        p = morph.parse(word)[0]
        inflected = p.inflect({"nomn"})
        return inflected.word if inflected else p.word
    except Exception:
        return word

# Транслитерация латиницы в кириллицу
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

def levenshtein_distance(s1: str, s2: str) -> int:
    if s1 == s2:
        return 0
    len_s1, len_s2 = len(s1), len(s2)
    if len_s1 == 0:
        return len_s2
    if len_s2 == 0:
        return len_s1

    matrix = np.zeros((len_s1 + 1, len_s2 + 1), dtype=int)
    for i in range(len_s1 + 1):
        matrix[i][0] = i
    for j in range(len_s2 + 1):
        matrix[0][j] = j

    for i in range(1, len_s1 + 1):
        for j in range(1, len_s2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            matrix[i][j] = min(
                matrix[i - 1][j] + 1,
                matrix[i][j - 1] + 1,
                matrix[i - 1][j - 1] + cost
            )
    return matrix[len_s1][len_s2]

def similarity_ratio(s1: str, s2: str) -> float:
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    dist = levenshtein_distance(s1, s2)
    return 1 - dist / max_len

def build_final_struct(base_map: dict, additions: dict = None) -> dict:
    final_map = {**base_map, **(additions or {})}
    if not final_map:
        return {"pattern": None, "map": {}, "len_max": 0}

    keys_sorted = sorted(final_map.keys(), key=len, reverse=True)
    escaped = [re.escape(k) for k in keys_sorted if k.strip()]
    pattern = re.compile(r'(?<!\w)(?:' + "|".join(escaped) + r')(?!\w)', flags=re.IGNORECASE)

    mapping: dict = {}
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

def process_texts_parallel(texts: list, final_struct: dict, translit_allowed: bool=True) -> list:
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(process_text_fast_core, text, final_struct, translit_allowed)
            for text in texts
        ]
        return [f.result() for f in futures]

# Веб-интерфейс
def run_streamlit_app():
    if st is None:
        print("Streamlit не установлен.")
        return

    st.set_page_config(page_title="Автообработка", layout="wide")
    st.title("Распознавание брендов/моделей — ускорённый модуль")
    st.markdown("Загрузите CSV/XLSX, выберите столбец — скрипт автоматически подсветит и добавит переводы.")

    uploaded_dict_file = st.sidebar.file_uploader(
        "Загрузить пользовательский словарь (XLSX или CSV)", type=["xlsx", "xls", "csv"]
    )
    user_dict = {}
    if uploaded_dict_file:
        try:
            if uploaded_dict_file.name.lower().endswith(('.xls', '.xlsx')):
                df_dict = pd.read_excel(uploaded_dict_file)
            elif uploaded_dict_file.name.lower().endswith('.csv'):
                df_dict = pd.read_csv(uploaded_dict_file)
            else:
                st.sidebar.error("Неподдерживаемый формат файла.")
                df_dict = pd.DataFrame()
            for _, row in df_dict.iterrows():
                if len(row) >= 2:
                    key = str(row[0]).strip()
                    val = str(row[1]).strip()
                    if key and val:
                        user_dict[key] = val
            if user_dict:
                st.sidebar.success(f"Загружено {len(user_dict)} пар из пользовательского файла.")
        except Exception as e:
            st.sidebar.error(f"Ошибка при чтении файла: {e}")

    # Обновляем основной словарь
    global added_pairs
    added_pairs = load_user_additions()
    if user_dict:
        added_pairs.update(user_dict)
        save_user_additions(added_pairs)

    current_dict = {**car_brands_models, **added_pairs}

    threshold = st.sidebar.slider("Порог похожести для автодобавления", 0.6, 0.99, 0.8, 0.01)
    translit_allowed = st.sidebar.checkbox("Автотранслитерация", value=True)

    uploaded = st.file_uploader("Загрузите CSV/XLSX", type=["csv", "xls", "xlsx"])
    external_url = st.text_input("URL внешнего источника (CSV/XLSX) — необязательно")
    if not uploaded:
        st.info("Загрузите файл выше.")
        return

    try:
        if uploaded.name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(uploaded)
        else:
            df = pd.read_csv(uploaded)
    except Exception as e:
        st.error(f"Не удалось прочитать файл: {e}")
        return

    col_name = st.selectbox("Столбец для обработки", df.columns.tolist())

    if st.button("Обработать"):
        final_struct = build_final_struct(current_dict, {})
        series = df[col_name]
        dataset_words = set()
        external_words = set()

        for val in series.astype(str):
            dataset_words.update(re.findall(r'\b\w+\b', val.lower()))
        if external_url:
            try:
                ext_df = pd.read_csv(external_url)
                for val in ext_df.stack().astype(str):
                    external_words.update(re.findall(r'\b\w+\b', val.lower()))
            except:
                pass

        base_keys = set(current_dict.keys())
        candidates = (dataset_words | external_words) - base_keys

        additions = prepare_additions_fast(base_keys, candidates, threshold=threshold)
        if additions:
            current_dict.update(additions)
            try:
                existing_additions = load_user_additions()
                existing_additions.update(additions)
                save_user_additions(existing_additions)
            except:
                pass

        final_struct = build_final_struct(current_dict, additions)
        texts = series.astype(str).tolist()
        processed_texts = process_texts_parallel(texts, final_struct, translit_allowed)

        col_idx = df.columns.get_loc(col_name)
        new_col_name = col_name + " (переведённый)"
        df.insert(col_idx + 1, new_col_name, "")
        for i, orig in enumerate(texts):
            df.iat[i, col_idx + 1] = f"{orig} ({processed_texts[i]})"

        export_format = st.radio("Экспортировать как", ("CSV", "Excel"))
        buf = io.BytesIO()
        if export_format == "Excel":
            df.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button("Скачать Excel", buf, file_name="result.xlsx")
        else:
            df.to_csv(buf, index=False)
            buf.seek(0)
            st.download_button("Скачать CSV", buf, file_name="result.csv", mime="text/csv")

# CLI
def process_file_cli(input_path: str, column: str, external_url: str, output_path: str):
    try:
        if input_path.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(input_path)
        else:
            df = pd.read_csv(input_path)
    except Exception as e:
        print(f"Ошибка чтения файла: {e}")
        return

    if column not in df.columns:
        print(f"Столбец '{column}' не найден.")
        return

    external_words = set()
    if external_url:
        try:
            ext_df = pd.read_csv(external_url)
            for val in ext_df.stack().astype(str):
                external_words.update(re.findall(r'\b\w+\b', val.lower()))
        except:
            pass

    dataset_words = set()
    for val in df[column].astype(str):
        dataset_words.update(re.findall(r'\b\w+\b', val.lower()))

    base_keys = set(car_brands_models.keys())
    candidates = (dataset_words | external_words) - base_keys

    additions = prepare_additions_fast(base_keys, candidates, threshold=0.85)
    if additions:
        car_brands_models.update(additions)

    final_struct = build_final_struct(car_brands_models, additions)

    def process_text(text: str):
        return process_text_fast_core(text, final_struct, translit_allowed=True)

    df[column] = df[column].astype(str).apply(process_text)

    try:
        if output_path.lower().endswith(('.xls', '.xlsx')):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)
        print(f"Результат сохранен в {output_path}")
    except Exception as e:
        print(f"Ошибка сохранения файла: {e}")

# CLI запуск
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Обработка названий автомобилей")
    parser.add_argument("--input", "-i", help="Путь к файлу входных данных")
    parser.add_argument("--column", "-c", help="Название столбца для обработки")
    parser.add_argument("--external", "-e", help="URL внешних данных")
    parser.add_argument("--output", "-o", help="Путь для результата")
    parser.add_argument("--list", action="store_true", help="Вывести список ключей словаря")
    parser.add_argument("--dict", help="Путь к пользовательскому словарю (XLSX или CSV)")
    args = parser.parse_args()

    global added_pairs
    if args.dict:
        custom_dict = load_brands_from_file(args.dict)
        added_pairs.update(custom_dict)
        save_user_additions(added_pairs)

    if args.list:
        print("Всего ключей:", len(car_brands_models))
        for k in sorted(car_brands_models):
            print(k, "→", car_brands_models[k])
        return

    if not args.input or not args.column:
        print("Укажите --input и --column или --list")
        return

    output_path = args.output or ("result.xlsx" if args.input.lower().endswith(('.xls', '.xlsx')) else "result.csv")
    process_file_cli(args.input, args.column, args.external or "", output_path)

if __name__ == "__main__":
    if st:
        run_streamlit_app()
    else:
        main()
