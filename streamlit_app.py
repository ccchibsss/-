# !/usr/bin/env python3
import io
import os
import json
import requests
import pandas as pd
from functools import lru_cache
from typing import Dict, Tuple, Any, List, Optional

import streamlit as st

try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except ImportError:
    morph = None

CSV_ENCODING = "utf-8-sig"
ADDITIONS_FILE = "additional_brands.json"

# Изначальный встроенный словарь
car_brands_models: Dict[str, str] = {
    "Toyota": "Toyota",
    "Honda": "Honda",
    "BMW": "BMW",
    "Audi": "Audi",
    "Kia": "Kia",
}

# Загрузка дополнений
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
    if not word or morph is None:
        return word
    try:
        p = morph.parse(word)[0]
        inf = p.inflect({"nomn"})
        return inf.word if inf else p.word
    except Exception:
        return word

def load_dictionary(source: Optional[str]=None, fileobj: Optional[io.BytesIO]=None) -> Dict[str, str]:
    result: Dict[str, str] = {}
    try:
        if fileobj:
            if source and source.lower().endswith('.json'):
                content = fileobj.getvalue().decode('utf-8')
                data = json.loads(content)
                if isinstance(data, dict):
                    result = data
            elif source and source.lower().endswith(('.csv', '.xls', '.xlsx')):
                if source.lower().endswith('.csv'):
                    df = pd.read_csv(fileobj)
                else:
                    df = pd.read_excel(fileobj)
                if len(df.columns) >= 2:
                    result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        elif source:
            if source.startswith('http'):
                resp = requests.get(source, timeout=10)
                if source.lower().endswith('.json'):
                    data = resp.json()
                    if isinstance(data, dict):
                        result = data
                else:
                    if source.lower().endswith('.csv'):
                        df = pd.read_csv(io.StringIO(resp.text))
                    else:
                        df = pd.read_excel(io.BytesIO(resp.content))
                    if len(df.columns) >= 2:
                        result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
            else:
                if source.lower().endswith('.json'):
                    with open(source, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, dict):
                            result = data
                elif source.lower().endswith('.csv'):
                    df = pd.read_csv(source)
                    if len(df.columns) >= 2:
                        result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
                else:
                    df = pd.read_excel(source)
                    if len(df.columns) >= 2:
                        result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        return {str(k): str(v) for k, v in result.items()}
    except Exception:
        return {}

search_struct = None

def update_search_struct(base_dict=None):
    global search_struct
    if base_dict is None:
        base_dict = car_brands_models
    search_struct = build_final_struct_fast(base_dict, {})

def build_final_struct_fast(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    final_map = {**base_map}
    if additions:
        final_map.update(additions)
    final_map = {k: v for k, v in final_map.items() if isinstance(k, str) and k.strip()}
    if not final_map:
        return {"engine": None, "map": {}, "max_len": 0, "use_aho": False}
    mapping: Dict[str, Tuple[str, str]] = {}
    max_len = 0
    for k, v in final_map.items():
        lk = k.lower()
        ru = v if v is not None else k
        ru_decl = decline_word_cached(str(ru))
        mapping[lk] = (k, ru_decl)
        if len(lk) > max_len:
            max_len = len(lk)
    trie = {}
    END = "_end_"
    for lk, pair in mapping.items():
        node = trie
        for ch in lk:
            node = node.setdefault(ch, {})
        node[END] = (lk, pair)
    return {"engine": trie, "map": mapping, "max_len": max_len, "use_aho": False}

def get_matches(text: str, struct: Dict[str, Any]) -> List[Tuple[int, int, str, str]]:
    if not text or not struct or struct["engine"] is None:
        return []
    return _find_matches_trie(text, struct)

def _find_matches_trie(text: str, struct: Dict[str, Any]) -> List[Tuple[int, int, str, str]]:
    trie = struct["engine"]
    text_l = text.lower()
    n = len(text_l)
    max_len = struct["max_len"]
    matches: List[Tuple[int, int, str, str]] = []
    END = "_end_"
    for i in range(n):
        node = trie
        j = i
        while j < n and (j - i) < max_len and text_l[j] in node:
            node = node[text_l[j]]
            if END in node:
                lk, (orig_key, ru_decl) = node[END]
                start_idx = i
                end_idx = j
                # Проверка границ слова
                if start_idx > 0 and _is_word_char(text_l[start_idx - 1]):
                    pass
                elif end_idx + 1 < n and _is_word_char(text[start_idx - 1]):
                    pass
                else:
                    matches.append((start_idx, end_idx, orig_key, ru_decl))
            j += 1
    if not matches:
        return []
    matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    filtered = []
    last_end = -1
    for s, e, ok, ru in matches:
        if s > last_end:
            filtered.append((s, e, ok, ru))
            last_end = e
    return filtered

def _is_word_char(c):
    return c.isalnum() or c == "_"

def contains_latin(text: str) -> bool:
    return any('a' <= ch.lower() <= 'z' for ch in text)

def contains_cyrillic(text: str) -> bool:
    return any('а' <= ch.lower() <= 'я' or 'ё' <= ch.lower() <= 'ё' for ch in text)

def latin_to_cyrillic(text: str) -> str:
    # Можно реализовать транслитерацию, если нужно
    return text

def process_text_fast_optimized(text: str, struct: Dict[str, Any]) -> str:
    if not text or not struct or struct["engine"] is None:
        return text
    matches = get_matches(text, struct)
    if not matches:
        return text
    result = []
    last_idx = 0
    for s, e, ok, ru in matches:
        if s > last_idx:
            result.append(text[last_idx:s])
        matched_text = text[s:e+1]
        result.append(f"{matched_text}")  # Можно изменить оформление
        last_idx = e + 1
    if last_idx < len(text):
        result.append(text[last_idx:])
    return "".join(result)

def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str, struct: Dict[str, Any]) -> pd.DataFrame:
    file_stream = io.BytesIO(file_bytes)
    if filename.lower().endswith('.xlsx'):
        df = pd.read_excel(file_stream)
    else:
        df = pd.read_csv(io.BytesIO(file_bytes), encoding=CSV_ENCODING)
    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден в файле")
    df[col_name] = df[col_name].astype(str).apply(lambda x: process_text_fast_optimized(x, struct))
    return df

# Основная функция с улучшенным оформлением
def run():
    st.set_page_config(page_title="Обработка данных", layout="wide")
    st.title("🚗 Обработка данных и расширение словаря")
    st.markdown("---")
    
    # Инициализация базового словаря
    global base_dict
    base_dict = car_brands_models.copy()

    # Раздел: Настройки словаря
    with st.expander("🛠️ Настройки словаря", expanded=True):
        col1, col2 = st.columns([2, 1])
        with col1:
            dict_file = st.file_uploader("📁 Загрузить файл словаря", type=["json", "csv", "xls", "xlsx"])
            dict_url = st.text_input("🌐 Или ввести URL файла словаря")
            if st.button("🔄 Обновить словарь"):
                with st.spinner("Загружаем словарь..."):
                    loaded = {}
                    if dict_file:
                        try:
                            loaded = load_dictionary(source=dict_file.name, fileobj=dict_file)
                        except:
                            st.error("Ошибка при загрузке файла")
                    elif dict_url:
                        try:
                            loaded = load_dictionary(source=dict_url)
                        except:
                            st.error("Ошибка при загрузке по URL")
                    if loaded:
                        base_dict.update(loaded)
                        with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
                            json.dump({str(k): str(v) for k, v in base_dict.items()}, f, ensure_ascii=False, indent=2)
                        update_search_struct(base_dict)
                        st.success(f"Обновлено {len(loaded)} пар слов")
        with col2:
            st.markdown("#### Текущий словарь")
            df_dict = pd.DataFrame(list(base_dict.items()), columns=["Ключ", "Значение"])
            st.dataframe(df_dict, height=200)
            st.markdown("#### Добавить пару")
            new_key = st.text_input("🔑 Новый ключ")
            new_value = st.text_input("📝 Новое значение")
            if st.button("➕ Добавить пару"):
                if new_key and new_value:
                    base_dict[new_key] = new_value
                    with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
                        json.dump({str(k): str(v) for k, v in base_dict.items()}, f, ensure_ascii=False, indent=2)
                    update_search_struct(base_dict)
                    st.success(f"Пара '{new_key}':'{new_value}' добавлена")
                else:
                    st.warning("Заполните оба поля")
    
    st.markdown("---")
    # Раздел: Обработка файла
    with st.expander("📝 Настройки обработки файла", expanded=True):
        uploaded_file = st.file_uploader("📂 Выберите файл для обработки", type=["csv", "xls", "xlsx"])
        col_name = st.text_input("🔤 Имя столбца для обработки")
        if uploaded_file and not col_name:
            try:
                if uploaded_file.name.lower().endswith('.xlsx'):
                    df_preview = pd.read_excel(uploaded_file, nrows=0)
                else:
                    df_preview = pd.read_csv(uploaded_file, nrows=0)
                if hasattr(df_preview, 'columns'):
                    col_name = st.selectbox("📑 Выберите столбец для обработки", df_preview.columns.tolist())
            except:
                pass
        if uploaded_file and col_name:
            try:
                with st.spinner("Обрабатываем файл..."):
                    file_bytes = uploaded_file.read()
                    struct = search_struct if search_struct else build_final_struct_fast(base_dict, {})
                    df_result = process_file_for_processing(file_bytes, uploaded_file.name, col_name, struct)
                st.success("✅ Обработка завершена")
                st.markdown("### Результат")
                st.dataframe(df_result.head(20))
                # Кнопки скачивания
                buf_xlsx = io.BytesIO()
                buf_csv = io.BytesIO()
                if uploaded_file.name.lower().endswith('.xlsx'):
                    df_result.to_excel(buf_xlsx, index=False)
                    buf_xlsx.seek(0)
                    st.download_button("⬇️ Скачать XLSX", buf_xlsx, file_name="result.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                else:
                    df_result.to_csv(buf_csv, index=False, encoding=CSV_ENCODING)
                    buf_csv.seek(0)
                    st.download_button("⬇️ Скачать CSV", buf_csv, file_name="result.csv", mime="text/csv")
            except Exception as e:
                st.error(f"Ошибка: {e}")

if __name__ == "__main__":
    run()
