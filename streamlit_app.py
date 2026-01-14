# !/usr/bin/env python3
# Упрощенная версия: только загрузка словаря и файла, выбор колонки, скачивание результатов

from __future__ import annotations
import io
import os
import re
import json
import requests
import pandas as pd
from functools import lru_cache

try:
    import streamlit as st
except Exception:
    st = None

try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

CSV_ENCODING = "utf-8-sig"
ADDITIONS_FILE = "additional_brands.json"

# Изначальный словарь
car_brands_models: Dict[str, str] = {
    # ... ваш словарь ...
    # (оставьте или вставьте сюда весь ваш словарь)
}

# Загрузка дополнений из файла
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
        if fileobj is not None:
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

# Обновляем структуру поиска
search_struct = None

def update_search_struct():
    global search_struct
    search_struct = build_final_struct_fast(car_brands_models, {})

# Построение структуры поиска
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
    # Тут можно оставить только trie или aho, для простоты оставим trie
    trie = {}
    END = "_end_"
    for lk, pair in mapping.items():
        node = trie
        for ch in lk:
            node = node.setdefault(ch, {})
        node[END] = (lk, pair)
    return {"engine": trie, "map": mapping, "max_len": max_len, "use_aho": False}

# Обработка файла
def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str, struct: Dict[str, Any]) -> pd.DataFrame:
    # читаем файл
    if filename.lower().endswith(('.xls', '.xlsx')):
        df = pd.read_excel(io.BytesIO(file_bytes))
    else:
        df = pd.read_csv(io.BytesIO(file_bytes))
    # обработка
    def process_value(v: str) -> str:
        return process_text_fast_optimized(v, struct, translit_allowed=True)
    df["Обработанное"] = df[col_name].astype(str).apply(process_value)
    return df

# UI через streamlit
def run_streamlit_app():
    if st is None:
        return
    st.set_page_config(page_title="Загрузка и обработка", layout="wide")
    st.title("Загрузка словаря и файла для обработки")

    # Загрузка словаря
    dict_file = st.file_uploader("Загрузить словарь (json/csv/xlsx)", type=["json", "csv", "xls", "xlsx"])
    dict_url = st.text_input("или ввести URL словаря")
    if st.button("Загрузить словарь"):
        loaded = {}
        if dict_file:
            try:
                loaded = load_dictionary(source=dict_file.name, fileobj=dict_file)
            except Exception as e:
                st.error(f"Ошибка при загрузке файла: {e}")
        elif dict_url:
            try:
                loaded = load_dictionary(source=dict_url)
            except Exception as e:
                st.error(f"Ошибка при загрузке по URL: {e}")
        if loaded:
            for k, v in loaded.items():
                car_brands_models[k] = v
            # сохраняем дополнения
            try:
                with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
                    json.dump({str(k): str(v) for k, v in {**car_brands_models}.items()}, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
            update_search_struct()
            st.success(f"Загружено {len(loaded)} пар слов")
        else:
            st.info("Словарь не загружен или пуст")

    # Загрузка файла для обработки
    uploaded_file = st.file_uploader("Выберите CSV/XLSX файл для обработки", type=["csv", "xls", "xlsx"])
    col_name = st.text_input("Имя столбца для обработки")
    if uploaded_file and col_name:
        # Обработка файла
        try:
            file_bytes = uploaded_file.read()
            df = process_file_for_processing(file_bytes, uploaded_file.name, col_name, search_struct if search_struct else build_final_struct_fast(car_brands_models, {}))
            st.dataframe(df.head(10))
            # Предложение скачать результат
            buf = io.BytesIO()
            if uploaded_file.name.lower().endswith('.xlsx'):
                df.to_excel(buf, index=False)
                buf.seek(0)
                st.download_button("Скачать обработанный Excel", buf, file_name="processed.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                df.to_csv(buf, index=False, encoding=CSV_ENCODING)
                buf.seek(0)
                st.download_button("Скачать обработанный CSV", buf, file_name="processed.csv", mime="text/csv")
        except Exception as e:
            st.error(f"Ошибка обработки файла: {e}")

if __name__ == "__main__":
    run_streamlit_app()
