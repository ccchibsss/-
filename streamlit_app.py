#!/usr/bin/env python3
import io
import os
import json
from functools import lru_cache
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st

# Необязательный морфологический анализатор
try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

# --- Словарь транслитерации из латиницы в кириллицу ---
lat2cyr_dict = {
    'A':'А','a':'а','B':'Б','b':'б','V':'В','v':'в','G':'Г','g':'г',
    'D':'Д','d':'д','E':'Е','e':'е','Yo':'Ё','yo':'ё','ZH':'Ж','zh':'ж',
    'Z':'З','z':'з','I':'И','i':'и','Y':'Й','y':'й','K':'К','k':'к',
    'L':'Л','l':'л','M':'М','m':'м','N':'Н','n':'н','O':'О','o':'о',
    'P':'П','p':'п','R':'Р','r':'р','S':'С','s':'с','T':'Т','t':'т',
    'U':'У','u':'у','F':'Ф','f':'ф','Kh':'Х','kh':'х','Ts':'Ц','ts':'ц',
    'Ch':'Ч','ch':'ч','Sh':'Ш','sh':'ш','Shch':'Щ','shch':'щ',
    'Y\'':'Ы','y\'':'ы','E\'':'Э','e\'':'э','Yu':'Ю','yu':'ю','Ya':'Я','ya':'я'
}

latin_to_cyr = dict(lat2cyr_dict)

def transliterate_latin_to_cyrillic(text: str) -> str:
    result = ''
    i = 0
    while i < len(text):
        match_found = False
        for length in [3,2,1]:
            if i + length <= len(text):
                chunk = text[i:i+length]
                if chunk in latin_to_cyr:
                    result += latin_to_cyr[chunk]
                    i += length
                    match_found = True
                    break
        if not match_found:
            result += text[i]
            i += 1
    return result

def transliterate_cyrillic_to_latin(text: str) -> str:
    cyr2lat = {v: k for k, v in latin_to_cyr.items()}
    result = ''
    for ch in text:
        result += cyr2lat.get(ch, ch)
    return result

def transliterate(text: str, direction: str = 'lat2cyr') -> str:
    if direction == 'lat2cyr':
        return transliterate_latin_to_cyrillic(text)
    elif direction == 'cyr2lat':
        return transliterate_cyrillic_to_latin(text)
    else:
        return text

CSV_ENCODING = "utf-8-sig"
ADDITIONS_FILE = "additional_brands.json"

# --- Изначальный словарь марок и моделей ---
car_brands_models: Dict[str, str] = {
    "Acura": "Акура",
    "Integra": "Интегра",
    "MDX": "МДХ",
    "RDX": "РДХ",
    "RSX": "РСХ",
    "TLX": "ТЛКС",
    # ... (добавьте остальные по необходимости)
}

# --- Загрузка существующего файла словаря при запуске ---
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            saved_dict = json.load(f)
            if isinstance(saved_dict, dict):
                car_brands_models.update({str(k): str(v) for k, v in saved_dict.items()})
    except Exception:
        pass

# --- Функция для сохранения словаря в файл ---
def save_dictionary_to_file(dictionary: Dict[str, str], filename: str = ADDITIONS_FILE):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(dictionary, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.exception(f"Ошибка при сохранении файла: {e}")

# --- Вспомогательная функция для обновления словаря из файла ---
def update_dict_from_uploaded_file(uploaded_file):
    try:
        dict_bytes = uploaded_file.read()
        filename_lower = uploaded_file.name.lower()
        loaded_dict = {}
        if filename_lower.endswith(".json"):
            obj = json.loads(dict_bytes.decode("utf-8"))
            if isinstance(obj, dict):
                loaded_dict = {str(k): str(v) for k, v in obj.items()}
        elif filename_lower.endswith(".csv"):
            df_dict = pd.read_csv(io.StringIO(dict_bytes.decode("utf-8")))
            if len(df_dict.columns) >= 2:
                loaded_dict = {str(k): str(v) for k, v in zip(df_dict.iloc[:,0], df_dict.iloc[:,1])}
        elif filename_lower.endswith(".xlsx"):
            df_dict = pd.read_excel(io.BytesIO(dict_bytes), engine='openpyxl')
            if len(df_dict.columns) >= 2:
                loaded_dict = {str(k): str(v) for k, v in zip(df_dict.iloc[:,0], df_dict.iloc[:,1])}
        else:
            st.error("Некорректный тип файла.")
            return
        # Обновляем основной словарь и сохраняем
        if loaded_dict:
            car_brands_models.update(loaded_dict)
            save_dictionary_to_file(car_brands_models)
            st.success("Словарь обновлён из файла и сохранён.")
    except Exception as e:
        st.error(f"Ошибка при загрузке файла словаря: {e}")

# --- Основная функция ---
def run():
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")

    # --- Раздел "Настройки словаря" ---
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px">
        <h2 style="color:white;margin:0">🛠️ Настройки словаря</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Редактирование словаря марок и моделей автомобилей</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Инструкция по формату файла
    st.info("Поддерживаются файлы: JSON, CSV, XLSX. В файле должны быть минимум 2 столбца: латиница и кириллица. Например:\n\n" +
            "- JSON: {\"Acura\": \"Акура\", \"BMW\": \"БМВ\"}\n" +
            "- CSV: латиница,кириллица\n" +
            "- XLSX: первый столбец - латиница, второй - кириллица")

    # --- Загрузка словаря из файла ---
    uploaded_dict_file = st.file_uploader("Загрузить файл словаря (JSON, CSV или XLSX)", type=["json", "csv", "xlsx"])
    if uploaded_dict_file:
        update_dict_from_uploaded_file(uploaded_dict_file)

    # --- Ручное редактирование словаря ---
    st.subheader("Редактировать словарь вручную")
    dict_text = "\n".join([f"{k},{v}" for k, v in car_brands_models.items()])
    edited_text = st.text_area("Редактировать словарь (каждая строка: латиница,кириллица)", value=dict_text, height=300)

    if st.button("Сохранить словарь"):
        new_dict = {}
        for line in edited_text.splitlines():
            if line.strip():
                parts = line.split(",", 1)
                if len(parts) == 2:
                    k, v = parts
                    new_dict[k.strip()] = v.strip()
        car_brands_models.clear()
        car_brands_models.update(new_dict)
        save_dictionary_to_file(car_brands_models)
        st.success("Словарь сохранён.")

    # --- Транслитерация ---
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#4CAF50,#81C784);padding:16px;border-radius:8px;margin-top:20px">
        <h2 style="color:white;margin:0">🌐 Транслитерация (латиница → кириллица)</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Введите латиницу для преобразования в кириллицу</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    latin_input = st.text_area("Введите текст на латинице", height=100)
    col_direction = st.radio("Направление транслитерации", ("Латиница → Кириллица", "Кириллица → Латиница"))
    if st.button("🔤 Транслитерировать"):
        if col_direction == "Латиница → Кириллица":
            result = transliterate(latin_input, 'lat2cyr')
            st.success("Результат транслитерации (латиница → кириллица):")
            st.code(result)
        else:
            result = transliterate(latin_input, 'cyr2lat')
            st.success("Результат транслитерации (кириллица → латиница):")
            st.code(result)

    # --- Обработка файла и поиск ---
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px;margin-top:20px">
        <h2 style="color:white;margin:0">🚗 Обработка данных</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Загрузка файла (Excel или CSV), поиск и подсветка</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    uploaded_file = st.file_uploader("Загрузите Excel или CSV файл", type=["xlsx", "xls", "csv"])

    if uploaded_file:
        try:
            file_bytes = uploaded_file.read()
            ext = os.path.splitext(uploaded_file.name)[1].lower()

            # Предварительный просмотр колонок
            if ext in ['.xlsx', '.xls']:
                df_preview = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl', nrows=0)
            elif ext == '.csv':
                df_preview = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')), nrows=0)
            else:
                df_preview = pd.DataFrame()

            # --------- ВАЖНО: Выбор из всех столбцов файла через всплывающее меню ---------
            if not df_preview.empty:
                col_options = list(df_preview.columns)
                # Пользователь выбирает колонку из всех доступных
                col_name = st.selectbox("Выберите название столбца для обработки", options=col_options)
            else:
                col_name = st.text_input("Введите название столбца для обработки", value="Название")
            
            if col_name:
                # Обработка файла полностью
                df = process_file_for_processing(file_bytes, uploaded_file.name, col_name, {})
                st.success("Файл успешно обработан")
                st.dataframe(df)
                # --- Скачивание ---
                col1, col2 = st.columns(2)
                with col1:
                    # Скачать как Excel
                    buffer_xlsx = io.BytesIO()
                    df.to_excel(buffer_xlsx, index=False, engine='openpyxl')
                    buffer_xlsx.seek(0)
                    st.download_button(
                        label="Скачать как Excel",
                        data=buffer_xlsx,
                        file_name="processed_" + uploaded_file.name,
                        mime="application/vnd.openpyxl.spreadsheetml.sheet"
                    )
                with col2:
                    # Скачать как CSV
                    buffer_csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Скачать как CSV",
                        data=buffer_csv,
                        file_name="processed_" + os.path.splitext(uploaded_file.name)[0] + ".csv",
                        mime="text/csv"
                    )
            else:
                st.warning("Не удалось определить названия столбцов.")
        except Exception as e:
            st.error(f"Ошибка при обработке файла: {e}")

# --- Вспомогательные функции для подсветки и обработки ---
def process_text(text: str, struct: Dict[str, Any]) -> Tuple[str, str]:
    if not text:
        return text, ""
    matches = get_matches(text, struct)
    html_result = highlight_html(text, matches)
    return text, html_result

def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str, struct: Dict[str, Any]) -> pd.DataFrame:
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".csv":
        try:
            text = file_bytes.decode("utf-8")
        except:
            text = file_bytes.decode("cp1251", errors="replace")
        df = pd.read_csv(io.StringIO(text))
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден в файле.")
    series = df[col_name].fillna("").astype(str)
    plain_list = []
    html_list = []
    for txt in series:
        plain, html = process_text(txt, struct)
        plain_list.append(plain)
        html_list.append(html)
    df[col_name] = plain_list
    df[f"{col_name}_preview_html"] = html_list
    return df

import html

def get_matches(text: str, struct: Dict[str, Any]) -> List[Tuple[int, int, str, str]]:
    if not text or not struct or struct.get("trie") is None:
        return []
    trie = struct["trie"]
    text_l = text.lower()
    n = len(text_l)
    max_len = struct.get("max_len", n)
    END = "_end_"
    matches: List[Tuple[int, int, str, str]] = []
    for i in range(n):
        node = trie
        for j in range(i, min(n, i + max_len)):
            ch = text_l[j]
            if ch not in node:
                break
            node = node[ch]
            if END in node:
                lk, (orig_key, disp) = node[END]
                start = i
                end = j + 1
                if start > 0 and _is_word_char(text[start - 1]):
                    continue
                if end < n and _is_word_char(text[end]):
                    continue
                matches.append((start, end, orig_key, disp))
    matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    filtered = []
    last_end = -1
    for s, e, ok, du in matches:
        if s >= last_end:
            filtered.append((s, e, ok, du))
            last_end = e
    return filtered

def _is_word_char(c: str) -> bool:
    return c.isalnum() or c == "_"

def highlight_html(text: str, matches: List[Tuple[int, int, str, str]]) -> str:
    if not matches:
        return escape_html(text)
    out = []
    last = 0
    for s, e, orig, disp in matches:
        out.append(escape_html(text[last:s]))
        snippet = escape_html(text[s:e])
        out.append(f'<mark style="{HIGHLIGHT_STYLE}">{snippet}</mark>')
        last = e
    out.append(escape_html(text[last:]))
    return "".join(out)

def escape_html(s: str) -> str:
    return html.escape(s)

HIGHLIGHT_STYLE = "background: #ffeb3b; color: #000; padding: 0 2px; border-radius: 2px;"

if __name__ == "__main__":
    run()
