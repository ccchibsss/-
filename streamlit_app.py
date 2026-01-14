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

car_brands_models: Dict[str, str] = {
    "Acura": "Акура",
    "Integra": "Интегра",
    "MDX": "МДХ",
    "RDX": "РДХ",
    "RSX": "РСХ",
    "TLX": "ТЛКС",
}

if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                car_brands_models.update({str(k): str(v) for k, v in loaded.items()})
    except Exception:
        pass

@lru_cache(maxsize=20000)
def decline_word_cached(word: str) -> str:
    if not word or morph is None:
        return word
    try:
        p = morph.parse(word)[0]
        inf = p.inflect({"nomn"})
        return inf.word if inf else p.word
    except Exception:
        return word

def build_final_struct(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    final = {**(base_map or {})}
    if additions:
        final.update(additions)
    final = {k: v for k, v in final.items() if isinstance(k, str) and k.strip()}
    if not final:
        return {"trie": None, "map": {}, "max_len": 0}
    mapping: Dict[str, Tuple[str, str]] = {}
    max_len = 0
    for k, v in final.items():
        lk = k.lower()
        display = v if v is not None else k
        display_decl = decline_word_cached(str(display))
        mapping[lk] = (k, display_decl)
        if len(lk) > max_len:
            max_len = len(lk)
    trie = {}
    END = "_end_"
    for lk, pair in mapping.items():
        node = trie
        for ch in lk:
            node = node.setdefault(ch, {})
        node[END] = (lk, pair)
    return {"trie": trie, "map": mapping, "max_len": max_len}

def _is_word_char(c: str) -> bool:
    return c.isalnum() or c == "_"

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

HIGHLIGHT_STYLE = "background: #ffeb3b; color: #000; padding: 0 2px; border-radius: 2px;"

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
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))

def process_text(text: str, struct: Dict[str, Any]) -> Tuple[str, str]:
    if not text:
        return text, ""
    matches = get_matches(text, struct)
    html = highlight_html(text, matches)
    return text, html

def load_dictionary(source: Optional[str] = None, fileobj: Optional[io.BytesIO] = None) -> Dict[str, str]:
    try:
        if fileobj is not None:
            if hasattr(fileobj, "getvalue"):
                data_bytes = fileobj.getvalue()
            else:
                data_bytes = fileobj.read()
            name = getattr(fileobj, "name", "") or source or ""
            if name.lower().endswith(".json"):
                text = data_bytes.decode("utf-8")
                obj = json.loads(text)
                return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
            if name.lower().endswith(".csv"):
                text = data_bytes.decode("utf-8")
                df = pd.read_csv(io.StringIO(text))
            else:
                df = pd.read_excel(io.BytesIO(data_bytes), engine='openpyxl')
            if len(df.columns) >= 2:
                return {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
        if source:
            if source.startswith("http"):
                r = requests.get(source, timeout=10)
                r.raise_for_status()
                if source.lower().endswith(".json"):
                    obj = r.json()
                    return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
                if source.lower().endswith(".csv"):
                    df = pd.read_csv(io.StringIO(r.text))
                else:
                    ext = os.path.splitext(source)[1].lower()
                    if ext in ['.xlsx', '.xlsm', '.xltx', '.xltm']:
                        df = pd.read_excel(io.BytesIO(r.content), engine='openpyxl')
                    elif ext == '.xls':
                        df = pd.read_excel(io.BytesIO(r.content), engine='xlrd')
                    else:
                        df = pd.read_excel(io.BytesIO(r.content), engine='openpyxl')
                if len(df.columns) >= 2:
                    return {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
            else:
                ext = os.path.splitext(source)[1].lower()
                if ext == '.json':
                    with open(source, "r", encoding="utf-8") as f:
                        obj = json.load(f)
                        return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
                elif ext == '.csv':
                    df = pd.read_csv(source)
                elif ext in ['.xlsx', '.xlsm', '.xltx', '.xltm']:
                    df = pd.read_excel(source, engine='openpyxl')
                elif ext == '.xls':
                    df = pd.read_excel(source, engine='xlrd')
                if len(df.columns) >= 2:
                    return {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
    except Exception:
        return {}
    return {}

def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str, struct: Dict[str, Any]) -> pd.DataFrame:
    if filename.lower().endswith(".csv"):
        try:
            text = file_bytes.decode("utf-8")
        except Exception:
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

# --- Основная функция ---
def run():
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")

    # --- Раздел "Настройки словаря" ---
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px">
        <h2 style="color:white;margin:0">🛠️ Настройки словаря</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Редактирование словаря латиница→кириллица</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Загрузка словаря из файла (JSON, CSV, XLSX)
    uploaded_dict_file = st.file_uploader("Загрузить файл словаря (JSON, CSV или XLSX)", type=["json", "csv", "xlsx"])
    if uploaded_dict_file:
        try:
            dict_bytes = uploaded_dict_file.read()
            if uploaded_dict_file.name.lower().endswith(".json"):
                loaded_dict = json.loads(dict_bytes.decode("utf-8"))
            elif uploaded_dict_file.name.lower().endswith(".csv"):
                df_dict = pd.read_csv(io.StringIO(dict_bytes.decode("utf-8")))
                loaded_dict = {str(k): str(v) for k, v in zip(df_dict.iloc[:,0], df_dict.iloc[:,1])}
            elif uploaded_dict_file.name.lower().endswith(".xlsx"):
                df_dict = pd.read_excel(io.BytesIO(dict_bytes), engine='openpyxl')
                loaded_dict = {str(k): str(v) for k, v in zip(df_dict.iloc[:,0], df_dict.iloc[:,1])}
            else:
                loaded_dict = {}
            if loaded_dict:
                latin_to_cyr.update({k:v for k,v in loaded_dict.items()})
                st.success("Словарь обновлён из файла.")
        except Exception as e:
            st.error(f"Ошибка при загрузке словаря: {e}")

    # Редактирование словаря вручную через text_area
    st.subheader("Редактировать словарь вручную")
    dict_text = "\n".join([f"{k},{v}" for k, v in latin_to_cyr.items()])
    edited_text = st.text_area("Редактировать словарь (каждая строка: латиница,кириллица)", value=dict_text, height=100)

    if st.button("Сохранить словарь"):
        new_dict = {}
        for line in edited_text.splitlines():
            if line.strip():
                parts = line.split(",", 1)
                if len(parts) == 2:
                    k, v = parts
                    new_dict[k.strip()] = v.strip()
        latin_to_cyr.clear()
        latin_to_cyr.update(new_dict)
        # Можно сохранить на диск
        with open("latin_to_cyr.json", "w", encoding="utf-8") as f:
            json.dump(latin_to_cyr, f, ensure_ascii=False, indent=2)
        st.success("Словарь сохранён.")

    # --- Остальной интерфейс (транслитерация, обработка файла) ---

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
    col_name = st.text_input("Введите название столбца для обработки", value="Название")
    if uploaded_file and col_name:
        try:
            file_bytes = uploaded_file.read()
            df = process_file_for_processing(file_bytes, uploaded_file.name, col_name, {})
            st.success("Файл успешно обработан")
            st.dataframe(df)
            # Предлагаем скачать
            if uploaded_file.name.lower().endswith((".xlsx", ".xls")):
                buffer = io.BytesIO()
                df.to_excel(buffer, index=False, engine='openpyxl')
                buffer.seek(0)
                st.download_button(
                    label="Скачать обработанный Excel",
                    data=buffer,
                    file_name="processed_" + uploaded_file.name,
                    mime="application/vnd.openpyxl.spreadsheetml.sheet"
                )
            elif uploaded_file.name.lower().endswith(".csv"):
                csv_bytes = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Скачать обработанный CSV",
                    data=csv_bytes,
                    file_name="processed_" + uploaded_file.name,
                    mime="text/csv"
                )
        except Exception as e:
            st.error(f"Ошибка при обработке файла: {e}")

if __name__ == "__main__":
    run()
