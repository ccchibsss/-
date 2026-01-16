#!/usr/bin/env python3
# Улучшенная версия для Streamlit с настройками обработки

from __future__ import annotations
import io
import os
import json
import tempfile
import logging
from typing import Dict, Optional
import re

import pandas as pd
import streamlit as st

# Попытка импортировать pymorphy2 (опционально)
try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

logging.basicConfig(level=logging.INFO)

# --- Настройки ---
ADDITIONS_FILE = "additional_brands.json"

# Пример словаря брендов и моделей
car_brands_models: Dict[str, str] = {
    "Acura": "Акура",
    "Integra": "Интегра",
    "MDX": "МДХ",
    "RDX": "РДХ",
    "RSX": "РСХ",
    "TLX": "ТЛКС",
}

# --- Транслитерация + карты ---
_LAT_TO_CYR = {
    'Shch':'Щ','shch':'щ','SHCH':'Щ',
    'Yo':'Ё','yo':'ё','YO':'Ё',
    'Zh':'Ж','zh':'ж','ZH':'Ж',
    'Kh':'Х','kh':'х','KH':'Х',
    'Ts':'Ц','ts':'ц','TS':'Ц',
    'Ch':'Ч','ch':'ч','CH':'Ч',
    'Sh':'Ш','sh':'ш','SH':'Ш',
    'Yu':'Ю','yu':'ю','YU':'Ю',
    'Ya':'Я','ya':'я','YA':'Я',
    "Y'":"Ы","y'":"ы",
    "E'":"Э","e'":"э",
    'A':'А','a':'а','B':'Б','b':'б','V':'В','v':'в','G':'Г','g':'г',
    'D':'Д','d':'д','E':'Е','e':'е','Z':'З','z':'з','I':'И','i':'и',
    'Y':'Й','y':'й','K':'К','k':'к','L':'Л','l':'л','M':'М','m':'м',
    'N':'Н','n':'н','O':'О','o':'о','P':'П','p':'п','R':'Р','r':'р',
    'S':'С','s':'с','T':'Т','t':'т','U':'У','u':'у','F':'Ф','f':'ф',
}

_CYR_TO_LAT = {
    'А':'A','а':'a','Б':'B','б':'b','В':'V','в':'v','Г':'G','г':'g',
    'Д':'D','д':'d','Е':'E','е':'e','Ё':'Yo','ё':'yo','Ж':'Zh','ж':'zh',
    'З':'Z','з':'z','И':'I','и':'i','Й':'Y','й':'y','К':'K','к':'k',
    'Л':'L','л':'l','М':'M','м':'m','Н':'N','н':'n','О':'O','о':'o',
    'П':'P','п':'p','Р':'R','р':'r','С':'S','с':'s','Т':'T','т':'t',
    'У':'U','у':'u','Ф':'F','ф':'f','Х':'Kh','х':'kh','Ц':'Ts','ц':'ts',
    'Ч':'Ch','ч':'ch','Ш':'Sh','ш':'sh','Щ':'Shch','щ':'shch',
    'Ы':"Y'",'ы':"y'",'Э':"E'",'э':"e'",'Ю':'Yu','ю':'yu','Я':'Ya','я':'ya'
}

_LAT_KEYS = sorted(_LAT_TO_CYR.keys(), key=len, reverse=True)

def transliterate_latin_to_cyrillic(text: str) -> str:
    if not text:
        return text
    i = 0
    out = []
    L = len(text)
    while i < L:
        matched = False
        for k in _LAT_KEYS:
            if text.startswith(k, i):
                out.append(_LAT_TO_CYR[k])
                i += len(k)
                matched = True
                break
        if not matched:
            out.append(text[i])
            i += 1
    return ''.join(out)

def transliterate_cyrillic_to_latin(text: str) -> str:
    if not text:
        return text
    return ''.join(_CYR_TO_LAT.get(ch, ch) for ch in text)

def transliterate(text: str, direction: str = 'lat2cyr') -> str:
    if direction == 'lat2cyr':
        return transliterate_latin_to_cyrillic(text)
    if direction == 'cyr2lat':
        return transliterate_cyrillic_to_latin(text)
    return text

# --- Вспомогательные функции ---
def safe_save_json(dictionary: Dict[str, str], filename: str = ADDITIONS_FILE) -> None:
    try:
        dirn = os.path.dirname(filename) or '.'
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=dirn, delete=False) as tmp:
            json.dump(dictionary, tmp, ensure_ascii=False, indent=2)
            tmp_name = tmp.name
        os.replace(tmp_name, filename)
        logging.info("Dictionary saved to %s", filename)
    except Exception as e:
        logging.exception("Failed to save dictionary: %s", e)

def load_additional_dict(filename: str = ADDITIONS_FILE) -> Dict[str, str]:
    if not os.path.exists(filename):
        return {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            obj = json.load(f)
            if isinstance(obj, dict):
                return {str(k): str(v) for k, v in obj.items()}
    except Exception:
        logging.exception("Failed to load additions file")
    return {}

_RE_CYR = re.compile(r'[А-Яа-яЁё]')
_RE_LAT = re.compile(r'[A-Za-z]')

def detect_language(text: str) -> str:
    if not text:
        return 'ru'
    cyr = len(_RE_CYR.findall(text))
    lat = len(_RE_LAT.findall(text))
    return 'en' if lat > cyr else 'ru'

def preserve_case_replace(src: str, repl: str) -> str:
    if src.isupper():
        return repl.upper()
    if src.istitle():
        return repl.capitalize()
    return repl

_RE_TOK = re.compile(r'\w+|\s+|[^\w\s]+', flags=re.UNICODE)

# Новая функция: обработка текста с учетом флага транслита и опций
def process_text(
    text: str,
    dict_brands_models: Dict[str, str],
    translit_enabled: bool = True,
    enable_dict: bool = True,
    enable_en_ru: bool = True,
    enable_lat_cyr: bool = True
) -> str:
    if text is None:
        return ''
    original = text
    norm_map = {k.lower(): v for k, v in dict_brands_models.items()}

    tokens = _RE_TOK.findall(text)
    lang = detect_language(text)

    for i, tk in enumerate(tokens):
        if tk.strip() and tk.strip().isalnum():
            key = tk.lower()
            replacement: Optional[str] = None
            # Обработка по словарю
            if enable_dict and key in norm_map:
                replacement = preserve_case_replace(tk, norm_map[key])
            elif enable_en_ru:
                pass  # Можно добавить дополнительные правила
            # Обработка транслитерации латиницы
            if enable_lat_cyr and re.match(r'^[A-Za-z]+$', tk):
                trans = transliterate(tk, 'lat2cyr')
                if trans.lower() in norm_map:
                    replacement = preserve_case_replace(tk, norm_map[trans.lower()])
                # иначе оставить как есть
                if replacement is not None:
                    tokens[i] = replacement

    joined = ''.join(tokens)

    # Для английского добавляем транслит
    if lang == 'en' and enable_lat_cyr:
        text_translit = transliterate(joined, 'lat2cyr')
        return f"{original} - ({text_translit})"

    # Для русского или других языков
    if joined == original:
        return f"{original} - ({joined})"
    return f"{original} - ({joined})"

# --- Работа с файлами ---
def read_dataframe_from_bytes(file_bytes: bytes, filename: str) -> pd.DataFrame:
    ext = os.path.splitext(filename)[1].lower()
    if ext in ('.xlsx', '.xls'):
        return pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    if ext == '.csv':
        for enc in ('utf-8', 'cp1251'):
            try:
                return pd.read_csv(io.StringIO(file_bytes.decode(enc)), dtype=str)
            except Exception:
                continue
        return pd.read_csv(io.StringIO(file_bytes.decode('latin1')), dtype=str)
    return pd.DataFrame()

def process_file_for_processing(
    file_bytes: bytes,
    filename: str,
    col_name: str,
    dict_brands_models: Dict[str, str],
    translit_enabled: bool
) -> pd.DataFrame:
    df = read_dataframe_from_bytes(file_bytes, filename)
    if df.empty:
        raise ValueError("В файле нет данных или формат не поддерживается.")
    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден в файле.")
    series = df[col_name].astype(str).fillna("")
    processed = [process_text(s, dict_brands_models, translit_enabled) for s in series]
    df_out = df.copy()
    df_out[col_name] = processed
    return df_out

# --- Загрузка словаря при старте ---
car_brands_models.update(load_additional_dict(ADDITIONS_FILE))

# --- UI Streamlit ---
def run():
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")
    st.title("Обработка брендов/моделей")

    # Флаги обработки
    enable_dict = st.checkbox("Обрабатывать по словарю", value=True)
    enable_en_ru = st.checkbox("Обрабатывать английский/русский", value=True)
    enable_lat_cyr = st.checkbox("Обрабатывать транслит (лат→кир)", value=True)

    # Загрузка файла словаря
    uploaded_dict_file = st.file_uploader("Загрузить файл словаря (JSON, CSV или XLSX)", type=["json", "csv", "xlsx"])
    if uploaded_dict_file:
        try:
            data = uploaded_dict_file.read()
            name = uploaded_dict_file.name.lower()
            new = {}
            if name.endswith('.json'):
                obj = json.loads(data.decode('utf-8'))
                if isinstance(obj, dict):
                    new = {str(k): str(v) for k, v in obj.items()}
            elif name.endswith('.csv'):
                df = pd.read_csv(io.StringIO(data.decode('utf-8')))
                if df.shape[1] >= 2:
                    new = {str(k): str(v) for k, v in zip(df.iloc[:,0].astype(str), df.iloc[:,1].astype(str))}
            elif name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(data), engine='openpyxl')
                if df.shape[1] >= 2:
                    new = {str(k): str(v) for k, v in zip(df.iloc[:,0].astype(str), df.iloc[:,1].astype(str))}
            if new:
                car_brands_models.clear()
                car_brands_models.update(new)
                safe_save_json(car_brands_models, ADDITIONS_FILE)
                st.success("Словарь обновлён и сохранён.")
        except Exception as e:
            st.error(f"Ошибка при загрузке словаря: {e}")

    # Редактирование словаря вручную
    dict_text = "\n".join(f"{k},{v}" for k, v in car_brands_models.items())
    edited_text = st.text_area("Редактировать словарь (каждая строка: латиница,кириллица)", value=dict_text, height=200)
    if st.button("Сохранить словарь"):
        new_dict = {}
        for line in edited_text.splitlines():
            if not line.strip():
                continue
            parts = line.split(",", 1)
            if len(parts) == 2:
                k, v = parts
                new_dict[k.strip()] = v.strip()
        if new_dict:
            car_brands_models.clear()
            car_brands_models.update(new_dict)
            safe_save_json(car_brands_models, ADDITIONS_FILE)
            st.success("Словарь сохранён.")
        else:
            st.warning("Нет корректных строк для сохранения.")

    # Загрузка файла для обработки
    uploaded_file = st.file_uploader("Загрузите Excel или CSV файл", type=["xlsx", "xls", "csv"])
    if uploaded_file:
        try:
            file_bytes = uploaded_file.read()
            # Предварительный просмотр и выбор столбца
            df_preview = read_dataframe_from_bytes(file_bytes, uploaded_file.name).head(5)
            if not df_preview.empty:
                col_options = list(df_preview.columns)
                col_name = st.selectbox("Выберите столбец для обработки", options=col_options)
            else:
                col_name = st.text_input("Введите название столбца для обработки", value="Название")
            # Кнопка для запуска обработки
            if st.button("Обработать файл"):
                df_processed = process_file_for_processing(file_bytes, uploaded_file.name, col_name, car_brands_models, enable_lat_cyr)
                st.success("Обработка завершена")
                st.dataframe(df_processed)
                # Скачивание в Excel
                buf_xlsx = io.BytesIO()
                df_processed.to_excel(buf_xlsx, index=False, engine='openpyxl')
                buf_xlsx.seek(0)
                st.download_button("Скачать как Excel", buf_xlsx, file_name=f"processed_{uploaded_file.name}",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                # Скачивание в CSV
                buf_csv = df_processed.to_csv(index=False).encode('utf-8')
                st.download_button("Скачать как CSV", buf_csv, file_name=f"processed_{os.path.splitext(uploaded_file.name)[0]}.csv",
                                   mime="text/csv")
            else:
                st.info("Выберите столбец и нажмите 'Обработать файл'")
        except Exception as e:
            st.error(f"Ошибка при обработке файла: {e}")

if __name__ == "__main__":
    run()
