# !/usr/bin/env python3
# Улучшенная и более надёжная версия исходного скрипта для Streamlit
# Основные улучшения:
# - Более устойчивое определение языка (подсчёт кириллических/латинских букв)
# - Сохранение регистра при подстановке (всё, Title, lower)
# - Более корректная токенизация с сохранением пробелов и пунктуации
# - Безопасная запись файла (атомарно) и обработка ошибок с логированием
# - Чтение CSV/XLSX с запасными кодировками
# - Небольшой пример словаря для теста (замените на полный)

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

# Optional morphological analyzer
try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

logging.basicConfig(level=logging.INFO)

# --- Настройки ---
ADDITIONS_FILE = "additional_brands.json"

# Пример словаря (замените/дополните на полный)
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

# compile sorted keys for longest-first matching
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
        # атомарная запись
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

# Язык: подсчёт букв
_RE_CYR = re.compile(r'[А-Яа-яЁё]')
_RE_LAT = re.compile(r'[A-Za-z]')

def detect_language(text: str) -> str:
    if not text:
        return 'ru'
    cyr = len(_RE_CYR.findall(text))
    lat = len(_RE_LAT.findall(text))
    # Threshold: если латинских букв больше, считаем en
    return 'en' if lat > cyr else 'ru'

def preserve_case_replace(src: str, repl: str) -> str:
    # Сохранить регистр: UPPER, Title, lower
    if src.isupper():
        return repl.upper()
    if src.istitle():
        return repl.capitalize()
    return repl

# Токенизация: возвращаем последовательность токенов (слова, пробелы,
# пунктуация)
_RE_TOK = re.compile(r'\w+|\s+|[^\w\s]+', flags=re.UNICODE)

def process_text(text: str, dict_brands_models: Dict[str, str], translit_enabled: bool = True) -> str:
    if text is None:
        return ''
    original = text
    # нормализованный словарь для быстрых проверок (lower -> value)
    norm_map = {k.lower(): v for k, v in dict_brands_models.items()}

    tokens = _RE_TOK.findall(text)
    lang = detect_language(text)

    for i, tk in enumerate(tokens):
        # process only word tokens (letters/digits/underscore)
        if tk.strip() and tk.strip().isalnum():
            key = tk.lower()
            replacement: Optional[str] = None
            if key in norm_map:
                replacement = preserve_case_replace(tk, norm_map[key])
            elif translit_enabled:
                # try transliteration of latin to cyrillic
                trans = transliterate(tk, 'lat2cyr')
                if trans.lower() in norm_map:
                    replacement = preserve_case_replace(tk, norm_map[trans.lower()])
                else:
                    # also try removing non-alnum from boundaries and retry
                    pass
            if replacement is not None:
                tokens[i] = replacement

    joined = ''.join(tokens)

    # Если исходный текст английский (латиница доминирует), добавляем транслит в скобках
    if lang == 'en':
        text_translit = transliterate(joined, 'lat2cyr')
        return f"{original} - ({text_translit})"

    # В остальных случаях возвращаем оригинал + обработанный в скобках (если изменился)
    if joined == original:
        return f"{original} - ({joined})"
    return f"{original} - ({joined})"

# --- Работа с загруженным файлом ---
def read_dataframe_from_bytes(file_bytes: bytes, filename: str) -> pd.DataFrame:
    ext = os.path.splitext(filename)[1].lower()
    if ext in ('.xlsx', '.xls'):
        # try openpyxl first; pandas can auto select engine in some setups
        return pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    if ext == '.csv':
        # try utf-8 then cp1251
        for enc in ('utf-8', 'cp1251'):
            try:
                return pd.read_csv(io.StringIO(file_bytes.decode(enc)), dtype=str)
            except Exception:
                continue
        # fallback: let pandas guess with latin1
        return pd.read_csv(io.StringIO(file_bytes.decode('latin1')), dtype=str)
    # unknown -> empty
    return pd.DataFrame()

def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str,
                                dict_brands_models: Dict[str, str], translit_enabled: bool) -> pd.DataFrame:
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

# --- Загрузка дополнительных данных при старте ---
car_brands_models.update(load_additional_dict(ADDITIONS_FILE))

# --- Streamlit UI ---
def update_dict_from_uploaded_file(uploaded_file) -> None:
    try:
        data = uploaded_file.read()
        name = uploaded_file.name.lower()
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
            car_brands_models.update(new)
            safe_save_json(car_brands_models, ADDITIONS_FILE)
            st.success("Словарь обновлён и сохранён.")
        else:
            st.warning("Не удалось распарсить файл словаря.")
    except Exception as e:
        logging.exception("update_dict_from_uploaded_file failed")
        st.error(f"Ошибка при загрузке словаря: {e}")

def run():
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")
    st.title("Обработка брендов/моделей")

    uploaded_dict_file = st.file_uploader("Загрузить файл словаря (JSON, CSV или XLSX)", type=["json", "csv", "xlsx"])
    if uploaded_dict_file:
        update_dict_from_uploaded_file(uploaded_dict_file)

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

    translit_enabled = st.checkbox("Искать и по транслиту (лат→кир)", value=True)

    uploaded_file = st.file_uploader("Загрузите Excel или CSV файл", type=["xlsx", "xls", "csv"])
    if uploaded_file:
        try:
            file_bytes = uploaded_file.read()
            # preview
            try:
                df_preview = read_dataframe_from_bytes(file_bytes, uploaded_file.name).head(5)
            except Exception:
                df_preview = pd.DataFrame()
            if not df_preview.empty:
                col_name = st.selectbox("Выберите столбец для обработки", options=list(df_preview.columns))
            else:
                col_name = st.text_input("Введите название столбца для обработки", value="Название")

            if col_name:
                df_processed = process_file_for_processing(file_bytes, uploaded_file.name, col_name, car_brands_models, translit_enabled)
                st.success("Файл обработан")
                st.dataframe(df_processed)
                # downloads
                buf_xlsx = io.BytesIO()
                df_processed.to_excel(buf_xlsx, index=False, engine='openpyxl')
                buf_xlsx.seek(0)
                st.download_button("Скачать как Excel", buf_xlsx, file_name="processed_" + uploaded_file.name,
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                buf_csv = df_processed.to_csv(index=False).encode('utf-8')
                st.download_button("Скачать как CSV", buf_csv, file_name="processed_" + os.path.splitext(uploaded_file.name)[0] + ".csv",
                                   mime="text/csv")
            else:
                st.warning("Укажите имя столбца для обработки.")
        except Exception as e:
            logging.exception("Processing uploaded file failed")
            st.error(f"Ошибка при обработке файла: {e}")

if __name__ == "__main__":
    run()
