#!/usr/bin/env python3
import io
import os
import json
from typing import Dict
import pandas as pd
import streamlit as st
import re

# Импорт морфологического анализатора (не обязательно)
try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

# --- Словарь транслитерации ---
latin_to_cyr = {
    'A':'А','a':'а','B':'Б','b':'б','V':'В','v':'в','G':'Г','g':'г',
    'D':'Д','d':'д','E':'Е','e':'е','Yo':'Ё','yo':'ё','ZH':'Ж','zh':'ж',
    'Z':'З','z':'з','I':'И','i':'и','Y':'Й','y':'й','K':'К','k':'к',
    'L':'Л','l':'л','M':'М','m':'м','N':'Н','n':'н','O':'О','o':'о',
    'P':'П','p':'п','R':'Р','r':'р','S':'С','s':'с','T':'Т','t':'т',
    'U':'У','u':'у','F':'Ф','f':'ф','Kh':'Х','kh':'х','Ts':'Ц','ts':'ц',
    'Ch':'Ч','ch':'ч','Sh':'Ш','sh':'ш','Shch':'Щ','shch':'щ',
    'Y\'':'Ы','y\'':'ы','E\'':'Э','e\'':'э','Yu':'Ю','yu':'ю','Ya':'Я','ya':'я'
}
# Обратный словарь для кириллицы в латиницу
cyrillic_to_latin = {v: k for k, v in latin_to_cyr.items()}

def transliterate_latin_to_cyrillic(text: str) -> str:
    result = ''
    i = 0
    while i < len(text):
        match_found = False
        for length in [3, 2, 1]:
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
    result = ''
    for ch in text:
        result += cyrillic_to_latin.get(ch, ch)
    return result

def transliterate(text: str, direction: str='lat2cyr') -> str:
    if direction == 'lat2cyr':
        return transliterate_latin_to_cyrillic(text)
    elif direction == 'cyr2lat':
        return transliterate_cyrillic_to_latin(text)
    else:
        return text

# Ваш словарь марок и моделей
car_brands_models: Dict[str, str] = {
    # пример
    "Kia": "Киа",
    "Sportage": "Спортейдж",
    "Retona": "Ретона",
    # добавьте остальные по вашему списку
}

# Загрузка дополнительных данных
ADDITIONS_FILE = "additional_brands.json"
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            saved_dict = json.load(f)
            if isinstance(saved_dict, dict):
                car_brands_models.update({str(k): str(v) for k, v in saved_dict.items()})
    except:
        pass

def save_dictionary_to_file(dictionary: Dict[str, str], filename: str=ADDITIONS_FILE):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(dictionary, f, ensure_ascii=False, indent=2)
    except:
        pass

def update_dict_from_uploaded_file(uploaded_file):
    try:
        data_bytes = uploaded_file.read()
        filename_lower = uploaded_file.name.lower()
        loaded_dict = {}
        if filename_lower.endswith('.json'):
            obj = json.loads(data_bytes.decode('utf-8'))
            if isinstance(obj, dict):
                loaded_dict = {str(k): str(v) for k, v in obj.items()}
        elif filename_lower.endswith('.csv'):
            df = pd.read_csv(io.StringIO(data_bytes.decode('utf-8')))
            if len(df.columns) >=2:
                loaded_dict = {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
        elif filename_lower.endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(data_bytes), engine='openpyxl')
            if len(df.columns) >=2:
                loaded_dict = {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
        if loaded_dict:
            car_brands_models.update(loaded_dict)
            save_dictionary_to_file(car_brands_models)
            st.success("Словарь обновлён из файла и сохранён.")
    except Exception as e:
        st.error(f"Ошибка при загрузке файла: {e}")

# Основная обработка строки
def process_text(text: str, dict_brands_models: dict, translit_enabled: bool) -> str:
    if not text:
        return text

    separators = ['/', ';', '-', '—', '–']
    pattern_sep = '|'.join([re.escape(s) for s in separators])
    parts = re.split(f'({pattern_sep})', text)

    # Создаем быстрый доступ
    dict_lower = {k.lower(): v for k, v in dict_brands_models.items()}
    pattern_words = '|'.join([re.escape(k) for k in dict_brands_models.keys()])
    regex = re.compile(rf'({pattern_words})', re.IGNORECASE)

    found_translations = set()
    processed_parts = []

    for part in parts:
        part_strip = part.strip()
        if part_strip in separators:
            processed_parts.append(part)
        else:
            segment = part
            search_texts = [segment]
            if translit_enabled:
                translit_text = transliterate(segment, 'lat2cyr')
                search_texts.append(translit_text)

            def replacer(match):
                word_found = match.group(0)
                key_lower = word_found.lower()
                if key_lower in dict_lower:
                    found_translations.add(dict_lower[key_lower])
                return word_found

            for t in search_texts:
                segment = regex.sub(replacer, segment)

            processed_parts.append(segment)

    full_text = ''.join(processed_parts)
    if found_translations:
        return f"{full_text} - ({' / '.join(sorted(found_translations))})"
    else:
        return full_text

# Обработка файла для поиска и обработки строк
def process_file_for_processing(file_bytes, filename, col_name, dict_brands_models, translit_enabled):
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.xlsx', '.xls']:
        df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    elif ext == '.csv':
        try:
            df = pd.read_csv(io.BytesIO(file_bytes))
        except:
            df = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')))
    else:
        df = pd.DataFrame()

    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден в файле.")
    series = df[col_name].astype(str).fillna("")

    result_list = []
    for txt in series:
        result_list.append(process_text(txt, dict_brands_models, translit_enabled))
    df_result = df.copy()
    df_result[col_name] = result_list
    return df_result

# Streamlit интерфейс
def run():
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px">
        <h2 style="color:white;margin:0">🛠️ Настройки словаря</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Редактирование словаря марок и моделей автомобилей</p>
        </div>
        """, unsafe_allow_html=True
    )

    st.info("Поддерживаются файлы: JSON, CSV, XLSX. В файле минимум 2 столбца: латиница и кириллица.")
    uploaded_dict_file = st.file_uploader("Загрузить файл словаря (JSON, CSV или XLSX)", type=["json", "csv", "xlsx"])
    if uploaded_dict_file:
        update_dict_from_uploaded_file(uploaded_dict_file)

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

    translit_enabled = st.checkbox("Искать и по транслиту", value=True)

    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#4CAF50,#81C784);padding:16px;border-radius:8px;margin-top:20px">
        <h2 style="color:white;margin:0">🌐 Транслитерация (латиница → кириллица)</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Введите латиницу для преобразования в кириллицу</p>
        </div>
        """, unsafe_allow_html=True
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

    # Обработка файла
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px;margin-top:20px">
        <h2 style="color:white;margin:0">🚗 Обработка данных</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0">Загрузите файл (Excel или CSV), поиск и обработка</p>
        </div>
        """, unsafe_allow_html=True
    )

    uploaded_file = st.file_uploader("Загрузите Excel или CSV файл", type=["xlsx", "xls", "csv"])
    if uploaded_file:
        try:
            file_bytes = uploaded_file.read()
            ext = os.path.splitext(uploaded_file.name)[1].lower()

            # Предварительный просмотр колонок
            if ext in ['.xlsx', '.xls']:
                try:
                    df_preview = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl', nrows=5)
                except:
                    df_preview = pd.read_excel(io.BytesIO(file_bytes), nrows=5)
            elif ext == '.csv':
                try:
                    df_preview = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')), nrows=5)
                except:
                    df_preview = pd.read_csv(io.StringIO(file_bytes.decode('cp1251', errors='replace')), nrows=5)
            else:
                df_preview = pd.DataFrame()

            if not df_preview.empty:
                col_options = list(df_preview.columns)
                col_name = st.selectbox("Выберите название столбца для обработки", options=col_options)
            else:
                col_name = st.text_input("Введите название столбца для обработки", value="Название")
            if col_name:
                df_processed = process_file_for_processing(file_bytes, uploaded_file.name, col_name, car_brands_models, translit_enabled)
                st.success("Файл успешно обработан")
                st.dataframe(df_processed)
                buf_xlsx = io.BytesIO()
                df_processed.to_excel(buf_xlsx, index=False, engine='openpyxl')
                buf_xlsx.seek(0)
                st.download_button("Скачать как Excel", buf_xlsx, "processed_" + uploaded_file.name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                buf_csv = df_processed.to_csv(index=False).encode('utf-8')
                st.download_button("Скачать как CSV", buf_csv, "processed_" + os.path.splitext(uploaded_file.name)[0]+".csv", mime="text/csv")
            else:
                st.warning("Не удалось определить название столбца.")
        except Exception as e:
            st.error(f"Ошибка при обработке файла: {e}")

if __name__ == "__main__":
    run()
