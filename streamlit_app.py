#!/usr/bin/env python3
import io
import os
import json
from typing import Dict, Tuple
import pandas as pd
import streamlit as st
import html

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
    # Вставьте сюда весь ваш словарь как есть
    "Acura": "Акура",
    "Integra": "Интегра",
    "MDX": "МДХ",
    # ... (остальной ваш словарь) ...
}

# Загрузка существующего файла при запуске
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            saved_dict = json.load(f)
            if isinstance(saved_dict, dict):
                car_brands_models.update({str(k): str(v) for k, v in saved_dict.items()})
    except Exception:
        pass

# Сохранение словаря
def save_dictionary_to_file(dictionary: Dict[str, str], filename: str = ADDITIONS_FILE):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(dictionary, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.exception(f"Ошибка при сохранении файла: {e}")

# Обновление из файла
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
        if loaded_dict:
            car_brands_models.update(loaded_dict)
            save_dictionary_to_file(car_brands_models)
            st.success("Словарь обновлён из файла и сохранён.")
    except Exception as e:
        st.error(f"Ошибка при загрузке файла словаря: {e}")

# Основная обработка текста с подсветкой только слов внутри всего текста
def process_text(
    text: str,
    struct: dict,
    dict_brands_models: Dict[str, str],
    translit_enabled: bool
) -> Tuple[str, str]:
    if not text:
        return text, ""
    search_terms = list(dict_brands_models.keys())
    texts_for_search = [text]
    if translit_enabled:
        translit_text = transliterate(text, 'lat2cyr')
        texts_for_search.append(translit_text)
    matches_info = []
    lower_texts = [t.lower() for t in texts_for_search]
    for idx, t in enumerate(lower_texts):
        for word in search_terms:
            word_lower = word.lower()
            start_idx = t.find(word_lower)
            if start_idx != -1:
                end_idx = start_idx + len(word_lower)
                # Индексы в оригинальном тексте
                start_in_orig = start_idx
                end_in_orig = end_idx
                # Запоминаем слово для подсветки
                matches_info.append((start_in_orig, end_in_orig, word))
    # Создаем подсветку только для найденных слов внутри текста
    html_preview = highlight_html_full(text, matches_info)
    if matches_info:
        translations = " / ".join({dict_brands_models.get(w, "") for _, _, w in matches_info})
        result_str = f"{text} - ({translations})"
    else:
        result_str = text
    return result_str, html_preview

# Подсветка всего текста с выделением найденных слов
def highlight_html_full(text: str, matches: list) -> str:
    if not matches:
        return html.escape(text)
    # Сортируем по началу
    matches_sorted = sorted(matches, key=lambda x: x[0])
    out = []
    last_idx = 0
    for start, end, _ in matches_sorted:
        # добавляем часть текста до слова
        if start > last_idx:
            out.append(html.escape(text[last_idx:start]))
        # добавляем подсвеченное слово
        out.append(f'<mark style="{HIGHLIGHT_STYLE}">{html.escape(text[start:end])}</mark>')
        last_idx = end
    # добавляем остаток текста
    if last_idx < len(text):
        out.append(html.escape(text[last_idx:]))
    return "".join(out)

HIGHLIGHT_STYLE = "background: #ffeb3b; color: #000; padding: 0 2px; border-radius: 2px;"

# Обработка файла
def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str, dict_brands_models: Dict[str, str], translit_enabled: bool) -> pd.DataFrame:
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".csv":
        text = None
        for enc in ("utf-8", "cp1251", "latin1"):
            try:
                text = file_bytes.decode(enc)
                df = pd.read_csv(io.StringIO(text))
                break
            except:
                continue
        if text is None:
            text = file_bytes.decode("utf-8", errors="replace")
            df = pd.read_csv(io.StringIO(text))
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден в файле.")
    series = df[col_name].fillna("").astype(str)
    plain_list = []
    for txt in series:
        plain, _ = process_text(txt, {}, dict_brands_models, translit_enabled)
        plain_list.append(plain)
    df_result = df.copy()
    df_result[col_name] = plain_list
    return df_result

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

    # --- Загрузка файла словаря ---
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

    # --- Опция поиска по транслиту ---
    translit_enabled = st.checkbox("Искать и по транслиту", value=True)

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

    # --- Обработка файла ---    
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

            # Выбор столбца для обработки
            if not df_preview.empty:
                col_options = list(df_preview.columns)
                col_name = st.selectbox("Выберите название столбца для обработки", options=col_options)
            else:
                col_name = st.text_input("Введите название столбца для обработки", value="Название")
            
            if col_name:
                # Обработка файла и получение DataFrame с обработанными данными
                df_processed = process_file_for_processing(file_bytes, uploaded_file.name, col_name, car_brands_models, translit_enabled)
                st.success("Файл успешно обработан")
                # Выводим обработанный DataFrame без _preview_html
                st.dataframe(df_processed)
                # --- Скачивание ---
                col1, col2 = st.columns(2)
                with col1:
                    buf_xlsx = io.BytesIO()
                    df_processed.to_excel(buf_xlsx, index=False, engine='openpyxl')
                    buf_xlsx.seek(0)
                    st.download_button(
                        label="Скачать как Excel",
                        data=buf_xlsx,
                        file_name="processed_" + uploaded_file.name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                with col2:
                    buf_csv = df_processed.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Скачать как CSV",
                        data=buf_csv,
                        file_name="processed_" + os.path.splitext(uploaded_file.name)[0] + ".csv",
                        mime="text/csv"
                    )
            else:
                st.warning("Не удалось определить название столбца.")
        except Exception as e:
            st.error(f"Ошибка при обработке файла: {e}")

if __name__ == "__main__":
    run()
