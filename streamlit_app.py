# !/usr/bin/env python3
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

# --- Функции транслитерации ---
def transliterate_latin_to_cyrillic(text: str) -> str:
    latin_to_cyr = {
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
    # longest-first match
    keys = sorted(latin_to_cyr.keys(), key=len, reverse=True)
    i = 0
    res = ""
    while i < len(text):
        matched = False
        for k in keys:
            if text.startswith(k, i):
                res += latin_to_cyr[k]
                i += len(k)
                matched = True
                break
        if not matched:
            res += text[i]
            i += 1
    return res

def transliterate_cyrillic_to_latin(text: str) -> str:
    cyrillic_to_latin = {
        'А':'A','а':'a','Б':'B','б':'b','В':'V','в':'v','Г':'G','г':'g',
        'Д':'D','д':'d','Е':'E','е':'e','Ё':'Yo','ё':'yo','Ж':'Zh','ж':'zh',
        'З':'Z','з':'z','И':'I','и':'i','Й':'Y','й':'y','К':'K','к':'k',
        'Л':'L','л':'l','М':'M','м':'м','Н':'N','н':'н','О':'O','о':'o',
        'П':'P','п':'p','Р':'R','р':'р','С':'S','с':'s','Т':'T','т':'t',
        'У':'U','у':'u','Ф':'F','ф':'f','Х':'Kh','х':'kh','Ц':'Ts','ц':'ts',
        'Ч':'Ch','ч':'ch','Ш':'Sh','ш':'sh','Щ':'Shch','щ':'shch',
        'Ы':"Y'",'ы':"y'",'Э':"E'",'э':"e'",'Ю':'Yu','ю':'yu','Я':'Ya','я':'ya'
    }
    return ''.join(cyrillic_to_latin.get(ch, ch) for ch in text)

def transliterate(text: str, direction: str='lat2cyr') -> str:
    if direction == 'lat2cyr':
        return transliterate_latin_to_cyrillic(text)
    elif direction == 'cyr2lat':
        return transliterate_cyrillic_to_latin(text)
    return text

# --- Основной код ---
# Ваш словарь марок и моделей (пример)
car_brands_models: Dict[str, str] = {
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
        with open(filename, "w", encoding='utf-8') as f:
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
                loaded_dict = {str(k): str(v) for k, v in zip(df.iloc[:,0].astype(str), df.iloc[:,1].astype(str))}
        elif filename_lower.endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(data_bytes), engine='openpyxl')
            if len(df.columns) >=2:
                loaded_dict = {str(k): str(v) for k, v in zip(df.iloc[:,0].astype(str), df.iloc[:,1].astype(str))}
        if loaded_dict:
            car_brands_models.update(loaded_dict)
            save_dictionary_to_file(car_brands_models)
            st.success("Словарь обновлён из файла и сохранён.")
    except Exception as e:
        st.error(f"Ошибка при загрузке файла: {e}")

# Обновлённая функция для обработки текста:
# Требование: сначала ищется по словарю, затем по транслиту.
def process_text(text: str, dict_brands_models: dict, translit_enabled: bool) -> str:
    if not text:
        return text
    original = text
    seg = text

    # prepare dictionary maps (lowercase)
    dict_map = {k.lower(): v for k, v in dict_brands_models.items()}

    # 1) Replace occurrences of dictionary keys (case-insensitive).
    if dict_map:
        keys_sorted = sorted(dict_map.keys(), key=len, reverse=True)
        pattern = r'(' + '|'.join(re.escape(k) for k in keys_sorted) + r')'
        regex = re.compile(pattern, flags=re.IGNORECASE)
        def repl_dict(m):
            key = m.group(0).lower()
            return dict_map.get(key, m.group(0))
        seg = regex.sub(repl_dict, seg)

    # 2) If enabled, try transliterated keys: transliterate each dict key (lat->cyr),
    # build map from translit(key).lower() -> value, replace occurrences.
    if translit_enabled and dict_map:
        translit_map = {}
        for k, v in dict_brands_models.items():
            tk = transliterate(k, 'lat2cyr').lower()
            if tk and tk not in translit_map:
                translit_map[tk] = v
        if translit_map:
            keys_t = sorted(translit_map.keys(), key=len, reverse=True)
            pattern_t = r'(' + '|'.join(re.escape(k) for k in keys_t) + r')'
            regex_t = re.compile(pattern_t, flags=re.IGNORECASE)
            def repl_trans(m):
                key = m.group(0).lower()
                return translit_map.get(key, m.group(0))
            seg = regex_t.sub(repl_trans, seg)

    # Возвращаем формат: "исходный - (обработанный)"
    return f"{original} - ({seg})"

# Обработка файла для поиска и обработки строк
def process_file_for_processing(file_bytes, filename, col_name, dict_brands_models, translit_enabled):
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.xlsx', '.xls']:
        df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    elif ext == '.csv':
        try:
            df = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')))
        except:
            df = pd.read_csv(io.StringIO(file_bytes.decode('cp1251', errors='replace')))
    else:
        df = pd.DataFrame()

    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден в файле.")
    series = df[col_name].astype(str).fillna("")

    result_list = [process_text(txt, dict_brands_models, translit_enabled) for txt in series]
    df_result = df.copy()
    df_result[col_name] = result_list
    return df_result

# Streamlit интерфейс (как в вашем примере)
def run():
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")
    st.markdown("<h3>Обработка брендов/моделей</h3>", unsafe_allow_html=True)

    uploaded_dict_file = st.file_uploader("Загрузить файл словаря (JSON, CSV или XLSX)", type=["json", "csv", "xlsx"])
    if uploaded_dict_file:
        update_dict_from_uploaded_file(uploaded_dict_file)

    dict_text = "\n".join([f"{k},{v}" for k, v in car_brands_models.items()])
    edited_text = st.text_area("Редактировать словарь (каждая строка: латиница,кириллица)", value=dict_text, height=200)
    if st.button("Сохранить словарь"):
        new_dict = {}
        for line in edited_text.splitlines():
            if line.strip():
                parts = line.split(",", 1)
                if len(parts) == 2:
                    k, v = parts
                    new_dict[k.strip()] = v.strip()
        if new_dict:
            car_brands_models.clear()
            car_brands_models.update(new_dict)
            save_dictionary_to_file(car_brands_models)
            st.success("Словарь сохранён.")

    translit_enabled = st.checkbox("Искать и по транслиту (лат→кир)", value=True)

    uploaded_file = st.file_uploader("Загрузите Excel или CSV файл", type=["xlsx", "xls", "csv"])
    if uploaded_file:
        try:
            file_bytes = uploaded_file.read()
            ext = os.path.splitext(uploaded_file.name)[1].lower()
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
