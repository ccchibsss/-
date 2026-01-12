import streamlit as st
import pandas as pd
import io
import re
import json
import difflib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

# Глобальные переменные
car_brands_models: dict = {}
history_stack: list = []

# --- Функции сохранения/загрузки ---
def save_dictionary(format: str = 'xlsx'):
    df = pd.DataFrame({
        'Ключ': list(car_brands_models.keys()),
        'Название': list(car_brands_models.values())
    })
    buf = io.BytesIO()
    if format == 'xlsx':
        df.to_excel(buf, index=False)
        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = "dictionary.xlsx"
    elif format == 'csv':
        df.to_csv(buf, index=False)
        mime_type = "text/csv"
        filename = "dictionary.csv"
    elif format == 'json':
        json.dump(car_brands_models, buf)
        buf.seek(0)
        mime_type = "application/json"
        filename = "dictionary.json"
    buf.seek(0)
    st.download_button("Скачать словарь", buf, file_name=filename, mime=mime_type)

def load_dictionary(file):
    global car_brands_models
    try:
        if file.name.endswith('.xlsx'):
            df = pd.read_excel(file)
        elif file.name.endswith('.csv'):
            df = pd.read_csv(file)
        elif file.name.endswith('.json'):
            data = json.load(file)
            if isinstance(data, dict):
                car_brands_models = data
                return
            else:
                st.error("Некорректный формат JSON")
                return
        else:
            st.error("Неподдерживаемый формат файла")
            return
        new_dict = {}
        for _, row in df.iterrows():
            key = str(row.get('Ключ', '')).strip()
            value = str(row.get('Название', '')).strip()
            if key and value:
                new_dict[key] = value
        car_brands_models = new_dict
        st.success("Словарь успешно загружен")
    except Exception as e:
        st.error(f"Ошибка загрузки: {e}")

# --- История ---
def save_state():
    global history_stack
    history_stack.append(car_brands_models.copy())

def restore_state():
    global car_brands_models, history_stack
    if history_stack:
        car_brands_models = history_stack.pop()
        st.success("Восстановлено предыдущее состояние")
    else:
        st.info("История пуста")

# --- Добавление / удаление ---
def add_entry():
    key = st.text_input("Ключ")
    value = st.text_input("Значение")
    if st.button("Добавить пару") and key and value:
        car_brands_models[key.strip()] = value.strip()
        st.success("Пара добавлена")

def delete_entry():
    keys = list(car_brands_models.keys())
    key_to_delete = st.selectbox("Выберите для удаления", keys)
    if st.button("Удалить") and key_to_delete:
        del car_brands_models[key_to_delete]
        st.success("Пара удалена")

# --- ML для автоматического расширения словаря ---
def train_ml_model():
    keys = list(car_brands_models.keys())
    if len(keys) < 2:
        st.warning("Недостаточно данных для обучения модели")
        return None, None
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(keys)
    kmeans = KMeans(n_clusters=min(3, len(keys)//2+1))
    kmeans.fit(X)
    return vectorizer, kmeans

def suggest_new_keys(texts, vectorizer, kmeans, threshold=0.5):
    suggestions = []
    if not vectorizer or not kmeans:
        return suggestions
    for text in texts:
        vec = vectorizer.transform([text])
        label = kmeans.predict(vec)[0]
        # Можно добавить более сложную логику, например, сравнение схожести
        suggestions.append(text)
    return suggestions

# --- Основной интерфейс ---
def main():
    st.set_page_config(page_title="Расширенное управление словарём", layout="wide")
    st.title("Расширенное управление словарём — функции ML и API")

    # --- Импорт/Экспорт ---
    with st.sidebar:
        st.header("Импорт/Экспорт")
        uploaded_file = st.file_uploader("Загрузить файл словаря", type=["xlsx", "csv", "json"])
        if uploaded_file:
            load_dictionary(uploaded_file)
        if st.button("Сохранить как Excel"):
            save_dictionary('xlsx')
        if st.button("Сохранить как CSV"):
            save_dictionary('csv')
        if st.button("Сохранить как JSON"):
            save_dictionary('json')

    # --- Управление пар ---
    st.subheader("Добавление / Удаление пар")
    add_entry()
    delete_entry()

    # --- История ---
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Сделать снимок состояния"):
            save_state()
    with col2:
        if st.button("Восстановить предыдущее состояние"):
            restore_state()

    # --- ML для автоматического расширения ---
    st.subheader("Автоматическое расширение словаря (ML)")
    ml_vectorizer, ml_kmeans = train_ml_model()
    sample_texts = st.text_area("Введите тестовые слова или фразы через запятую")
    if st.button("Предложить новые ключи") and sample_texts:
        texts_list = [t.strip() for t in sample_texts.split(',')]
        suggestions = suggest_new_keys(texts_list, ml_vectorizer, ml_kmeans)
        st.write("Предложенные ключи для добавления:")
        for s in suggestions:
            st.write(f"- {s}")

    # --- Мультиязычность и API ---
    st.subheader("Поддержка мультиязычности и внешние API")
    lang = st.selectbox("Выберите язык текста", ["ru", "en", "de", "fr", "zh"])
    text_to_process = st.text_area("Введите текст для обработки")
    if st.button("Обработать текст") and text_to_process:
        processed_text = text_to_process
        # Обработка с помощью словаря
        for key, val in car_brands_models.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            processed_text = pattern.sub(val, processed_text)
        st.write("Обработанный текст:")
        st.write(processed_text)
        # Можно расширить — добавлять вызовы внешних API для перевода и т.п.

    # --- Поиск и схожесть ---
    st.subheader("Поиск по словарю и схожести")
    search_term = st.text_input("Введите термин для поиска")
    if st.button("Найти") and search_term:
        matches = []
        for key, val in car_brands_models.items():
            ratio = difflib.SequenceMatcher(None, search_term, key).ratio()
            if ratio > 0.6:
                matches.append((key, val, ratio))
        if matches:
            st.write("Найденные совпадения:")
            for k, v, r in sorted(matches, key=lambda x: x[2], reverse=True):
                st.write(f"**{k}** : {v} (схожесть: {r:.2f})")
        else:
            st.write("Совпадений не найдено.")

    # --- Обработка мультиязычного текста (пример) ---
    st.subheader("Обработка текста с определением языка")
    detect_text = st.text_area("Введите текст для определения языка")
    if st.button("Определить язык") and detect_text:
        # Можно подключить API или использовать библиотеки типа langdetect
        try:
            from langdetect import detect
            lang_code = detect(detect_text)
            st.write(f"Определен язык: {lang_code}")
        except ImportError:
            st.error("Библиотека langdetect не установлена. Установите через pip install langdetect.")

if __name__ == "__main__":
    main()
