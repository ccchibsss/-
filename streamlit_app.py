"""
Улучшенная и исправленная версия скрипта обработки названий автомобилей.

Исправления/улучшения:
- Добавлен импорт json и проверка при загрузке/сохранении дополнительных записей.
- Загруженные дополнительные пары объединяются с основным словарём.
- Исправлена опечатка "Fabiа" -> "Fabia".
- При подготовке расширений (candidates) now additions map values -> русские имена (если найдены).
- Более надёжное чтение внешних файлов/URL и сохранение добавлений.
- Скрипт работает как с Streamlit (если установлен), так и в CLI режиме.
- Небольшие проверки и обработка ошибок.
"""

from __future__ import annotations
import io
import re
import sys
import argparse
import requests
import pandas as pd
from difflib import SequenceMatcher
from functools import lru_cache
import os
import json
from typing import Optional

# Попытка импортировать streamlit (необязательно)
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None

# Попытка инициализировать pymorphy2 (необязательно)
try:
    import pymorphy2  # type: ignore
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

# Путь к файлу для хранения пользовательских добавлений
ADDITIONS_FILE = "additional_brands.json"

# Исходный словарь марок и моделей (исправлена опечатка "Fabia")
car_brands_models = {
    # Немецкие
    "BMW": "БМВ", "1 Series": "1 Серия", "2 Series": "2 Серия", "3 Series": "3 Серия",
    "4 Series": "4 Серия", "5 Series": "5 Серия", "6 Series": "6 Серия", "7 Series": "7 Серия",
    "8 Series": "8 Серия", "X1": "Икс 1", "X2": "Икс 2", "X3": "Икс 3", "X4": "Икс 4",
    "X5": "Икс 5", "X6": "Икс 6", "X7": "Икс 7", "Z4": "Зет 4", "M3": "Эм 3", "M5": "Эм 5",
    "M Series": "Эм Серия", "Mercedes-Benz": "Мерседес-Бенц", "A-Class": "А-Класс",
    "B-Class": "Б-Класс", "C-Class": "С-Класс", "E-Class": "Е-Класс", "S-Class": "Си-Класс",
    "GLC": "ГЛЦ", "GLE": "ГЛЕ", "GLS": "ГЛС", "G-Class": "Г-Класс", "CLS": "ЦЛС",
    "Vito": "Вито", "Sprinter": "Спритер",
    # Япония
    "Toyota": "Тойота", "Corolla": "Королла", "Camry": "Камри", "RAV4": "Рав 4", "Prius": "Приус",
    "Land Cruiser": "Ленд Крузер", "Yaris": "Ярис", "Highlander": "Хайлендер", "Hilux": "Хайлюкс",
    "Sienta": "Сента", "Avensis": "Авенсис",
    "Mazda": "Мазда", "Mazda3": "Мазда 3", "Mazda6": "Мазда 6", "CX-3": "Кс 3", "CX-5": "Кс 5",
    "CX-9": "Кс 9", "MX-5": "МХ 5", "Subaru": "Субару", "Impreza": "Импреза", "Forester": "Форестер",
    "Outback": "Аутбек", "XV": "Икс ВИ",
    # Корея
    "Kia": "Киа", "Rio": "Рио", "Ceed": "Сид", "Sportage": "Спортейдж", "Sorento": "Соренто",
    "Soul": "Соул", "Optima": "Оптима", "Carnival": "Карнавал", "Stinger": "Стингер",
    "Hyundai": "Хёндай", "Elantra": "Элантра", "Sonata": "Соната", "Tucson": "Тусон",
    "Santa Fe": "Санта Фе", "Kona": "Кона", "Veloster": "Велюстер",
    # Китай
    "BYD": "БайДжи", "Han": "Хан", "Tang": "Танг", "Song": "Сонг", "Dolphin": "Дельфин",
    "F3": "Ф3", "F7": "Ф7", "Geely": "Джили", "Atlas": "Атлас", "Tiggo": "Тигго",
    "Coolray": "Кулрэй", "Emgrand": "Эмгранд", "Binrui": "Бинрай", "Chery": "Черри",
    "Tiggo 7": "Тигго 7", "Arrizo": "Аризо", "Exeed": "Эксид", "JAC": "Джак", "Refine": "Рефайн",
    "S2": "Эс 2", "S3": "Эс 3", "Megan": "Меган", "Lifan": "Лифан", "Baojun": "Баоцзюнь",
    "Hongqi": "Хунци", "FAW": "Фав", "Bestune": "Бестюн", "Levdeo": "Левдео", "Wey": "Вей",
    "Yema": "Йема",
    # Русские
    "Lada": "Лада", "Vesta": "Веста", "Granta": "Гранта", "Kalina": "Калина", "Niva": "Нива",
    "UAZ": "УАЗ", "Gaz": "Газ", "ZAZ": "Заз", "Vaz": "Ваз", "Lada Priora": "Лада Приора",
    "Lada 4x4": "Лада 4х4", "Lada XRay": "Лада Xray",
    # Европа
    "Audi": "Ауди", "A1": "А1", "A3": "А3", "A4": "А4", "A6": "А6", "A8": "А8",
    "Q3": "Кью 3", "Q5": "Кью 5", "Q7": "Кью 7", "Q8": "Кью 8", "RS3": "Эр Эс 3",
    "RS5": "Эр Эс 5", "TT": "ТТ", "Volkswagen": "Фольксваген", "Golf": "Гольф",
    "Passat": "Пассат", "Tiguan": "Тигуан", "Touareg": "Туарег", "Jetta": "Джетта",
    "Arteon": "Артеон", "Skoda": "Шкода", "Octavia": "Октавия", "Superb": "Суперб",
    "Kodiaq": "Кодьяк", "Karoq": "Кароак", "Fabia": "Фабия", "Yeti": "Йети",
    # Америка
    "Ford": "Форд", "Fiesta": "Фиеста", "Focus": "Фокус", "Mustang": "Мустанг",
    "Ranger": "Рейнджер", "Bronco": "Бронко", "Chevrolet": "Шевроле", "Aveo": "Авео",
    "Lacetti": "Лачетти", "Malibu": "Мальбу", "Trailblazer": "Трейлблейзер",
    "Tahoe": "Тахо", "Silverado": "Сильверадо",
    # Франция/Италия/Другое
    "Peugeot": "Пежо", "208": "208", "308": "308", "508": "508", "3008": "3008",
    "5008": "5008", "Expert": "Эксперт", "Renault": "Рено", "Clio": "Клио", "Megane": "Меган",
    "Captur": "Каптюр", "Kangoo": "Кангру", "Koleos": "Колеос", "Duster": "Дастер",
    "Logan": "Логан", "Sandero": "Сандеро", "Fiat": "Фиат", "Panda": "Панда", "500": "500",
    "Tipo": "Типо", "Lancia": "Ланча", "Alfa Romeo": "Альфа Ромео", "Giulia": "Джулия",
    "Stelvio": "Стельвио", "Suzuki": "Сузуки", "Honda": "Хонда", "Dacia": "Дачия",
    "SsangYong": "СангЁнг",
}

# Загружаем пользовательские добавления (если файл есть), объединяем со словарём
added_pairs: dict = {}
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                added_pairs = loaded
                # Приводим ключи/значения к str и обновляем основной словарь
                cleaned = {str(k): str(v) for k, v in added_pairs.items()}
                car_brands_models.update(cleaned)
            else:
                print(f"Предупреждение: {ADDITIONS_FILE} не содержит словарь, пропускаю.")
    except Exception as e:
        print("Ошибка при загрузке дополнительных добавлений:", e)

def save_additions() -> None:
    try:
        with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): str(v) for k, v in added_pairs.items()}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("Ошибка при сохранении дополнительных добавлений:", e)

# ---------------------------
# Склонение слов с кешированием
# ---------------------------
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

# ---------------------------
# Транслитерация: латиница -> кириллица
# ---------------------------
LAT_TO_CYR_RULES = [
    ("shch", "щ"), ("sch", "щ"), ("sht", "шт"),
    ("oye", "ое"), ("oyu", "ою"), ("iya", "ия"), ("iye", "ие"),
    ("aye", "ая"), ("ayu", "аю"), ("eyu", "ею"), ("iu", "ю"),
    ("ia", "ия"), ("yo", "ё"), ("yu", "ю"), ("ya", "я"),
    ("zh", "ж"), ("kh", "х"), ("ts", "ц"), ("ch", "ч"),
    ("sh", "ш"), ("ye", "е"), ("ja", "я"), ("ju", "ю"),
    ("je", "е"),
    ("a", "а"), ("b", "б"), ("v", "в"), ("g", "г"), ("d", "д"),
    ("e", "е"), ("z", "з"), ("i", "и"), ("k", "к"), ("l", "л"),
    ("m", "м"), ("n", "н"), ("o", "о"), ("p", "п"), ("r", "р"),
    ("s", "с"), ("t", "т"), ("u", "у"), ("f", "ф"), ("y", "ы"),
    ("j", "й"), ("'", "ь"), ('"', "ъ"), ("x", "кс"), ("q", "к"), ("w", "в"),
]

def latin_to_cyrillic(text: str) -> str:
    if not isinstance(text, str) or not text:
        return text
    def translit_word(word: str) -> str:
        lower = word.lower()
        i = 0
        out = ""
        while i < len(lower):
            matched = False
            for lat, cyr in LAT_TO_CYR_RULES:
                if lower.startswith(lat, i):
                    out += cyr
                    i += len(lat)
                    matched = True
                    break
            if not matched:
                out += lower[i]
                i += 1
        if word.isupper():
            return out.upper()
        elif word[0].isupper():
            return out.capitalize()
        return out
    parts = re.split(r'(\s+)', text)
    result = []
    for p in parts:
        if re.search(r'[A-Za-z]', p):
            sub = re.split(r'([^A-Za-z]+)', p)
            for s in sub:
                if re.search(r'[A-Za-z]', s):
                    result.append(translit_word(s))
                else:
                    result.append(s)
        else:
            result.append(p)
    return "".join(result)

def contains_latin(text: str) -> bool:
    return bool(re.search(r'[A-Za-z]', str(text)))

def contains_cyrillic(text: str) -> bool:
    return bool(re.search(r'[\u0400-\u04FF]', str(text)))

# ---------------------------
# Вспомогательные: поиск похожих слов
# ---------------------------
def find_similar_word(word: str, keys_lower: list, keys_map: dict, threshold: float = 0.8) -> Optional[str]:
    w = (word or "").lower()
    best_ratio = 0.0
    best_key_lower = None
    for k_lower in keys_lower:
        ratio = SequenceMatcher(None, w, k_lower).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_key_lower = k_lower
    if best_ratio >= threshold and best_key_lower is not None:
        return keys_map[best_key_lower]
    return None

# ---------------------------
# Создаём regex из ключей
# ---------------------------
def build_regex_from_keys(keys: list[str]):
    if not keys:
        return None
    escaped = sorted((re.escape(k) for k in keys), key=len, reverse=True)
    pattern = r'(?<!\w)(?:' + "|".join(escaped) + r')(?!\w)'
    return re.compile(pattern, flags=re.IGNORECASE)

# ---------------------------
# Извлечение слов из Series
# ---------------------------
def extract_words_from_series(series: pd.Series) -> set:
    if series is None:
        return set()
    all_text = series.dropna().astype(str).str.cat(sep=' ')
    return set(re.findall(r'[A-Za-zА-Яа-я0-9\-_/\.]+', all_text))

def prepare_additions(base_keys: set, candidates: set, threshold: float = 0.85) -> dict:
    additions = {}
    keys_map = {k.lower(): k for k in base_keys}
    keys_lower = list(keys_map.keys())
    for cand in candidates:
        if cand in base_keys:
            continue
        sim = find_similar_word(cand, keys_lower, keys_map, threshold=threshold)
        if sim:
            # добавляем candidate -> русское название найденного похожего ключа (если есть)
            additions[cand] = car_brands_models.get(sim, sim)
    return additions

# ---------------------------
# Обработка текста
# ---------------------------
def process_text(text: str, base_dict: dict, additions_map: dict, translit_allowed: bool = True) -> str:
    if not isinstance(text, str):
        return text
    if translit_allowed and contains_latin(text) and not contains_cyrillic(text):
        cyr = latin_to_cyrillic(text)
        return f"{text} ({decline_word_cached(cyr)})"
    final_map = {**base_dict, **additions_map}
    pattern = build_regex_from_keys(list(final_map.keys()))
    if not pattern:
        return text
    def repl(m):
        found = m.group(0)
        # поиск ключа без учёта регистра
        for k in final_map:
            if k.lower() == found.lower():
                ru = final_map[k] or k
                return f"{found} ({decline_word_cached(ru)})"
        return found
    return pattern.sub(repl, text)

# ---------------------------
# Загрузка внешних данных
# ---------------------------
def load_external_data(url: str) -> pd.DataFrame:
    if not url:
        return pd.DataFrame()
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "").lower()
        if "text/csv" in ct or url.lower().endswith(".csv"):
            return pd.read_csv(io.StringIO(resp.text))
        try:
            # попытаться как Excel
            return pd.read_excel(io.BytesIO(resp.content))
        except Exception:
            # fallback CSV
            return pd.read_csv(io.StringIO(resp.text))
    except Exception as e:
        if st:
            st.error(f"Ошибка загрузки внешнего URL: {e}")
        else:
            print("Ошибка загрузки внешнего URL:", e)
        return pd.DataFrame()

# ---------------------------
# Вывод списка ключей
# ---------------------------
def list_brands_models() -> None:
    print("Всего записей в словаре:", len(car_brands_models))
    print("Ключи (бренд/модель):")
    for k in sorted(car_brands_models.keys()):
        print(" -", k, "→", car_brands_models[k])
    print("\nЧтобы добавить: car_brands_models['NewKey'] = 'НоваяРусскаяВерсия'")

# ---------------------------
# Обработка файла в CLI
# ---------------------------
def process_file(input_path: str, column: str, external_url: Optional[str], output_path: Optional[str]) -> None:
    try:
        if input_path.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(input_path)
        else:
            df = pd.read_csv(input_path)
    except Exception as e:
        print("Ошибка чтения файла:", e)
        return
    if column not in df.columns:
        print("Столбец не найден. Доступные столбцы:", df.columns.tolist())
        return
    external_df = load_external_data(external_url) if external_url else pd.DataFrame()
    series = df[column]
    dataset_words = extract_words_from_series(series)
    external_words = extract_words_from_series(external_df.stack()) if not external_df.empty else set()
    base_keys = set(car_brands_models.keys())
    candidates = (dataset_words | external_words) - base_keys
    additions = prepare_additions(base_keys, candidates, threshold=0.85)
    print(f"Кандидатов для добавления: {len(additions)}")
    if additions:
        # сохраняем найденные кандидаты в локальный набор (как автодобавления)
        added_pairs.update({k: v for k, v in additions.items()})
        car_brands_models.update(additions)
        save_additions()
    df[column] = df[column].fillna("").astype(str).apply(lambda v: process_text(v, car_brands_models, additions))
    if not output_path:
        output_path = "result.xlsx" if input_path.lower().endswith(('.xls', '.xlsx')) else "result.csv"
    try:
        if output_path.lower().endswith(('.xls', '.xlsx')):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)
        print("Результат сохранён в:", output_path)
    except Exception as e:
        print("Ошибка сохранения:", e)

# ---------------------------
# Streamlit интерфейс
# ---------------------------
def run_streamlit_app() -> None:
    st.set_page_config(page_title="Автообработка расширенная", layout="wide")
    st.title("Обработка названий автомобилей — улучшенная версия")
    st.markdown("Загрузите файл (CSV/XLSX), укажите URL для расширения словаря.")
    uploaded_file = st.file_uploader("Файл", type=["xlsx", "xls", "csv"])
    external_url = st.text_input("URL внешнего источника (CSV / XLSX) — необязательно")
    if uploaded_file:
        try:
            if uploaded_file.name.lower().endswith(('.xls', '.xlsx')):
                df = pd.read_excel(uploaded_file)
            else:
                df = pd.read_csv(uploaded_file)
        except Exception as e:
            st.error(f"Ошибка чтения файла: {e}")
            return
        col = st.selectbox("Столбец для обработки", df.columns.tolist())

        # Добавление новых пар через sidebar
        st.sidebar.header("Добавить новый бренд/модель")
        new_key = st.sidebar.text_input("Ключ (бренд/модель)")
        new_value = st.sidebar.text_input("Русское название или описание")
        if st.sidebar.button("Добавить в словарь"):
            if new_key and new_value:
                car_brands_models[new_key] = new_value
                added_pairs[new_key] = new_value
                save_additions()
                st.sidebar.success(f"Добавлено: {new_key} → {new_value}")
            else:
                st.sidebar.error("Введите оба поля для добавления.")

        if st.button("Показать/добавить список словаря"):
            st.write(f"Всего ключей: {len(car_brands_models)}")
            for k in sorted(car_brands_models.keys()):
                st.write(f"- {k} → {car_brands_models[k]}")

        if st.button("Обработать"):
            external_df = load_external_data(external_url) if external_url else pd.DataFrame()
            series = df[col]
            dataset_words = extract_words_from_series(series)
            external_words = extract_words_from_series(external_df.stack()) if not external_df.empty else set()
            base_keys = set(car_brands_models.keys())
            candidates = (dataset_words | external_words) - base_keys
            additions = prepare_additions(base_keys, candidates, threshold=0.85)
            if additions:
                st.success(f"Найдено {len(additions)} кандидатов — добавлено локально.")
                # сохраняем
                added_pairs.update({k: v for k, v in additions.items()})
                car_brands_models.update(additions)
                save_additions()
            else:
                st.info("Новые кандидаты не найдены.")
            df[col] = df[col].fillna("").astype(str).apply(lambda v: process_text(v, car_brands_models, additions))
            st.subheader("Результат (первые 200 строк)")
            st.dataframe(df.head(200))
            fmt = st.radio("Формат экспорта", ["CSV", "Excel"])
            if fmt == "Excel":
                buf = io.BytesIO()
                df.to_excel(buf, index=False)
                buf.seek(0)
                st.download_button("Скачать Excel", buf, "result.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
                st.download_button("Скачать CSV", csv_bytes, "result.csv", mime="text/csv")

# ---------------------------
# Запуск
# ---------------------------
def main() -> None:
    if st:
        run_streamlit_app()
        return
    parser = argparse.ArgumentParser(description="Обработка названий автомобилей")
    parser.add_argument("--input", "-i", required=False, help="Входной файл CSV/XLSX")
    parser.add_argument("--column", "-c", required=False, help="Имя столбца для обработки")
    parser.add_argument("--external", "-e", required=False, help="URL внешнего CSV/XLSX")
    parser.add_argument("--output", "-o", required=False, help="Путь для сохранения результата")
    parser.add_argument("--list", action="store_true", help="Вывести список ключей словаря и выйти")
    args = parser.parse_args()
    if args.list:
        list_brands_models()
        return
    if not args.input or not args.column:
        print("Запуск в CLI. Укажите --input и --column, или --list для просмотра словаря.")
        print("Пример: python script.py --input data.csv --column description --output result.csv")
        return
    process_file(args.input, args.column, args.external, args.output)

if __name__ == "__main__":
    main()
