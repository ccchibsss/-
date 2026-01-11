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

ADDITIONS_FILE = "additional_brands.json"

# ---------------------------
# Базовый словарь брендов/моделей
# ---------------------------
car_brands_models = {
    "BMW": "БМВ", "1 Series": "1 Серия", "2 Series": "2 Серия", "3 Series": "3 Серия",
    "4 Series": "4 Серия", "5 Series": "5 Серия", "6 Series": "6 Серия", "7 Series": "7 Серия",
    "8 Series": "8 Серия", "X1": "Икс 1", "X2": "Икс 2", "X3": "Икс 3", "X4": "Икс 4",
    "X5": "Икс 5", "X6": "Икс 6", "X7": "Икс 7", "Z4": "Зет 4", "M3": "Эм 3", "M5": "Эм 5",
    "M Series": "Эм Серия", "Mercedes-Benz": "Мерседес-Бенц", "A-Class": "А-Класс",
    "B-Class": "Б-Класс", "C-Class": "С-Класс", "E-Class": "Е-Класс", "S-Class": "Си-Класс",
    "GLC": "ГЛЦ", "GLE": "ГЛЕ", "GLS": "ГЛС", "G-Class": "Г-Класс", "CLS": "ЦЛС",
    "Vito": "Вито", "Sprinter": "Спритер",
    "Toyota": "Тойота", "Corolla": "Королла", "Camry": "Камри", "RAV4": "Рав 4", "Prius": "Приус",
    "Land Cruiser": "Ленд Крузер", "Yaris": "Ярис", "Highlander": "Хайлендер", "Hilux": "Хайлюкс",
    "Sienta": "Сента", "Avensis": "Авенсис",
    "Mazda": "Мазда", "Mazda3": "Мазда 3", "Mazda6": "Мазда 6", "CX-3": "Кс 3", "CX-5": "Кс 5",
    "CX-9": "Кс 9", "MX-5": "МХ 5", "Subaru": "Субару", "Impreza": "Импреза", "Forester": "Форестер",
    "Outback": "Аутбек", "XV": "Икс ВИ",
    "Kia": "Киа", "Rio": "Рио", "Ceed": "Сид", "Sportage": "Спортейдж", "Sorento": "Соренто",
    "Soul": "Соул", "Optima": "Оптима", "Carnival": "Карнавал", "Stinger": "Стингер",
    "Hyundai": "Хёндай", "Elantra": "Элантра", "Sonata": "Соната", "Tucson": "Тусон",
    "Santa Fe": "Санта Фе", "Kona": "Кона", "Veloster": "Велюстер",
    "BYD": "БайДжи", "Han": "Хан", "Tang": "Танг", "Song": "Сонг", "Dolphin": "Дельфин",
    "F3": "Ф3", "F7": "Ф7", "Geely": "Джили", "Atlas": "Атлас", "Tiggo": "Тигго",
    "Coolray": "Кулрэй", "Emgrand": "Эмгранд", "Binrui": "Бинрай", "Chery": "Черри",
    "Tiggo 7": "Тигго 7", "Arrizo": "Аризо", "Exeed": "Эксид", "JAC": "Джак", "Refine": "Рефайн",
    "S2": "Эс 2", "S3": "Эс 3", "Megan": "Меган", "Lifan": "Лифан", "Baojun": "Баоцзюнь",
    "Hongqi": "Хунци", "FAW": "Фав", "Bestune": "Бестюн", "Levdeo": "Левдео", "Wey": "Вей",
    "Yema": "Йема",
    "Lada": "Лада", "Vesta": "Веста", "Granta": "Гранта", "Kalina": "Калина", "Niva": "Нива",
    "UAZ": "УАЗ", "Gaz": "Газ", "ZAZ": "Заз", "Vaz": "Ваз", "Lada Priora": "Лада Приора",
    "Lada 4x4": "Лада 4х4", "Lada XRay": "Лада Xray",
    "Audi": "Ауди", "A1": "А1", "A3": "А3", "A4": "А4", "A6": "А6", "A8": "А8",
    "Q3": "Кью 3", "Q5": "Кью 5", "Q7": "Кью 7", "Q8": "Кью 8", "RS3": "Эр Эс 3",
    "RS5": "Эр Эс 5", "TT": "ТТ", "Volkswagen": "Фольксваген", "Golf": "Гольф",
    "Passat": "Пассат", "Tiguan": "Тигуан", "Touareg": "Туарег", "Jetta": "Джетта",
    "Arteon": "Артеон", "Skoda": "Шкода", "Octavia": "Октавия", "Superb": "Суперб",
    "Kodiaq": "Кодьяк", "Karoq": "Кароак", "Fabia": "Фабия", "Yeti": "Йети",
    "Ford": "Форд", "Fiesta": "Фиеста", "Focus": "Фокус", "Mustang": "Мустанг",
    "Ranger": "Рейнджер", "Bronco": "Бронко", "Chevrolet": "Шевроле", "Aveo": "Авео",
    "Lacetti": "Лачетти", "Malibu": "Мальбу", "Trailblazer": "Трейлблейзер",
    "Tahoe": "Тахо", "Silverado": "Сильверадо",
    "Peugeot": "Пежо", "208": "208", "308": "308", "508": "508", "3008": "3008",
    "5008": "5008", "Expert": "Эксперт", "Renault": "Рено", "Clio": "Клио", "Megane": "Меган",
    "Captur": "Каптюр", "Kangoo": "Кангру", "Koleos": "Колеос", "Duster": "Дастер",
    "Logan": "Логан", "Sandero": "Сандеро", "Fiat": "Фиат", "Panda": "Панда", "500": "500",
    "Tipo": "Типо", "Lancia": "Ланча", "Alfa Romeo": "Альфа Ромео", "Giulia": "Джулия",
    "Stelvio": "Стельвио", "Suzuki": "Сузуки", "Honda": "Хонда", "Dacia": "Дачия",
    "SsangYong": "СангЁнг",
}

# ---------------------------
# Загрузка пользовательских добавлений (если есть)
# ---------------------------
added_pairs: dict = {}
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                added_pairs = {str(k): str(v) for k, v in loaded.items()}
                car_brands_models.update(added_pairs)
            else:
                print(f"Предупреждение: {ADDITIONS_FILE} не содержит словарь, пропускаю.")
    except Exception as e:
        print("Ошибка при загрузке дополнительных добавлений:", e)

# ---------------------------
# Дополнительные расширения (большой словарь — можно дополнить)
# ---------------------------
EXTENDED_BRANDS_ADDITIONS = {
    # Японские / американские / европейские / китайские — выборка популярных
    "Nissan": "Ниссан", "Altima": "Альтима", "Sentra": "Сентра", "Maxima": "Максима",
    "Rogue": "Роудж", "X-Trail": "Икс-Трэйл", "Qashqai": "Кашкай", "Leaf": "Лиф",
    "Titan": "Титан", "Navara": "Навара", "Patrol": "Патрол", "Murano": "Муранo",
    "Avalon": "Эвалон", "Venza": "Венза", "C-HR": "C-HR", "Tacoma": "Такома", "Tundra": "Тундра",
    "Accord": "Акорд", "Civic": "Сивик", "Fit": "Фит", "Jazz": "Джаз",
    "CR-V": "CR-V", "HR-V": "HR-V", "Pilot": "Пилот", "Odyssey": "Одиссея",
    "Mazda2": "Мазда 2", "Mazda CX-30": "Мазда CX-30", "MX-30": "Мазда MX-30",
    "Legacy": "Легаси", "BRZ": "BRZ", "Crosstrek": "Кросстрек",
    "L200": "L200", "ASX": "ASX", "Eclipse Cross": "Иклепс Кросс",
    "Swift": "Свифт", "Ignis": "Игнис", "Vitara": "Витара",
    "Seltos": "Селтос", "Stonic": "Стонік",
    "i30": "i30", "i20": "i20", "Palisade": "Палисад", "Kona Electric": "Кона Электрик",
    "i4": "i4", "iX": "iX",

    "Polestar": "Полистар", "Polestar 2": "Полистар 2",
    "Polestar 3": "Полистар 3", "Lucid": "Лусид", "Air": "Эйр",
    "Rivian": "Ривиан", "R1T": "R1T", "NIO": "Нио", "ES6": "ES6", "ES7": "ES7", "XPeng": "ХПэнг", "P7": "P7",
    "Tesla": "Тесла", "Model S": "Модель S", "Model 3": "Модель 3", "Model X": "Модель X", "Model Y": "Модель Y",

    "Polestar": "Полистар", "Volvo": "Вольво", "S60": "S60", "XC40": "XC40", "XC60": "XC60", "XC90": "XC90",
    "Volkswagen": "Фольксваген", "Polo": "Поло", "T-Roc": "T-Roc",
    "Seat": "Сеат", "Cupra": "Купра", "Renault": "Рено", "Megane": "Меган",
    "Peugeot": "Пежо", "3008": "3008", "508": "508",

    "Audi": "Ауди", "A7": "А7", "Q2": "Q2", "RS6": "RS6",
    "Mercedes": "Мерседес", "CLA": "CLA", "GLA": "GLA", "Maybach": "Майбах",
    "Porsche": "Порше", "911": "911", "Cayman": "Кайман", "Macan": "Макан", "Taycan": "Тайкан",
    "Jaguar": "Ягуар", "Land Rover": "Ленд Ровер", "Range Rover": "Рендж Ровер", "Discovery": "Дискавери",
    "Mini": "Мини", "Cooper": "Купер",
    "Ferrari": "Феррари", "Lamborghini": "Ламборгини", "Huracan": "Уракан", "Urus": "Урус",
    "Maserati": "Мазерати", "Ghibli": "Гибли",

    "Chevrolet": "Шевроле", "Cruze": "Круз", "Equinox": "Экуинокс", "Blazer": "Блейзер",
    "GMC": "ДжиЭмСи", "Sierra": "Сиерра", "Cadillac": "Кадиллак", "Escalade": "Эскадил",
    "Dodge": "Додж", "Challenger": "Челленджер", "Charger": "Чарджер",
    "Jeep": "Джип", "Wrangler": "Рэнглер", "Grand Cherokee": "Гранд Чероки",

    "Great Wall": "Грейт Уолл", "Haval": "Хавал", "Ora": "Ора", "Neta": "Нета",
    "Geely": "Джили", "Coolray": "Кулрэй", "Emgrand": "Эмгранд", "Exeed": "Иксид",
    "Wuling": "Вулинг", "Baojun": "Баоджун", "Roewe": "Роу",
    "Hybrid": "Гибрид", "Plug-in Hybrid": "Подключаемый гибрид", "Electric": "Электро",
}

def merge_extended_brands(base: dict, additions: dict, overwrite: bool = False) -> int:
    """
    Добавляет пары из additions в base.
    Если overwrite=False, существующие ключи не перезаписываются.
    Возвращает количество добавленных записей.
    """
    added = 0
    for k, v in additions.items():
        if k in base:
            if overwrite and base[k] != v:
                base[k] = v
                added += 1
            continue
        base[k] = v
        added += 1
    return added

_added = merge_extended_brands(car_brands_models, EXTENDED_BRANDS_ADDITIONS, overwrite=False)
if _added:
    print(f"[info] Добавлено новых записей в car_brands_models: {_added}")

# ---------------------------
# Сохранение пользовательских добавлений
# ---------------------------
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

def build_regex_from_keys(keys: list[str]):
    if not keys:
        return None
    escaped = sorted((re.escape(k) for k in keys), key=len, reverse=True)
    pattern = r'(?<!\w)(?:' + "|".join(escaped) + r')(?!\w)'
    return re.compile(pattern, flags=re.IGNORECASE)

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
            additions[cand] = car_brands_models.get(sim, sim)
    return additions

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
        for k in final_map:
            if k.lower() == found.lower():
                ru = final_map[k] or k
                return f"{found} ({decline_word_cached(ru)})"
        return found
    return pattern.sub(repl, text)

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
            return pd.read_excel(io.BytesIO(resp.content))
        except Exception:
            return pd.read_csv(io.StringIO(resp.text))
    except Exception as e:
        if st:
            st.error(f"Ошибка загрузки внешнего URL: {e}")
        else:
            print("Ошибка загрузки внешнего URL:", e)
        return pd.DataFrame()

def list_brands_models() -> None:
    print("Всего записей в словаре:", len(car_brands_models))
    print("Ключи (бренд/модель):")
    for k in sorted(car_brands_models.keys()):
        print(" -", k, "→", car_brands_models[k])
    print("\nЧтобы добавить: car_brands_models['NewKey'] = 'НоваяРусскаяВерсия'")

def process_file(input_path: str, column: str, external_url: Optional[str], output_path: Optional[str]) -> None:
    try:
        if input_path.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(input_path)
        else:
            df = pd.read_csv(input_path)
    except Exception as e:
        print("Ошибка чтения файла:", e)
        return
    print(f"Загружен входной файл: {input_path} ({df.shape[0]} строк x {df.shape[1]} столбцов)")
    if column not in df.columns:
        print("Столбец не найден. Доступные столбцы:", df.columns.tolist())
        return
    external_df = load_external_data(external_url) if external_url else pd.DataFrame()
    if external_url:
        if not external_df.empty:
            print(f"Загружен внешний источник: {external_url} ({external_df.shape[0]} строк x {external_df.shape[1]} столбцов)")
        else:
            print(f"Внешний источник {external_url} пуст или не загружен.")
    series = df[column]
    dataset_words = extract_words_from_series(series)
    external_words = extract_words_from_series(external_df.stack()) if not external_df.empty else set()
    base_keys = set(car_brands_models.keys())
    candidates = (dataset_words | external_words) - base_keys
    additions = prepare_additions(base_keys, candidates, threshold=0.85)
    print(f"Найдено кандидатов для добавления: {len(additions)}")
    if additions:
        print("Добавленные пары (кандидат -> русское):")
        for k, v in additions.items():
            print(" -", k, "→", v)
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
        print("Чтобы скачать файл — возьмите его из текущей директории или перенесите по FTP/HTTP.")
    except Exception as e:
        print("Ошибка сохранения:", e)

# ---------------------------
# Улучшенная визуализация для Streamlit
# ---------------------------
def _build_final_map(base: dict, additions: dict) -> dict:
    m = {**base, **additions}
    keys_lower_map = {k.lower(): k for k in m.keys()}
    return {"map": m, "keys_lower_map": keys_lower_map}

def _highlight_html(text: str, final_map: dict, keys_lower_map: dict) -> str:
    if not isinstance(text, str) or not final_map:
        return text or ""
    escaped = sorted((re.escape(k) for k in final_map.keys()), key=len, reverse=True)
    if not escaped:
        return text
    pattern = re.compile(r'(?<!\w)(?:' + "|".join(escaped) + r')(?!\w)', flags=re.IGNORECASE)
    def repl(m):
        found = m.group(0)
        k_lower = found.lower()
        orig_k = keys_lower_map.get(k_lower)
        if orig_k:
            ru = final_map.get(orig_k, orig_k) or orig_k
            ru_decl = decline_word_cached(ru)
            return f"<mark>{found} ({ru_decl})</mark>"
        return found
    return pattern.sub(repl, text)

def _count_matches_in_series(series: pd.Series, final_map: dict) -> pd.Series:
    if series is None or series.empty or not final_map:
        return pd.Series(dtype=int)
    all_text = series.dropna().astype(str).str.cat(sep=' ')
    counts = {}
    for k in final_map.keys():
        pattern = re.compile(r'(?<!\w)' + re.escape(k) + r'(?!\w)', flags=re.IGNORECASE)
        counts[k] = len(pattern.findall(all_text))
    return pd.Series(counts).sort_values(ascending=False)

def run_streamlit_app() -> None:
    st.set_page_config(page_title="Автообработка расширенная", layout="wide")
    st.title("Обработка названий автомобилей — улучшенная версия")
    st.markdown(
        "Загрузите файл (CSV/XLSX), укажите URL для расширения словаря. "
        "Настройте порог похожести, просмотрите статистику и скачайте результат."
    )

    # Sidebar controls
    st.sidebar.header("Настройки")
    threshold = st.sidebar.slider("Порог похожести (для автодобавления)", 0.6, 0.99, 0.85, 0.01)
    top_n = st.sidebar.number_input("Показывать топ-значений в графике", min_value=5, max_value=100, value=15, step=5)
    show_highlight_preview = st.sidebar.checkbox("Показывать подсветку совпадений (HTML превью)", value=True)
    show_mapping = st.sidebar.checkbox("Показать текущий словарь (таблица)", value=False)
    translit_allowed = st.sidebar.checkbox("Автотранслитерация латиницы → кириллица", value=True)

    st.sidebar.header("Добавить бренд/модель")
    new_key = st.sidebar.text_input("Ключ (бренд/модель)", "")
    new_value = st.sidebar.text_input("Русское название/описание", "")
    if st.sidebar.button("Добавить в словарь"):
        if new_key and new_value:
            car_brands_models[new_key] = new_value
            added_pairs[new_key] = new_value
            save_additions()
            st.sidebar.success(f"Добавлено: {new_key} → {new_value}")
        else:
            st.sidebar.error("Оба поля обязательны.")

    if st.sidebar.button("Сохранить текущий словарь в файл"):
        try:
            with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
                json.dump({str(k): str(v) for k, v in car_brands_models.items()}, f, ensure_ascii=False, indent=2)
            st.sidebar.success(f"Словарь сохранён в {ADDITIONS_FILE}")
        except Exception as e:
            st.sidebar.error(f"Ошибка при сохранении: {e}")

    uploaded_file = st.file_uploader("Файл (CSV/XLSX)", type=["xlsx", "xls", "csv"])
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

        st.info(f"Загружен файл: {uploaded_file.name} — {df.shape[0]} строк × {df.shape[1]} столбцов")
        st.dataframe(df.head(5))
        col = st.selectbox("Столбец для обработки", df.columns.tolist())

        if st.button("Обработать"):
            external_df = load_external_data(external_url) if external_url else pd.DataFrame()
            if external_url:
                if not external_df.empty:
                    st.success(f"Внешний источник загружен: {external_url} — {external_df.shape[0]} строк × {external_df.shape[1]} столбцов")
                    st.dataframe(external_df.head(5))
                else:
                    st.warning("Внешний источник пуст или не удалось загрузить.")

            series = df[col]
            dataset_words = extract_words_from_series(series)
            external_words = extract_words_from_series(external_df.stack()) if not external_df.empty else set()
            base_keys = set(car_brands_models.keys())
            candidates = (dataset_words | external_words) - base_keys

            st.info(f"Уникальных слов в колонке: {len(dataset_words)}. Во внешнем источнике: {len(external_words)}.")
            st.info(f"Кандидатов (не в словаре): {len(candidates)}. Примеры: {', '.join(list(candidates)[:10])}")

            additions = prepare_additions(base_keys, candidates, threshold=threshold)
            if additions:
                st.success(f"Найдено {len(additions)} кандидатов — добавлено локально.")
                st.dataframe(pd.DataFrame.from_dict(additions, orient="index", columns=["rus"]).reset_index().rename(columns={"index": "key"}).head(100))
                added_pairs.update(additions)
                car_brands_models.update(additions)
                save_additions()
            else:
                st.info("Новые кандидаты не найдены по выбранному порогу.")

            final = _build_final_map(car_brands_models, additions)
            final_map = final["map"]
            keys_lower_map = final["keys_lower_map"]

            df["_processed"] = df[col].fillna("").astype(str).apply(lambda v: process_text(v, final_map, additions, translit_allowed=translit_allowed))

            st.subheader("Статистика найденных брендов/моделей")
            counts = _count_matches_in_series(df[col], final_map)
            total_matches = int(counts.sum()) if not counts.empty else 0
            st.metric("Всего вхождений найденных ключей", total_matches)
            if not counts.empty:
                top = counts[counts > 0].head(top_n)
                if not top.empty:
                    st.write("Топ найденных (ключ → количество):")
                    st.dataframe(top.reset_index().rename(columns={"index": "key", 0: "count"}).head(top_n))
                    st.bar_chart(top)
                else:
                    st.info("Совпадений не найдено в данных.")
            else:
                st.info("Нет ключей для подсчёта совпадений.")

            if show_mapping:
                st.subheader("Текущий словарь брендов/моделей")
                dmap = pd.DataFrame(list(final_map.items()), columns=["key", "rus"])
                st.dataframe(dmap.sort_values("key").reset_index(drop=True))
                map_bytes = json.dumps(final_map, ensure_ascii=False, indent=2).encode("utf-8")
                st.download_button("Скачать словарь (JSON)", map_bytes, "brands_map.json", mime="application/json")

            st.subheader("Превью обработанных строк")
            preview = df.head(200).copy()
            st.dataframe(preview[[col, "_processed"]].rename(columns={col: "original", "_processed": "processed"}))

            if show_highlight_preview:
                st.markdown("### Подсвеченное превью (HTML)")
                rows_html = []
                for i in preview.index.tolist():
                    orig = preview.at[i, col]
                    highlighted = _highlight_html(str(orig), final_map, keys_lower_map)
                    rows_html.append(f"<tr><td style='padding:6px;border:1px solid #ddd'><code>{str(orig)}</code></td>"
                                     f"<td style='padding:6px;border:1px solid #ddd'>{highlighted}</td></tr>")
                table_html = (
                    "<table style='border-collapse:collapse;width:100%;'>"
                    "<thead><tr><th style='text-align:left;padding:8px;border:1px solid #ddd'>Оригинал</th>"
                    "<th style='text-align:left;padding:8px;border:1px solid #ddd'>Подсветка</th></tr></thead>"
                    "<tbody>"
                    + "".join(rows_html) +
                    "</tbody></table>"
                )
                st.markdown(table_html, unsafe_allow_html=True)

            export_fmt = st.radio("Формат экспорта результата", ["CSV", "Excel"], horizontal=True)
            if export_fmt == "Excel":
                buf = io.BytesIO()
                try:
                    df.to_excel(buf, index=False)
                    buf.seek(0)
                    st.download_button("Скачать Excel", buf, "result.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                except Exception as e:
                    st.error(f"Ошибка при формировании Excel: {e}")
            else:
                csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
                st.download_button("Скачать CSV", csv_bytes, "result.csv", mime="text/csv")
    else:
        st.info("Загрузите файл для обработки или запустите CLI (см. README).")

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
