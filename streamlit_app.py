# !/usr/bin/env python3
# integrated_car_processing_with_dict_load_fixed_csv_encoding.py
# Исправления: при чтении/записи CSV используется utf-8-sig (BOM) чтобы Excel
# корректно видел русские символы.

from __future__ import annotations
import io
import re
import sys
import json
import os
import argparse
import requests
import pandas as pd
from difflib import SequenceMatcher
from functools import lru_cache
from collections import Counter
from typing import Optional, Dict, Set, List, Any

# опциональные зависимости
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None
try:
    import altair as alt  # type: ignore
except Exception:
    alt = None
try:
    import pymorphy2  # type: ignore
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

CSV_ENCODING = "utf-8-sig"  # Используем BOM-совместимую кодировку для CSV (Excel-friendly)
ADDITIONS_FILE = "additional_brands.json"

# Базовый словарь (англ -> рус)
car_brands_models: Dict[str, str] = {
    "BMW": "БМВ",
    "1 Series": "1 Серия", "2 Series": "2 Серия", "3 Series": "3 Серия",
    "4 Series": "4 Серия", "5 Series": "5 Серия", "6 Series": "6 Серия",
    "7 Series": "7 Серия", "8 Series": "8 Серия",
    "X1": "Икс 1", "X2": "Икс 2", "X3": "Икс 3", "X4": "Икс 4",
    "X5": "Икс 5", "X6": "Икс 6", "X7": "Икс 7", "Z4": "Зет 4",
    "M3": "Эм 3", "M5": "Эм 5", "M Series": "Эм Серия",
    "Mercedes-Benz": "Мерседес-Бенц", "Mercedes": "Мерседес",
    "A-Class": "А-Класс", "B-Class": "Б-Класс", "C-Class": "С-Класс",
    "E-Class": "Е-Класс", "S-Class": "Си-Класс", "CLA": "CLA", "GLA": "GLA",
    "GLC": "ГЛЦ", "GLE": "ГЛЕ", "GLS": "ГЛС", "G-Class": "Г-Класс", "CLS": "ЦЛС",
    "Vito": "Вито", "eVito": "еВито", "Sprinter": "Спринтер", "Citan": "Ситан", "V-Class": "В-Класс",
    "Toyota": "Тойота", "Corolla": "Королла", "Camry": "Камри", "RAV4": "Рав 4",
    "Prius": "Приус", "Land Cruiser": "Ленд Крузер", "Yaris": "Ярис",
    "Highlander": "Хайлендер", "Hilux": "Хайлюкс", "Sienta": "Сента",
    "Avensis": "Авенсис", "HiAce": "ХайЭйс", "Proace": "Проэйс", "Dyna": "Дайна",
    "Toyota Hiace Commuter": "ХайЭйс Комьютер", "Toyota Proace City": "Проэйс Сити",
    "Corolla Cross": "Королла Кросс", "C-HR": "C-HR",
    "Mazda": "Мазда", "Mazda3": "Мазда 3", "Mazda6": "Мазда 6", "Mazda2": "Мазда 2",
    "Mazda CX-30": "Мазда CX-30", "Mazda CX-5": "Мазда CX-5", "MX-5": "МХ 5", "MX-30": "Мазда MX-30",
    "Subaru": "Субару", "Impreza": "Импреза", "Forester": "Форестер",
    "Outback": "Аутбек", "XV": "Икс ВИ", "BRZ": "BRZ", "Crosstrek": "Кросстрек", "Legacy": "Легаси",
    "Kia": "Киа", "Rio": "Рио", "Ceed": "Сид", "Sportage": "Спортейдж", "Sorento": "Соренто",
    "Soul": "Соул", "Optima": "Оптима", "Carnival": "Карнавал", "Stinger": "Стингер",
    "Kia Stonic": "Стонік", "Kia Seltos": "Селтос", "Seltos": "Селтос", "Stonic": "Стонік",
    "Kia EV6": "Киа EV6", "Kia EV9": "Киа EV9",
    "Hyundai": "Хёндай", "Elantra": "Элантра", "Sonata": "Соната", "Tucson": "Тусон",
    "Santa Fe": "Санта Фе", "Kona": "Кона", "Kona Electric": "Кона Электрик",
    "Palisade": "Палисад", "i30": "i30", "i20": "i20", "i4": "i4", "iX": "iX",
    "Hyundai Ioniq": "Ионик", "Ioniq 5": "Ионик 5", "Ioniq 6": "Ионик 6", "Hyundai Santa Cruz": "Санта Крус",
    "BYD": "БайДжи", "Han": "Хан", "Tang": "Танг", "Song": "Сонг", "Dolphin": "Дельфин",
    "BYD Tang EV": "Танг ЕВ", "BYD Atto 3": "Атто 3",
    "Geely": "Джили", "Atlas": "Атлас", "Tiggo": "Тигго", "Tiggo 7": "Тигго 7", "Coolray": "Кулрэй",
    "Emgrand": "Эмгранд", "Binrui": "Бинрай",
    "Chery": "Черри", "Arrizo": "Аризо", "Exeed": "Эксид",
    "JAC": "Джак", "Refine": "Рефайн",
    "Lifan": "Лифан", "F3": "Ф3", "F7": "Ф7", "Baojun": "Баоцзюнь",
    "Hongqi": "Хунци", "FAW": "Фав", "Bestune": "Бестюн", "Levdeo": "Левдео", "Wey": "Вей", "Yema": "Йема",
    "Lada": "Лада", "Vesta": "Веста", "Granta": "Гранта", "Kalina": "Калина", "Niva": "Нива",
    "Lada Priora": "Лада Приора", "Lada 4x4": "Лада 4х4", "Lada XRay": "Лада Xray",
    "UAZ": "УАЗ", "Patriot": "Патриот", "Hunter": "Хантер", "Pickup": "Пикап",
    "Gaz": "Газ", "GAZelle": "ГАЗель", "GAZelle Next": "ГАЗель Некст", "Gazelle Next": "ГАЗель Некст",
    "Sobol": "Соболь", "Sobol 4x4": "Соболь 4х4",
    "ZAZ": "Заз", "Vaz": "Ваз",
    "Audi": "Ауди", "A1": "А1", "A3": "А3", "A4": "А4", "A6": "А6", "A8": "А8", "TT": "ТТ",
    "Q3": "Кью 3", "Q5": "Кью 5", "Q7": "Кью 7", "Q8": "Кью 8", "RS3": "Эр Эс 3", "RS5": "Эр Эс 5",
    "Volkswagen": "Фольксваген", "Golf": "Гольф", "Polo": "Поло", "Passat": "Пассат",
    "Tiguan": "Тигуан", "Touareg": "Туарег", "Jetta": "Джетта", "Arteon": "Артеон",
    "Transporter": "Транспортер", "Caddy": "Кэдди", "Crafter": "Крафтер",
    "Volkswagen Caravelle": "Каравелле", "Multivan": "Мультивэн", "ID.3": "АйДи.3", "ID.4": "АйДи.4", "ID.Buzz": "АйДи.Базз",
    "Skoda": "Шкода", "Octavia": "Октавия", "Superb": "Суперб", "Kodiaq": "Кодьяк", "Karoq": "Кароак",
    "Fabia": "Фабия", "Yeti": "Йети", "Skoda Enyaq": "Еняк",
    "Ford": "Форд", "Fiesta": "Фиеста", "Focus": "Фокус", "Mustang": "Мустанг",
    "Ranger": "Рейнджер", "Bronco": "Бронко", "Transit": "Транзит", "Transit Custom": "Транзит Кастом",
    "Transit Connect": "Транзит Коннект", "Ford Transit Van": "Транзит Фургон", "Ford Courier": "Форд Курьер", "Ford Galaxy": "Форд Гэлакси",
    "e-Transit": "е-Транзит", "eSprinter": "еСпринтер", "eVito Tourer": "еВито Турайер",
    "Chevrolet": "Шевроле", "Aveo": "Авео", "Lacetti": "Лачетти", "Malibu": "Мальбу",
    "Cruze": "Круз", "Equinox": "Экуинокс", "Blazer": "Блейзер", "Tahoe": "Тахо", "Silverado": "Сильверадо",
    "Chevrolet Express": "Экспресс",
    "Peugeot": "Пежо", "208": "208", "308": "308", "508": "508", "3008": "3008", "5008": "5008",
    "Partner": "Партнёр", "Peugeot Partner": "Пежо Партнёр", "Boxer": "Боксер", "Peugeot Boxer": "Пежо Боксер",
    "Renault": "Рено", "Clio": "Клио", "Megane": "Меган", "Captur": "Каптюр",
    "Kangoo": "Кангру", "Kangoo Van": "Кангру Ван", "Kangoo Express": "Кангру Экспресс", "Kangoo ZE": "Кангру ЗЕ",
    "Trafic": "Трафик", "Master": "Мастер", "Renault Master": "Мастер", "Renault Master Van": "Мастер Фургон",
    "Renault Kangoo Express": "Кангру Экспресс", "Renault Trafic Passenger": "Трафик Пассенджер", "Koleos": "Колеос", "Duster": "Дастер", "Logan": "Логан", "Sandero": "Сандеро",
    "Fiat": "Фиат", "Panda": "Панда", "500": "500", "Tipo": "Типо", "Ducato": "Дукато",
    "Ducato Maxi": "Дукато Макси", "Fiat Ducato Maxi": "Дукато Макси", "Doblo": "Добло", "Fiorino": "Фиорино", "Talento": "Таленто",
    "Fiat Professional": "Фиат Профешионал",
    "Lancia": "Ланча",
    "Alfa Romeo": "Альфа Ромео", "Giulia": "Джулия", "Stelvio": "Стельвио",
    "Suzuki": "Сузуки", "Swift": "Свифт", "Ignis": "Игнис", "Vitara": "Витара", "Suzuki Carry": "Сузуки Кэрри",
    "Honda": "Хонда", "Accord": "Акорд", "Civic": "Сивик", "Fit": "Фит", "Jazz": "Джаз", "CR-V": "CR-V", "HR-V": "HR-V", "Pilot": "Пилот", "Odyssey": "Одиссея",
    "Mitsubishi": "Митсубиси", "Outlander": "Аутлендер", "Pajero": "Паджеро", "ASX": "ASX", "L200": "L200", "Mitsubishi L300": "Л300", "Eclipse Cross": "Иклепс Кросс",
    "Isuzu": "Исузу", "D-Max": "Ди-Макс", "Isuzu N-Series": "Исузу N-Серия",
    "Nissan": "Ниссан", "Altima": "Альтима", "Sentra": "Сентра", "Maxima": "Максима", "Rogue": "Роудж",
    "X-Trail": "Икс-Трэйл", "Qashqai": "Кашкай", "Leaf": "Лиф", "Titan": "Титан", "Navara": "Навара", "Patrol": "Патрол", "Murano": "Муранo", "Avalon": "Эвалон", "Venza": "Венза", "Tacoma": "Такома", "Tundra": "Тундра", "Nissan NV200": "НВ200", "e-NV200": "е-НВ200", "NV300": "НВ300", "NV400": "НВ400", "Nissan Patrol Y62": "Патрол Y62",
    "Polestar": "Полистар", "Polestar 2": "Полистар 2", "Polestar 3": "Полистар 3",
    "Lucid": "Лусид", "Air": "Эйр",
    "Rivian": "Ривиан", "R1T": "R1T",
    "NIO": "Нио", "ES6": "ES6", "ES7": "ES7",
    "XPeng": "ХПэнг", "P7": "P7",
    "Tesla": "Тесла", "Model S": "Модель S", "Model 3": "Модель 3", "Model X": "Модель X", "Model Y": "Модель Y",
    "Volvo": "Вольво", "S60": "S60", "S90": "S90", "V60": "V60", "XC40": "XC40", "XC60": "XC60", "XC90": "XC90",
    "Seat": "Сеат", "Cupra": "Купра",
    "Porsche": "Порше", "911": "911", "Cayman": "Кайман", "Macan": "Макан", "Taycan": "Тайкан",
    "Jaguar": "Ягуар", "Land Rover": "Ленд Ровер", "Range Rover": "Рендж Ровер", "Discovery": "Дискавери",
    "Mini": "Мини", "Cooper": "Купер",
    "Ferrari": "Феррари", "Lamborghini": "Ламборгини", "Huracan": "Уракан", "Urus": "Урус",
    "Maserati": "Мазерати", "Ghibli": "Гибли",
    "GMC": "ДжиЭмСи", "Sierra": "Сиерра", "Cadillac": "Кадиллак", "Escalade": "Эскадил",
    "Dodge": "Додж", "Challenger": "Челленджер", "Charger": "Чарджер",
    "Jeep": "Джип", "Wrangler": "Рэнглер", "Grand Cherokee": "Гранд Чероки",
    "Great Wall": "Грейт Уолл", "Haval": "Хавал", "Haval H9": "Хавал Н9", "Ora": "Ора", "Neta": "Нета", "Wuling": "Вулинг", "Roewe": "Роу", "Great Wall Wingle": "Вингл", "Great Wall Poer": "Поэр", "Gonow": "Гонов",
    "Opel": "Опель", "Astra": "Астра", "Corsa": "Корса", "Insignia": "Инсигния", "Vivaro": "Виваро", "Movano": "Мовано", "Combo": "Комбо", "Vauxhall": "Воксхолл",
    "Peugeot Partner": "Пежо Партнёр", "Citroen": "Ситроен", "Berlingo": "Берлинго", "Jumper": "Джампер", "Citroen Jumper": "Ситроен Джампер",
    "Iveco": "Ивеко", "Daily": "Дейли", "Iveco Daily Van": "Ивеко Дейли Фургон",
    "Maxus": "Максус", "V80": "В80", "G10": "Г10", "V80 LDV": "В80 ЛДВ", "LDV": "ЛДВ",
    "Foton": "Фотон", "View": "Вью",
    "Changan": "Чанган", "Omoda": "Омода", "Dongfeng": "Донгфэнг", "SouEast": "СаутИст",
    "Tata": "Тата", "Mahindra": "Махиндра",
    # типы/аббревиатуры
    "Hybrid": "Гибрид", "Plug-in Hybrid": "Подключаемый гибрид", "Electric": "Электро",
    "Van": "Фургон", "Minivan": "Минивэн", "MPV": "МПВ", "Pickup": "Пикап", "Crew Cab": "Дабл Кэб",
    "Chassis Cab": "Шасси-Кабина", "Panel Van": "Панель Ван",
}

# загрузка пользовательских добавлений
added_pairs: Dict[str, str] = {}
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                added_pairs = {str(k): str(v) for k, v in loaded.items()}
                car_brands_models.update(added_pairs)
    except Exception:
        pass

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
_LAT_RULES_SORTED = sorted(LAT_TO_CYR_RULES, key=lambda x: -len(x[0]))

def latin_to_cyrillic(text: str) -> str:
    if not isinstance(text, str) or not text:
        return text
    def translit_word(word: str) -> str:
        lower = word.lower()
        i = 0
        out = []
        while i < len(lower):
            matched = False
            for lat, cyr in _LAT_RULES_SORTED:
                if lower.startswith(lat, i):
                    out.append(cyr)
                    i += len(lat)
                    matched = True
                    break
            if not matched:
                out.append(lower[i])
                i += 1
        out_s = "".join(out)
        if word.isupper():
            return out_s.upper()
        if word[0].isupper():
            return out_s.capitalize()
        return out_s
    parts = re.split(r'(\s+)', text)
    res = []
    for p in parts:
        if re.search(r'[A-Za-z]', p):
            pieces = re.split(r'([^A-Za-z]+)', p)
            for s in pieces:
                res.append(translit_word(s) if re.search(r'[A-Za-z]', s) else s)
        else:
            res.append(p)
    return "".join(res)

def contains_latin(text: str) -> bool:
    return bool(re.search(r'[A-Za-z]', str(text)))

def contains_cyrillic(text: str) -> bool:
    return bool(re.search(r'[\u0400-\u04FF]', str(text)))

def build_final_struct(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict:
    final_map = {**base_map, **(additions or {})}
    if not final_map:
        return {"pattern": None, "map": {}, "len_max": 0}
    keys_sorted = sorted(final_map.keys(), key=len, reverse=True)
    escaped = [re.escape(k) for k in keys_sorted if k.strip()]
    pattern = re.compile(r'(?<!\w)(?:' + "|".join(escaped) + r')(?!\w)', flags=re.IGNORECASE)
    mapping: Dict[str, tuple] = {}
    for k in keys_sorted:
        ru = final_map.get(k) or k
        ru_decl = decline_word_cached(ru)
        mapping[k.lower()] = (k, ru_decl)
    return {"pattern": pattern, "map": mapping, "len_max": max((len(k) for k in final_map.keys()), default=0)}

def process_text_fast(text: str, final_struct: Dict, translit_allowed: bool = True) -> str:
    if not isinstance(text, str) or not final_struct:
        return text
    if translit_allowed and contains_latin(text) and not contains_cyrillic(text):
        cyr = latin_to_cyrillic(text)
        return f"{text} ({decline_word_cached(cyr)})"
    pattern = final_struct.get("pattern")
    mapping = final_struct.get("map", {})
    if pattern is None:
        return text
    def repl(m):
        f = m.group(0)
        info = mapping.get(f.lower())
        if info:
            return f"{f} ({info[1]})"
        return f
    return pattern.sub(repl, text)

def count_matches_in_series_fast(series: pd.Series, final_struct: Dict) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(dtype=int)
    pattern = final_struct.get("pattern")
    mapping = final_struct.get("map", {})
    if pattern is None or not mapping:
        return pd.Series(dtype=int)
    all_text = series.dropna().astype(str).str.cat(sep=' ')
    found = pattern.findall(all_text)
    cnt = Counter()
    for f in found:
        info = mapping.get(f.lower())
        if info:
            cnt[info[0]] += 1
    if not cnt:
        return pd.Series(dtype=int)
    s = pd.Series(cnt)
    return s.sort_values(ascending=False)

def find_similar_word_fast(word: str, keys_list: List[str], keys_map: Dict[str, str], threshold: float = 0.85) -> Optional[str]:
    if not word:
        return None
    w = word.lower()
    lw = len(w)
    best_ratio = 0.0
    best_key = None
    for k in keys_list:
        lk = len(k)
        if abs(lk - lw) > max(2, int(0.4 * max(lk, lw))):
            continue
        ratio = SequenceMatcher(None, w, k).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_key = k
    if best_ratio >= threshold and best_key:
        return keys_map[best_key]
    return None

def prepare_additions_fast(base_keys: Set[str], candidates: Set[str], threshold: float = 0.85) -> Dict[str, str]:
    additions: Dict[str, str] = {}
    keys_map = {k.lower(): k for k in base_keys}
    keys_lower = list(keys_map.keys())
    for cand in candidates:
        if cand in base_keys:
            continue
        cand_lower = cand.lower()
        sim = find_similar_word_fast(cand_lower, keys_lower, keys_map, threshold=threshold)
        if sim:
            additions[cand] = car_brands_models.get(sim, sim)
    return additions

def load_external_data(url: str) -> pd.DataFrame:
    if not url:
        return pd.DataFrame()
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "").lower()
        if "text/csv" in ct or url.lower().endswith(".csv"):
            txt = resp.content.decode(CSV_ENCODING, errors="ignore")
            return pd.read_csv(io.StringIO(txt))
        try:
            return pd.read_excel(io.BytesIO(resp.content))
        except Exception:
            txt = resp.content.decode(CSV_ENCODING, errors="ignore")
            return pd.read_csv(io.StringIO(txt))
    except Exception:
        return pd.DataFrame()

def extract_words_from_series(series: pd.Series) -> Set[str]:
    if series is None:
        return set()
    all_text = series.dropna().astype(str).str.cat(sep=' ')
    return set(re.findall(r'[A-Za-zА-Яа-я0-9\-_/\.]+', all_text))

def save_additions():
    try:
        with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): str(v) for k, v in added_pairs.items()}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# --------- Загрузка внешнего словаря (файл или URL) ----------
def parse_mapping_from_dataframe(df: pd.DataFrame) -> Dict[str, str]:
    if df is None or df.empty:
        return {}
    cols = [c.lower() for c in df.columns]
    if "key" in cols and "value" in cols:
        kcol = df.columns[cols.index("key")]
        vcol = df.columns[cols.index("value")]
        return {str(k): str(v) for k, v in zip(df[kcol].astype(str), df[vcol].astype(str)) if str(k).strip()}
    if df.shape[1] >= 2:
        kcol = df.columns[0]
        vcol = df.columns[1]
        return {str(k): str(v) for k, v in zip(df[kcol].astype(str), df[vcol].astype(str)) if str(k).strip()}
    if df.shape[1] == 1:
        vals = df.iloc[:, 0].dropna().astype(str).tolist()
        out = {}
        for v in vals:
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    out.update({str(k): str(vv) for k, vv in parsed.items()})
            except Exception:
                continue
        return out
    return {}

def load_dictionary(source: Optional[str] = None, fileobj: Optional[Any] = None) -> Dict[str, str]:
    """
    Load dictionary mapping (eng->rus) from:
      - fileobj (uploaded file-like object, BytesIO, etc.)
      - source: URL (http/https) or local path
    Supports JSON (object), CSV/XLSX two-column or columns named key/value.
    Uses utf-8-sig decoding for CSV/text to handle BOM and Excel.
    """
    def try_parse_bytes(bts: bytes) -> Dict[str, str]:
        text = bts.decode(CSV_ENCODING, errors="ignore")
        # JSON
        try:
            j = json.loads(text)
            if isinstance(j, dict):
                return {str(k): str(v) for k, v in j.items()}
        except Exception:
            pass
        # CSV
        try:
            df = pd.read_csv(io.StringIO(text))
            return parse_mapping_from_dataframe(df)
        except Exception:
            pass
        # Excel fallback
        try:
            df = pd.read_excel(io.BytesIO(bts))
            return parse_mapping_from_dataframe(df)
        except Exception:
            pass
        return {}

    # Try fileobj first
    if fileobj is not None:
        try:
            if hasattr(fileobj, "seek"):
                try:
                    fileobj.seek(0)
                except Exception:
                    pass
            # read raw
            if hasattr(fileobj, "read"):
                raw = fileobj.read()
                if isinstance(raw, bytes):
                    return try_parse_bytes(raw)
                if isinstance(raw, str):
                    # string already decoded
                    try:
                        j = json.loads(raw)
                        if isinstance(j, dict):
                            return {str(k): str(v) for k, v in j.items()}
                    except Exception:
                        pass
                    try:
                        df = pd.read_csv(io.StringIO(raw))
                        return parse_mapping_from_dataframe(df)
                    except Exception:
                        pass
            name = getattr(fileobj, "name", "") or ""
            if name.lower().endswith(".json"):
                try:
                    if hasattr(fileobj, "seek"):
                        try:
                            fileobj.seek(0)
                        except Exception:
                            pass
                    txt = fileobj.read()
                    if isinstance(txt, bytes):
                        txt = txt.decode(CSV_ENCODING, errors="ignore")
                    j = json.loads(txt)
                    if isinstance(j, dict):
                        return {str(k): str(v) for k, v in j.items()}
                except Exception:
                    pass
            if hasattr(fileobj, "getvalue"):
                try:
                    raw = fileobj.getvalue()
                    if isinstance(raw, bytes):
                        return try_parse_bytes(raw)
                    if isinstance(raw, str):
                        return try_parse_bytes(raw.encode(CSV_ENCODING, errors="ignore"))
                except Exception:
                    pass
        except Exception:
            pass

    # Try source (URL or local path)
    if source:
        try:
            if source.startswith("http"):
                r = requests.get(source, timeout=15)
                r.raise_for_status()
                ct = r.headers.get("Content-Type", "").lower()
                if "application/json" in ct or source.lower().endswith(".json"):
                    data = r.json()
                    if isinstance(data, dict):
                        return {str(k): str(v) for k, v in data.items()}
                if "text/csv" in ct or source.lower().endswith(".csv"):
                    txt = r.content.decode(CSV_ENCODING, errors="ignore")
                    df = pd.read_csv(io.StringIO(txt))
                    return parse_mapping_from_dataframe(df)
                # try excel
                try:
                    df = pd.read_excel(io.BytesIO(r.content))
                    return parse_mapping_from_dataframe(df)
                except Exception:
                    try:
                        j = json.loads(r.content.decode(CSV_ENCODING, errors="ignore"))
                        if isinstance(j, dict):
                            return {str(k): str(v) for k, v in j.items()}
                    except Exception:
                        return {}
            else:
                # local path
                if os.path.exists(source):
                    if source.lower().endswith(".json"):
                        with open(source, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            if isinstance(data, dict):
                                return {str(k): str(v) for k, v in data.items()}
                    if source.lower().endswith((".xls", ".xlsx")):
                        df = pd.read_excel(source)
                        return parse_mapping_from_dataframe(df)
                    # fallback csv
                    try:
                        df = pd.read_csv(source, encoding=CSV_ENCODING)
                        return parse_mapping_from_dataframe(df)
                    except Exception:
                        pass
        except Exception:
            pass
    return {}

# ---------------- Streamlit UI ----------------
def run_streamlit_app() -> None:
    if st is None:
        return
    st.set_page_config(page_title="Автообработка (ускорено)", layout="wide")
    st.title("Распознавание брендов/моделей — ускорённый модуль")
    st.markdown("Загрузите CSV/XLSX, выберите столбец — скрипт автоматически подсветит и добавит переводы.")

    sidebar = st.sidebar
    threshold = sidebar.slider("Порог похожести для автодобавления", 0.6, 0.99, 0.85, 0.01)
    translit_allowed = sidebar.checkbox("Автотранслитерация (латиница→кириллица)", value=True)

    # загрузка внешнего словаря через sidebar
    sidebar.header("Загрузить словарь (опционально)")
    dict_file = sidebar.file_uploader("Файл словаря (json/csv/xlsx)", type=["json", "csv", "xls", "xlsx"])
    dict_url = sidebar.text_input("URL словаря (json/csv/xlsx) — опционально")
    if sidebar.button("Загрузить словарь"):
        loaded = {}
        if dict_file is not None:
            try:
                loaded = load_dictionary(fileobj=dict_file)
            except Exception as e:
                st.error(f"Не удалось загрузить файл словаря: {e}")
        elif dict_url:
            loaded = load_dictionary(source=dict_url)
        if loaded:
            car_brands_models.update(loaded)
            added_pairs.update(loaded)
            save_additions()
            st.success(f"Загружено {len(loaded)} пар из словаря")
        else:
            st.info("Словарь не загружен или пустой")

    # ручное добавление
    sidebar.header("Добавить пару в словарь")
    new_k = sidebar.text_input("Ключ (англ)")
    new_v = sidebar.text_input("Русское название")
    if sidebar.button("Добавить в словарь вручную"):
        if new_k and new_v:
            car_brands_models[new_k] = new_v
            added_pairs[new_k] = new_v
            save_additions()
            st.success(f"Добавлено: {new_k} → {new_v}")
        else:
            st.error("Поля обязательны")

    uploaded = st.file_uploader("Загрузите CSV/XLSX", type=["csv", "xls", "xlsx"])
    external_url = st.text_input("URL внешнего источника (CSV/XLSX) — необязательно")

    if not uploaded:
        st.info("Загрузите файл выше, чтобы начать.")
        return

    try:
        # uploaded обычно является BytesIO/SpooledTemporaryFile; указываем кодировку для CSV
        if uploaded.name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(uploaded)
        else:
            # для безопасного чтения используем decode через getvalue если нужно
            try:
                df = pd.read_csv(uploaded, encoding=CSV_ENCODING)
            except Exception:
                # fallback: decode bytes and read from StringIO
                raw = uploaded.getvalue()
                txt = raw.decode(CSV_ENCODING, errors="ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
                df = pd.read_csv(io.StringIO(txt))
    except Exception as e:
        st.error(f"Не удалось прочитать файл: {e}")
        return

    st.success(f"Файл загружен: {uploaded.name} ({df.shape[0]}×{df.shape[1]})")
    st.dataframe(df.head(5))
    col = st.selectbox("Столбец для обработки", df.columns.tolist())

    if st.button("Обработать"):
        external_df = load_external_data(external_url) if external_url else pd.DataFrame()
        series = df[col]
        dataset_words = extract_words_from_series(series)
        external_words = extract_words_from_series(external_df.stack()) if not external_df.empty else set()
        base_keys = set(car_brands_models.keys())
        candidates = (dataset_words | external_words) - base_keys

        st.info(f"Уникальных токенов: {len(dataset_words)}; кандидатов вне словаря: {len(candidates)}")
        additions = prepare_additions_fast(base_keys, candidates, threshold=threshold)
        if additions:
            st.success(f"Найдено кандидатов для добавления: {len(additions)}")
            st.dataframe(pd.DataFrame.from_dict(additions, orient="index", columns=["rus"]).reset_index().rename(columns={"index":"key"}))
            added_pairs.update(additions)
            car_brands_models.update(additions)
            save_additions()
        else:
            st.info("Новые кандидаты не найдены по выбранному порогу.")

        final_struct = build_final_struct(car_brands_models, additions)
        df["_processed"] = df[col].fillna("").astype(str).apply(lambda v: process_text_fast(v, final_struct, translit_allowed=translit_allowed))

        st.dataframe(
            df[[col, "_processed"]]
            .rename(columns={col: "Исходник", "_processed": "Обработанный"})
            .head(200)
        )

        # HTML подсветка
        patt = final_struct.get("pattern")
        mapping = final_struct.get("map", {})
        def highlight_html(text: str) -> str:
            if not text or patt is None:
                return text or ""
            def rep(m):
                f = m.group(0)
                info = mapping.get(f.lower())
                if info:
                    return f"<mark style='background:#fffd8a'>{f} ({info[1]})</mark>"
                return f
            return patt.sub(rep, str(text))
        rows = []
        preview = df.head(200)
        for idx in preview.index:
            orig = preview.at[idx, col]
            highlighted = highlight_html(orig)
            rows.append(f"<tr><td style='padding:6px;border:1px solid #ddd'><code>{str(orig)}</code></td>"
                        f"<td style='padding:6px;border:1px solid #ddd'>{highlighted}</td></tr>")
        table_html = "<table style='width:100%;border-collapse:collapse'><thead><tr><th>Оригинал</th><th>Подсветка</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
        st.markdown(table_html, unsafe_allow_html=True)

        export = st.radio("Формат экспорта", ("CSV", "Excel"))
        if export == "Excel":
            buf = io.BytesIO()
            df.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button("Скачать Excel", buf, file_name="result.xlsx")
        else:
            # экспорт с BOM (utf-8-sig) — Excel корректно распознает русские символы
            csv_str = df.to_csv(index=False)
            csv_bytes = csv_str.encode(CSV_ENCODING)
            st.download_button("Скачать CSV", csv_bytes, file_name="result.csv", mime="text/csv")

# ---------------- CLI режим ----------------
def process_file_cli(input_path: str, column: str, external_url: Optional[str], output_path: Optional[str], dict_source: Optional[str]) -> None:
    try:
        if input_path.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(input_path)
        else:
            df = pd.read_csv(input_path, encoding=CSV_ENCODING)
    except Exception as e:
        print("Ошибка чтения файла:", e)
        return
    if column not in df.columns:
        print("Столбец не найден. Доступные:", df.columns.tolist())
        return
    # загрузить внешний словарь если указан
    if dict_source:
        loaded = load_dictionary(source=dict_source)
        if loaded:
            car_brands_models.update(loaded)
            added_pairs.update(loaded)
            save_additions()
            print(f"Загружено пар из словаря: {len(loaded)}")
    external_df = load_external_data(external_url) if external_url else pd.DataFrame()
    series = df[column]
    dataset_words = extract_words_from_series(series)
    external_words = extract_words_from_series(external_df.stack()) if not external_df.empty else set()
    base_keys = set(car_brands_models.keys())
    candidates = (dataset_words | external_words) - base_keys
    additions = prepare_additions_fast(base_keys, candidates, threshold=0.85)
    if additions:
        added_pairs.update(additions)
        car_brands_models.update(additions)
        save_additions()
    final_struct = build_final_struct(car_brands_models, additions)
    df[column] = df[column].fillna("").astype(str).apply(lambda v: process_text_fast(v, final_struct, translit_allowed=True))
    if not output_path:
        output_path = "result.xlsx" if input_path.lower().endswith(('.xls', '.xlsx')) else "result.csv"
    try:
        if output_path.lower().endswith(('.xls', '.xlsx')):
            df.to_excel(output_path, index=False)
        else:
            # Сохраняем CSV в utf-8-sig, чтобы Excel корректно отображал кириллицу
            df.to_csv(output_path, index=False, encoding=CSV_ENCODING)
        print("Результат сохранён в", output_path)
    except Exception as e:
        print("Ошибка сохранения:", e)

def main():
    if st:
        run_streamlit_app()
        return
    parser = argparse.ArgumentParser(description="Обработка названий автомобилей (ускорённая)")
    parser.add_argument("--input", "-i", help="Входной файл CSV/XLSX")
    parser.add_argument("--column", "-c", help="Имя столбца для обработки")
    parser.add_argument("--external", "-e", help="URL внешнего CSV/XLSX")
    parser.add_argument("--output", "-o", help="Путь для сохранения результата")
    parser.add_argument("--list", action="store_true", help="Вывести список ключей словаря")
    parser.add_argument("--dict", "-d", help="Путь/URL к внешнему словарю (json/csv/xlsx)")
    args = parser.parse_args()
    if args.list:
        print("Всего ключей в словаре:", len(car_brands_models))
        for k in sorted(car_brands_models.keys()):
            print(k, "->", car_brands_models[k])
        return
    if not args.input or not args.column:
        print("Укажите --input и --column, или используйте --list для просмотра словаря.")
        print("Пример: python script.py --input data.csv --column description --output result.csv --dict mydict.json")
        return
    process_file_cli(args.input, args.column, args.external, args.output, args.dict)

if __name__ == "__main__":
    main()
