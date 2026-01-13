#!/usr/bin/env python3
# Полный исправленный код

from __future__ import annotations
import io
import os
import re
import json
import requests
import pandas as pd
from difflib import SequenceMatcher
from functools import lru_cache
from collections import Counter
from typing import Optional, Dict, Set, List, Any

try:
    import streamlit as st  # type: ignore
except Exception:
    st = None

try:
    import pymorphy2  # type: ignore
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

CSV_ENCODING = "utf-8-sig"
ADDITIONS_FILE = "additional_brands.json"

# Базовые данные
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
"Hybrid": "Гибрид", "Plug-in Hybrid": "Подключаемый гибрид", "Electric": "Электро",
"Van": "Фургон", "Minivan": "Минивэн", "MPV": "МПВ", "Pickup": "Пикап", "Crew Cab": "Дабл Кэб",
"Chassis Cab": "Шасси-Кабина", "Panel Van": "Панель Ван",
}

# Загрузка пользовательских добавлений
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                car_brands_models.update({str(k): str(v) for k, v in loaded.items()})
    except Exception:
        pass

added_pairs: Dict[str, str] = {}

@lru_cache(maxsize=10000)
def decline_word_cached(word: str) -> str:
    if not word or not morph:
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
        out_str = "".join(out)
        if word.isupper():
            return out_str.upper()
        if word[0].isupper():
            return out_str.capitalize()
        return out_str
    parts = re.split(r'(\s+)', text)
    res = []
    for p in parts:
        if re.search(r'[A-Za-z]', p):
            sub_parts = re.split(r'([^A-Za-z]+)', p)
            for s in sub_parts:
                if re.search(r'[A-Za-z]', s):
                    res.append(translit_word(s))
                else:
                    res.append(s)
        else:
            res.append(p)
    return "".join(res)

def contains_latin(text: str) -> bool:
    return bool(re.search(r'[A-Za-z]', str(text)))

def contains_cyrillic(text: str) -> bool:
    return bool(re.search(r'[\u0400-\u04FF]', str(text)))

def build_final_struct(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict:
    final_map = {**base_map}
    if additions:
        final_map.update(additions)
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

def format_custom(text: str, final_struct: Dict) -> str:
    if not isinstance(text, str) or not final_struct:
        return text
    years_match = re.search(r'(\d{4}\s*[~\-\–]\s*\d{4})', text)
    years = years_match.group(1) if years_match else ""
    text_no_years = text.replace(years, "").strip()
    pattern_pairs = re.findall(r'\([^\)]+\)|[A-Za-z0-9\-\.]+|[А-Яа-я0-9\-\.]+', text_no_years)
    brand = ""
    model = ""
    if pattern_pairs:
        brand_candidate = pattern_pairs[0]
        model_candidate = pattern_pairs[1] if len(pattern_pairs) > 1 else ""
        brand = re.sub(r'^[\(\s]+|[\)\s]+$', '', brand_candidate)
        model = re.sub(r'^[\(\s]+|[\)\s]+$', '', model_candidate)
    mp = final_struct.get("map", {})
    ru_brand = ""
    ru_model = ""
    if brand:
        val = mp.get(brand.lower())
        if val:
            ru_brand = val[1] if isinstance(val, (list, tuple)) and len(val) > 1 else ""
    if model:
        val = mp.get(model.lower())
        if val:
            ru_model = val[1] if isinstance(val, (list, tuple)) and len(val) > 1 else ""
    parts = []
    if brand:
        parts.append(brand)
    if model:
        parts.append(model)
    main = " ".join(parts).strip() or text.strip()
    extras = []
    if ru_brand:
        extras.append(ru_brand)
    if ru_model:
        extras.append(ru_model)
    if years:
        main = f"{main} {years}" if main else years
        extras.append(years)
    if extras:
        return f"{main} ({' '.join(extras).strip()})"
    return main

def translate_full_string(text: str, final_struct: Dict) -> str:
    parts = [part.strip() for part in str(text).split('/')]
    translated_parts = [format_custom(part, final_struct) for part in parts]
    return " / ".join(translated_parts)

def process_text_fast(text: str, final_struct: Dict, translit_allowed: bool = True) -> str:
    if not isinstance(text, str) or not final_struct:
        return text
    pattern_delim = re.compile(r'([/;])')
    if any(d in text for d in ['/', ';']):
        parts = pattern_delim.split(text)
        result_parts = []
        for part in parts:
            if part in ['/', ';']:
                result_parts.append(part)
            else:
                key_lower = part.lower()
                if key_lower in final_struct["map"]:
                    val = final_struct["map"][key_lower][1]
                    result_parts.append(val)
                else:
                    if translit_allowed and contains_latin(part) and not contains_cyrillic(part):
                        cyr = latin_to_cyrillic(part)
                        result_parts.append(f"{part} ({decline_word_cached(cyr)})")
                    else:
                        result_parts.append(part)
        return "".join(result_parts)
    else:
        key_lower = text.lower()
        if key_lower in final_struct["map"]:
            return final_struct["map"][key_lower][1]
        else:
            if translit_allowed and contains_latin(text) and not contains_cyrillic(text):
                cyr = latin_to_cyrillic(text)
                return f"{text} ({decline_word_cached(cyr)})"
            else:
                return format_custom(text, final_struct)

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
            json.dump({str(k): str(v) for k, v in {**car_brands_models, **added_pairs}.items()}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def parse_mapping_from_dataframe(df: pd.DataFrame) -> Dict[str, str]:
    if df is None or df.empty:
        return {}
    cols = [c.lower() for c in df.columns]
    if "key" in cols and "value" in cols:
        kcol = df.columns[cols.index("key")]
        vcol = df.columns[cols.index("value")]
        return {str(k): str(v) for k, v in zip(df[kcol].astype(str), df[vcol].astype(str)) if str(k).strip()}
    if len(cols) >= 2:
        kcol = df.columns[0]
        vcol = df.columns[1]
        return {str(k): str(v) for k, v in zip(df[kcol].astype(str), df[vcol].astype(str)) if str(k).strip()}
    if len(cols) == 1:
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
    def try_parse_bytes(bts: bytes) -> Dict[str, str]:
        text = bts.decode(CSV_ENCODING, errors="ignore")
        try:
            j = json.loads(text)
            if isinstance(j, dict):
                return {str(k): str(v) for k, v in j.items()}
        except Exception:
            pass
        try:
            return parse_mapping_from_dataframe(pd.read_csv(io.StringIO(text)))
        except Exception:
            pass
        try:
            return parse_mapping_from_dataframe(pd.read_excel(io.BytesIO(bts)))
        except Exception:
            pass
        return {}

    if fileobj:
        try:
            if hasattr(fileobj, "seek"):
                try:
                    fileobj.seek(0)
                except Exception:
                    pass
            if hasattr(fileobj, "read"):
                raw = fileobj.read()
                if isinstance(raw, bytes):
                    return try_parse_bytes(raw)
                if isinstance(raw, str):
                    try:
                        j = json.loads(raw)
                        if isinstance(j, dict):
                            return {str(k): str(v) for k, v in j.items()}
                    except Exception:
                        pass
                    try:
                        return parse_mapping_from_dataframe(pd.read_csv(io.StringIO(raw)))
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
                    return parse_mapping_from_dataframe(pd.read_csv(io.StringIO(txt)))
                try:
                    return parse_mapping_from_dataframe(pd.read_excel(io.BytesIO(r.content)))
                except Exception:
                    try:
                        return parse_mapping_from_dataframe(pd.read_csv(io.StringIO(r.content.decode(CSV_ENCODING))))
                    except Exception:
                        return {}
            else:
                if os.path.exists(source):
                    if source.lower().endswith(".json"):
                        with open(source, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            if isinstance(data, dict):
                                return {str(k): str(v) for k, v in data.items()}
                    if source.lower().endswith((".xls", ".xlsx")):
                        df = pd.read_excel(source)
                        return parse_mapping_from_dataframe(df)
                    try:
                        df = pd.read_csv(source, encoding=CSV_ENCODING)
                        return parse_mapping_from_dataframe(df)
                    except Exception:
                        pass
        except Exception:
            pass
    return {}

# --- UI и CLI ---

def run_streamlit_app() -> None:
    if st is None:
        return
    st.set_page_config(page_title="Автообработка", layout="wide")
    st.title("Распознавание брендов/моделей — улучшенная визуализация")
    st.markdown("Загрузите CSV/XLSX, выберите столбец — скрипт автоматически подсветит совпадения.")

    sidebar = st.sidebar
    threshold = sidebar.slider("Порог для автодобавления", 0.6, 0.99, 0.85, 0.01)
    translit_allowed = sidebar.checkbox("Автотранслитерация (латиница → кириллица)", value=True)

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

    sidebar.header("Добавить пару вручную")
    new_k = sidebar.text_input("Ключ (англ)")
    new_v = sidebar.text_input("Русское название")
    if sidebar.button("Добавить"):
        if new_k and new_v:
            car_brands_models[new_k] = new_v
            added_pairs[new_k] = new_v
            save_additions()
            st.success(f"Добавлено: {new_k} → {new_v}")
        else:
            st.error("Поля обязательны")

    uploaded = st.file_uploader("Выберите CSV/XLSX", type=["csv", "xls", "xlsx"])
    external_url = st.text_input("URL внешних данных (CSV/XLSX) — необязательно")
    if not uploaded:
        st.info("Загрузите файл выше.")
        return

    try:
        if uploaded.name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(uploaded)
        else:
            try:
                df = pd.read_csv(uploaded, encoding=CSV_ENCODING)
            except Exception:
                raw = uploaded.getvalue()
                txt = raw.decode(CSV_ENCODING, errors="ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
                df = pd.read_csv(io.StringIO(txt))
    except Exception as e:
        st.error(f"Ошибка чтения файла: {e}")
        return

    st.success(f"Файл: {uploaded.name} ({df.shape[0]} строк, {df.shape[1]} колонок)")
    st.dataframe(df.head(5))
    col = st.selectbox("Выберите столбец для обработки", df.columns.tolist())

    if st.button("Обработать"):
        ext_df = load_external_data(external_url) if external_url else pd.DataFrame()

        series = df[col]
        dataset_words = extract_words_from_series(series)
        external_words = extract_words_from_series(ext_df.stack()) if not ext_df.empty else set()
        base_keys = set(car_brands_models.keys())
        candidates = (dataset_words | external_words) - base_keys

        # Автоматическое добавление
        additions = prepare_additions_fast(base_keys, candidates, threshold=threshold)
        if additions:
            for k, v in additions.items():
                car_brands_models[k] = v
            added_pairs.update(additions)
            save_additions()

        final_struct = build_final_struct(car_brands_models, additions)
        pattern = final_struct.get("pattern")
        mapping = final_struct.get("map", {})

        # Создаем новые колонки: исходное и обработанное
        df["Исходное"] = df[col]
        df["Обработанное"] = df[col].astype(str).apply(lambda v: process_text_fast(v, final_struct, translit_allowed=translit_allowed))

        # Создаем HTML таблицу с подсветкой исходных данных
        def create_html_table(df: pd.DataFrame, pattern: re.Pattern, mapping: Dict) -> str:
            html_rows = ""
            for idx in df.index[:200]:
                original = df.at[idx, "Исходное"]
                def replacer(match):
                    f = match.group(0)
                    info = mapping.get(f.lower())
                    if info:
                        return f"<mark style='background:#fffd8a'>{f} ({info[1]})</mark>"
                    return f
                highlighted_value = pattern.sub(replacer, str(original))
                if "<mark" in highlighted_value:
                    style = "background-color:#ffff99"
                    icon = "🔍"
                else:
                    style = ""
                    icon = "⚪"
                html_rows += (
                    f"<tr>"
                    f"<td style='padding:6px;border:1px solid #ddd; {style}' title='Исходное: {original}'><code>{original}</code></td>"
                    f"<td style='padding:6px;border:1px solid #ddd'>{icon} {highlighted_value}</td>"
                    f"</tr>"
                )
            html_table = (
                "<table style='width:100%;border-collapse:collapse'>"
                "<thead><tr><th>Исходное</th><th>Подсветка</th></tr></thead>"
                "<tbody>" + html_rows + "</tbody></table>"
            )
            return html_table

        html_table = create_html_table(df, pattern, mapping)
        st.markdown(html_table, unsafe_allow_html=True)

        # Отображение таблицы с двумя колонками
        with st.expander("Полная таблица с исходным и обработанным"):
            display_df = df[["Исходное", "Обработанное"]]
            st.dataframe(display_df)

        # Экспорт
        export_format = st.radio("Формат экспорта", ("CSV", "Excel"))
        if export_format == "Excel":
            buf = io.BytesIO()
            df.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button("Скачать Excel", buf, file_name="result.xlsx")
        else:
            csv_bytes = df.to_csv(index=False, encoding=CSV_ENCODING).encode(CSV_ENCODING)
            st.download_button("Скачать CSV", csv_bytes, file_name="result.csv", mime="text/csv")


def main():
    if st:
        run_streamlit_app()
        return
    import argparse
    parser = argparse.ArgumentParser(description="Автообработка")
    parser.add_argument("--input", "-i", help="Входной файл CSV/XLSX")
    parser.add_argument("--column", "-c", help="Имя столбца")
    parser.add_argument("--external", "-e", help="URL внешних данных")
    parser.add_argument("--output", "-o", help="Путь для сохранения")
    parser.add_argument("--list", action="store_true", help="Показать список словаря")
    parser.add_argument("--dict", "-d", help="Файл или URL словаря")
    args = parser.parse_args()

    if args.list:
        print("Всего ключей в словаре:", len(car_brands_models))
        for k in sorted(car_brands_models):
            print(k, "→", car_brands_models[k])
        return
    if not args.input or not args.column:
        print("Укажите --input и --column, или --list")
        return

    # Обработка файла из командной строки
    try:
        if args.input.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(args.input)
        else:
            df = pd.read_csv(args.input, encoding=CSV_ENCODING)
    except Exception as e:
        print("Ошибка чтения файла:", e)
        return

    if args.column not in df.columns:
        print("Столбец не найден:", args.column)
        return

    # Загрузка словаря
    if args.dict:
        loaded = load_dictionary(source=args.dict)
        if loaded:
            for k, v in loaded.items():
                car_brands_models[k] = v
            added_pairs.update(loaded)
            save_additions()

    # Обработка внешних данных
    ext_df = load_external_data(args.external) if args.external else pd.DataFrame()

    # Подготовка
    series = df[args.column]
    dataset_words = extract_words_from_series(series)
    external_words = extract_words_from_series(ext_df.stack()) if not ext_df.empty else set()
    base_keys = set(car_brands_models.keys())
    candidates = (dataset_words | external_words) - base_keys

    # Автоматическое добавление
    additions = prepare_additions_fast(base_keys, candidates, threshold=0.85)
    for k, v in additions.items():
        car_brands_models[k] = v
    added_pairs.update(additions)
    save_additions()

    # Создаем структуру
    final_struct = build_final_struct(car_brands_models, additions)
    pattern = final_struct.get("pattern")
    mapping = final_struct.get("map", {})

    # Обработка данных
    df["Исходное"] = df[args.column]
    df["Обработанное"] = df[args.column].astype(str).apply(lambda v: process_text_fast(v, final_struct, translit_allowed=True))

    # Создаем колонку "Перевод" (если нужно)
    df["Перевод"] = df["Обработанное"]

    # Создаем колонку "Обработанное" (уже есть)
    # Экспорт
    output_path = args.output or ("result.xlsx" if args.input.lower().endswith(('.xls', '.xlsx')) else "result.csv")
    try:
        if output_path.lower().endswith(('.xls', '.xlsx')):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False, encoding=CSV_ENCODING)
        print("Результат сохранен:", output_path)
    except Exception as e:
        print("Ошибка сохранения:", e)


if __name__ == "__main__":
    main()
