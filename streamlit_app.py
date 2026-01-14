# !/usr/bin/env python3
# Интегрированный ускоренный скрипт: Aho-Corasick/Trie для быстрого поиска +
# Streamlit UI
from __future__ import annotations
import io
import os
import re
import json
import time
import html
import requests
import pandas as pd
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Optional, Dict, Set, List, Tuple, Any

import concurrent.futures
from typing import Callable, Iterable, List, Any

try:
    import streamlit as st  # type: ignore
except Exception:
    st = None

# pymorphy2 optional
try:
    import pymorphy2  # type: ignore
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

CSV_ENCODING = "utf-8-sig"
ADDITIONS_FILE = "additional_brands.json"

# --- ВАШ ПОЛНЫЙ СЛОВАРЬ --- вставьте сюда весь ваш словарь
car_brands_models: Dict[str, str] = {
    "Acura": "Акура",
    "Integra": "Интегра",
    "MDX": "МДХ",
    "RDX": "РДХ",
    "RSX": "РСХ",
    "TLX": "ТЛКС",
    "4C": "4C",
    "Alfa Romeo": "Альфа Ромео",
    "Giulia": "Джулия",
    "Stelvio": "Стельвио",
    "Tonale": "Тонале",
    "A1": "А1",
    "A3": "А3",
    "A4": "А4",
    "A5": "А 5",
    "A6": "А6",
    "A7": "А 7",
    "A8": "А8",
    "Audi": "Ауди",
    "e-tron": "Е-Трон",
    "e-tron GT": "Етрэн ГТ",
    "Q3": "Кью 3",
    "Q4 e-tron": "Кью 4 Етрэн",
    "Q5": "Кью 5",
    "Q7": "Кью 7",
    "Q8": "Кью 8",
    "R8": "R8",
    "RS Q3": "RS Кью 3",
    "RS3": "РС 3",
    "RS5": "РС 5",
    "RS7": "РС 7",
    "SQ5": "СКу 5",
    "SQ7": "СКу 7",
    "TT": "ТТ",
    "Aurus": "Аурус",
    "Aurus Komendant": "Аурус Командант",
    "Aurus Senat": "Аурус Сенат",
    "Baojun": "Баоцзюнь",
    "Baojun 510": "Баоцзюнь 510",
    "Baojun 530": "Баоцзюнь 530",
    "Baojun RC-6": "Баоцзюнь RC-6",
    "1 Series": "1 Серия",
    "2 Series": "2 Серия",
    "3 Series": "3 Серия",
    "4 Series": "4 Серия",
    "5 Series": "5 Серия",
    "6 Series": "6 Серия",
    "7 Series": "7 Серия",
    "8 Series": "8 Серия",
    "BMW": "БМВ",
    "M2": "Эм 2",
    "M3": "Эм 3",
    "M4": "Эм 4",
    "M5": "Эм 5",
    "X1": "Икс 1",
    "X2": "Икс 2",
    "X3": "Икс 3",
    "X4": "Икс 4",
    "X5": "Икс 5",
    "X6": "Икс 6",
    "X7": "Икс 7",
    "Z4": "Зет 4",
    "BYD Atto 3": "Атто 3",
    "BYD Dolphin": "Байджи Дельфин",
    "BYD Han": "Байджи Хан",
    "BYD Qin": "Байджи Цин",
    "BYD Seal": "Байджи Сил",
    "BYD Song": "Байджи Сонг",
    "BYD Tang": "Байджи Танг",
    "BYD Tang EV": "Танг ЕВ",
    "BYD Yuan": "Байджи Юань",
    "BYD Yuan EV": "Байджи Юань ЕВ",
    "Cadillac": "Кадиллак",
    "Escalade": "Эскадил",
    "Chery": "Черри",
    "Chery Arrizo 5": "Черри Аризо 5",
    "Chery QQ": "Черри QQ",
    "Chery Tiggo 2": "Черри Тигго 2",
    "Chery Tiggo 3": "Черри Тигго 3",
    "Chery Tiggo 7": "Черри Тигго 7",
    "Chery Tiggo 8": "Черри Тигго 8",
    "Bolt EV": "Болт ЕВ",
    "Chevrolet": "Шевроле",
    "Chevrolet Express": "Экспресс",
    "Aveo": "Авео",
    "Blazer": "Блейзер",
    "Cruz": "Круз",
    "Equinox": "Экуинокс",
    "Lacetti": "Лачетти",
    "Malibu": "Мальбу",
    "Silverado": "Сильверадо",
    "Spark": "Спарк",
    "Tahoe": "Тахо",
    "Traverse": "Трэверс",
    "Challenger": "Челленджер",
    "Charger": "Чарджер",
    "Dodge": "Додж",
    "EVolution": "Эволюция",
    "FAW": "Фав",
    "296 GTB": "296 GTB",
    "488": "488",
    "F8 Tributo": "F8 Трибуто",
    "Ferrari": "Феррари",
    "Roma": "Рома",
    "SF90": "SF90",
    "500": "500",
    "Doblo": "Добло",
    "Ducato": "Дукато",
    "Ducato Maxi": "Дукато Макси",
    "Fiat": "Фиат",
    "Fiat Ducato Maxi": "Дукато Макси",
    "Fiat Professional": "Фиат Профешионал",
    "Fiorino": "Фиорино",
    "Panda": "Панда",
    "Talento": "Таленто",
    "Tipo": "Типо",
    "Bronco": "Бронко",
    "e-Transit": "е-Транзит",
    "Ford": "Форд",
    "Ford Courier": "Форд Курьер",
    "Ford Galaxy": "Форд Гэлакси",
    "Ford Transit Van": "Транзит Фургон",
    "Mustang": "Мустанг",
    "Ranger": "Рейнджер",
    "Transit": "Транзит",
    "Transit Connect": "Транзит Коннект",
    "Transit Custom": "Транзит Кастом",
    "GAZ": "Газ",
    "GAZ Volga": "Волга",
    "GAZ Sadko": "Садко",
    "Gazel": "ГАЗель",
    "Gazel Business": "ГАЗель Бизнес",
    "Gazon Next": "Газон Некст",
    "GAZelle": "ГАЗель",
    "GAZelle Next": "ГАЗель Некст",
    "Sobol": "Соболь",
    "Sobol 4x4": "Соболь 4х4",
    "Atlas": "Атлас",
    "Binrui": "Бинрай",
    "Coolray": "Кулрэй",
    "Emgrand": "Эмгранд",
    "Geely": "Джили",
    "Geely Atlas": "Джили Атлас",
    "Geely Atlas Pro": "Джили Атлас Про",
    "Geely Binrui": "Джили Бинрай",
    "Geely Coolray": "Джили Кулрэй",
    "Geely Emgrand": "Джили Эмгранд",
    "Geely Geometry": "Джили Геометрия",
    "Geely Preface": "Джили Префейс",
    "Tiggo": "Тигго",
    "Tiggo 7": "Тигго 7",
    "GMC": "ДжиЭмСи",
    "Sierra": "Сиерра",
    "Great Wall": "Грейт Уолл",
    "Haval": "Хавал",
    "Haval F7": "Хавал F7",
    "Haval H2": "Хавал H2",
    "Haval H5": "Хавал H5",
    "Haval H6": "Хавал H6",
    "Haval H9": "Хавал Н9",
    "Haval Jolion": "Хавал Джолион",
    "Accord": "Акорд",
    "Civic": "Сивик",
    "CR-V": "КР-В",
    "Fit": "Фит",
    "HR-V": "ХР-В",
    "Honda": "Хонда",
    "Jazz": "Джаз",
    "NSX": "НСХ",
    "Odyssey": "Одиссей",
    "Pilot": "Пилот",
    "Ridgeline": "Риджлайн",
    "Hongqi": "Хунци",
    "Elantra": "Элантра",
    "Hyundai": "Хёндай",
    "Hyundai Ioniq": "Ионик",
    "Hyundai Santa Cruz": "Санта Крус",
    "i20": "i20",
    "i30": "i30",
    "i4": "i4",
    "iX": "iX",
    "Ioniq 5": "Ионик 5",
    "Ioniq 6": "Ионик 6",
    "Kona": "Кона",
    "Kona Electric": "Кона Электрик",
    "Palisade": "Палисад",
    "Santa Fe": "Санта Фе",
    "Sonata": "Соната",
    "Tucson": "Тусон",
    "D-Max": "Ди-Макс",
    "Isuzu": "Исузу",
    "Isuzu N-Series": "Исузу N-Серия",
    "JAC": "Джак",
    "JAC Refine S4": "Джак Рефайн S4",
    "JAC S2": "Джак S2",
    "JAC iEV": "Джак iEV",
    "Refine": "Рефайн",
    "Jaguar": "Ягуар",
    "Grand Cherokee": "Гранд Чероки",
    "Jeep": "Джип",
    "Wrangler": "Рэнглер",
    "KAMAZ": "КамАЗ",
    "KAMAZ Electric": "КамАЗ электромобиль",
    "KAMAZ Trucks": "КамАЗ грузовики",
    "Carnirval": "Карнавал",
    "Ceed": "Сид",
    "Kia": "Киа",
    "Kia EV6": "Киа EV6",
    "Kia EV9": "Киа EV9",
    "Kia Seltos": "Селтос",
    "Kia Stonic": "Стонік",
    "Optima": "Оптима",
    "Rio": "Рио",
    "Sorento": "Соренто",
    "Soul": "Соул",
    "Sportage": "Спортейдж",
    "Stinger": "Стингер",
    "4x4": "Нива 4x4",
    "Granta": "Гранта",
    "Kalina": "Калина",
    "Lada": "Лада",
    "Lada 4x4": "Лада 4х4",
    "Lada 4x4 Urban": "Лада 4x4 Урбан",
    "Lada Granta Cross": "Лада Гранта Кросс",
    "Lada Granta Liftback": "Лада Гранта хэтчбек",
    "Lada Granta Sedan": "Лада Гранта седан",
    "Lada Largus Cross": "Лада Ларгус Кросс",
    "Lada Niva Travel": "Лада Нива Тревел",
    "Lada Priora": "Лада Приора",
    "Lada Samara": "Лада Самара",
    "Lada Vesta Cross": "Лада Веста Кросс",
    "Lada Vesta Sport": "Лада Веста Спорт",
    "Lada Vesta SW": "Лада Веста Универсал",
    "Lada XRAY Cross": "Лада ХРей Кросс",
    "Lada XRay": "Лада Xray",
    "Largus": "Ларгус",
    "Niva": "Нива",
    "Vesta": "Веста",
    "Aventador": "Авендадор",
    "Huracan": "Уракан",
    "Lamborghini": "Ламборгини",
    "Sián": "Сиан",
    "Urus": "Урус",
    "Lancia": "Ланча",
    "Discovery": "Дискавери",
    "Land Rover": "Ленд Ровер",
    "Range Rover": "Рендж Ровер",
    "Levdeo": "Левдео",
    "F3": "Ф3",
    "F7": "Ф7",
    "Lifan": "Лифан",
    "Lifan 820": "Лифан 820",
    "Lifan KPR": "Лифан КРП",
    "Lifan Myway": "Лифан Майвэй",
    "Lifan Solano": "Лифан Солано",
    "Lifan X60": "Лифан X60",
    "Air": "Эйр",
    "Lucid": "Лусид",
    "Wey": "Линк & Ко",
    "Wey": "Вей",
    "Wuling": "Вулинг",
    "Wuling Hongguang": "Вулинг Хонггуан",
    "Wuling Rongguang": "Вулинг Жунгуан",
    "Wuling Sunshine": "Вулинг Саншайн",
    "G3": "ХПэнг G3",
    "G9": "ХПэнг G9",
    "P7": "ХПэнг P7",
    "XPeng": "ХПэнг",
    "XPeng G3": "ХПэнг G3",
    "XPeng G9": "ХПэнг G9",
    "XPeng P7": "ХПэнг P7",
    "Yema": "Йема",
    "ZAZ": "Заз",
    "Zetta": "Зетта",
    "Ambulance": "Скорая помощь",
    "Antique Car": "Антикварный автомобиль",
    "Armored Car": "Бронированный автомобиль",
    "ATV": "Вездеход",
    "Bus": "Автобус",
    "Bulldozer": "Бульдозер",
    "Cargo Truck": "Грузовой автомобиль",
    "Classic Car": "Классический автомобиль",
    "Construction Equipment": "Строительное оборудование",
    "Container Carrier": "Контейнеровоз",
    "Convertible": "Кабриолет",
    "Crane Truck": "Кран-манипулятор",
    "Cruiser": "Круизер",
    "Diplomatic Car": "Дипломатическое транспортное средство",
    "Dual Sport Bike": "Двухрежимный мотоцикл",
    "Dump Truck": "Самосвал",
    "Emergency Response": "Аварийно-спасательная служба",
    "Enduro Bike": "Эндуро",
    "Excavator": "Экскаватор",
    "Fire Engine": "Пожарная машина",
    "Flatbed": "Платформа",
    "Forklift": "Погрузчик",
    "Funeral Coach": "Катафалк",
    "Government Fleet": "Государственный автопарк",
    "Hot Rod": "Хотрод",
    "Loader": "Погрузчик",
    "Medical Transport": "Медицинская перевозка",
    "Military Vehicle": "Военная техника",
    "Mobile Crane": "Автомобильный кран",
    "Motorcycle": "Мотоцикл",
    "Muscle Car": "Мускул-кар",
    "Off-Road Bike": "Внедорожный мотоцикл",
    "Police Car": "Полиция",
    "Prison Transport": "Транспортировка заключенных",
    "Quad Bike": "Квадроцикл",
    "Reefer": "Изотермическая фура",
    "Rescue Vehicle": "Спасательное транспортное средство",
    "Retro Style": "Ретро-стиль",
    "Road Roller": "Каток дорожный",
    "Scooter": "Скутер",
    "Security Vehicle": "Охрана и безопасность",
    "Semi-trailer": "Полуприцеп",
    "Side-by-Side": "SSV (Side by Side)",
    "Snow Plow": "Снегоочистительная техника",
    "Sports Bike": "Спортбайк",
    "Three-Wheeler": "Трицикл",
    "Tipper": "Самосвальная техника",
    "Touring Bike": "Туристический мотоцикл",
    "Trailer": "Прицеп",
    "Trash Collector": "Мусоровоз",
    "Truck": "Грузовик",
    "UTV": "Универсальное транспортное средство",
}

# Загружаем дополнения
added_pairs: Dict[str, str] = {}
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                car_brands_models.update({str(k): str(v) for k, v in loaded.items()})
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

# транслитерация
LAT_TO_CYR_RULES = [
    ("shch", "щ"), ("sht", "шт"), ("sci", "щи"), ("sch", "щ"),
    ("oye", "ое"), ("oyu", "ою"), ("iya", "ия"), ("iye", "ие"),
    ("aye", "ая"), ("ayu", "аю"), ("eyu", "ею"), ("iu", "ю"),
    ("ia", "ия"), ("ya", "я"), ("yo", "ё"), ("yu", "ю"),
    ("zh", "ж"), ("ge", "ж"), ("j", "ж"), ("g", "ж"),
    ("kh", "х"), ("h", "х"), ("x", "х"),
    ("ts", "ц"), ("tz", "ц"), ("ch", "ч"), ("sh", "ш"),
    ("ye", "е"), ("i", "и"), ("j", "й"), ("ju", "ю"), ("ja", "я"),
    ("a", "а"), ("b", "б"), ("v", "в"), ("g", "г"), ("d", "д"),
    ("e", "е"), ("z", "з"), ("k", "к"), ("l", "л"), ("m", "м"),
    ("n", "н"), ("o", "о"), ("p", "п"), ("r", "р"), ("s", "с"),
    ("t", "т"), ("u", "у"), ("f", "ф"), ("y", "ы"), ("'", "ь"),
    ('"', "ъ"), ("x", "кс"), ("q", "к"), ("w", "в")
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

# Импорт ahocorasick, если есть
try:
    import ahocorasick  # type: ignore
    _HAS_AHO = True
except Exception:
    _HAS_AHO = False

def _is_word_char(ch: str) -> bool:
    return ch.isalnum() or ch == '_'

# Глобальная структура поиска
search_struct = None

def update_search_struct():
    global search_struct
    search_struct = build_final_struct_fast(car_brands_models, added_pairs)

def build_final_struct_fast(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    final_map = {**base_map}
    if additions:
        final_map.update(additions)
    final_map = {k: v for k, v in final_map.items() if isinstance(k, str) and k.strip()}
    if not final_map:
        return {"engine": None, "map": {}, "max_len": 0, "use_aho": False}
    mapping: Dict[str, Tuple[str, str]] = {}
    max_len = 0
    for k, v in final_map.items():
        lk = k.lower()
        ru = v if v is not None else k
        ru_decl = decline_word_cached(str(ru))
        mapping[lk] = (k, ru_decl)
        if len(lk) > max_len:
            max_len = len(lk)
    if _HAS_AHO:
        A = ahocorasick.Automaton()
        for lk, pair in mapping.items():
            A.add_word(lk, (lk, pair))
        A.make_automaton()
        return {"engine": A, "map": mapping, "max_len": max_len, "use_aho": True}
    else:
        trie = {}
        END = "_end_"
        for lk, pair in mapping.items():
            node = trie
            for ch in lk:
                node = node.setdefault(ch, {})
            node[END] = (lk, pair)
        return {"engine": trie, "map": mapping, "max_len": max_len, "use_aho": False}

def _find_matches_aho(text: str, struct: Dict[str, Any]) -> List[Tuple[int,int,str,str]]:
    A = struct["engine"]
    mapping = struct["map"]
    text_l = text.lower()
    matches: List[Tuple[int,int,str,str]] = []
    for end_idx, value in A.iter(text_l):
        lk, (orig_key, ru_decl) = value
        start_idx = end_idx - len(lk) + 1
        if start_idx > 0 and _is_word_char(text_l[start_idx - 1]):
            continue
        if end_idx + 1 < len(text_l) and _is_word_char(text_l[end_idx + 1]):
            continue
        matches.append((start_idx, end_idx, orig_key, ru_decl))
    if not matches:
        return []
    matches.sort(key=lambda x: (x[0], -(x[1]-x[0])))
    filtered = []
    last_end = -1
    for s,e,ok,ru in matches:
        if s > last_end:
            filtered.append((s,e,ok,ru))
            last_end = e
    return filtered

def _find_matches_trie(text: str, struct: Dict[str, Any]) -> List[Tuple[int,int,str,str]]:
    trie = struct["engine"]
    text_l = text.lower()
    n = len(text_l)
    max_len = struct["max_len"]
    matches: List[Tuple[int,int,str,str]] = []
    END = "_end_"
    for i in range(n):
        node = trie
        j = i
        while j < n and (j - i) < max_len and text_l[j] in node:
            node = node[text_l[j]]
            if END in node:
                lk, (orig_key, ru_decl) = node[END]
                start_idx = i
                end_idx = j
                if start_idx > 0 and _is_word_char(text_l[start_idx - 1]):
                    pass
                elif end_idx + 1 < n and _is_word_char(text_l[end_idx + 1]):
                    pass
                else:
                    matches.append((start_idx, end_idx, orig_key, ru_decl))
            j += 1
    if not matches:
        return []
    matches.sort(key=lambda x: (x[0], -(x[1]-x[0])))
    filtered = []
    last_end = -1
    for s,e,ok,ru in matches:
        if s > last_end:
            filtered.append((s,e,ok,ru))
            last_end = e
    return filtered

def get_matches(text: str, struct: Dict[str, Any]) -> List[Tuple[int,int,str,str]]:
    if not struct or struct["engine"] is None:
        return []
    return _find_matches_aho(text, struct) if struct["use_aho"] else _find_matches_trie(text, struct)

def process_text_fast_optimized(text: str, struct: Dict[str, Any], translit_allowed: bool = True) -> str:
    if not isinstance(text, str) or not struct:
        return text
    matches = get_matches(text, struct)
    if not matches:
        if translit_allowed and contains_latin(text) and not contains_cyrillic(text):
            cyr = latin_to_cyrillic(text)
            return f"{text} ({decline_word_cached(cyr)})"
        return text
    out_parts = []
    last = 0
    for s,e,orig,ru in matches:
        out_parts.append(text[last:s])
        out_parts.append(ru)
        last = e + 1
    out_parts.append(text[last:])
    return "".join(out_parts)

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
            return pd.read_csv(io.StringIO(resp.content.decode(CSV_ENCODING, errors="ignore")))
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

def load_dictionary(source: Optional[str]=None, fileobj: Optional[io.BytesIO]=None) -> Dict[str, str]:
    result: Dict[str, str] = {}
    try:
        if fileobj is not None:
            if source and source.lower().endswith('.json'):
                content = fileobj.getvalue().decode('utf-8')
                data = json.loads(content)
                if isinstance(data, dict):
                    result = data
            elif source and source.lower().endswith(('.csv', '.xls', '.xlsx')):
                if source.lower().endswith('.csv'):
                    df = pd.read_csv(fileobj)
                else:
                    df = pd.read_excel(fileobj)
                if len(df.columns) >= 2:
                    result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        elif source:
            if source.startswith('http'):
                resp = requests.get(source, timeout=10)
                if source.lower().endswith('.json'):
                    data = resp.json()
                    if isinstance(data, dict):
                        result = data
                else:
                    if source.lower().endswith('.csv'):
                        df = pd.read_csv(io.StringIO(resp.text))
                    else:
                        df = pd.read_excel(io.BytesIO(resp.content))
                    if len(df.columns) >= 2:
                        result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
            else:
                if source.lower().endswith('.json'):
                    with open(source, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, dict):
                            result = data
                elif source.lower().endswith('.csv'):
                    df = pd.read_csv(source)
                    if len(df.columns) >= 2:
                        result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
                else:
                    df = pd.read_excel(source)
                    if len(df.columns) >= 2:
                        result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        return {str(k): str(v) for k, v in result.items()}
    except Exception:
        return {}

def prepare_additions_fast(base_keys: set, candidates: set, threshold: float=0.85) -> Dict[str, str]:
    additions: Dict[str, str] = {}
    for candidate in candidates:
        for base in base_keys:
            similarity_full = SequenceMatcher(None, candidate, base).ratio()
            if similarity_full >= threshold:
                additions[candidate] = base
                break
            else:
                candidate_letters = sorted(candidate.lower())
                base_letters = sorted(base.lower())
                similarity_letters = SequenceMatcher(None, "".join(candidate_letters), "".join(base_letters)).ratio()
                if similarity_letters >= threshold:
                    additions[candidate] = base
                    break
    return additions

# ==========================
# ВАЖНО: вставьте сюда функции из второго файла (safe_thread_map, process_single_value, process_dataframe_column)
def safe_thread_map(func: Callable[[Any], Any], data: Iterable, requested_workers: Optional[int]=None) -> List:
    """
    ThreadPoolExecutor map с fallback на последовательную обработку
    """
    seq = list(data)
    if not seq:
        return []
    max_workers_env = os.cpu_count() or 1
    if requested_workers is None:
        requested_workers = min(8, max_workers_env)
    workers = max(1, min(requested_workers, max_workers_env, len(seq)))
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            return list(ex.map(func, seq))
    except RuntimeError as e:
        print("ThreadPoolExecutor не удалось запустить, fallback:", e)
        return [func(x) for x in seq]
    except Exception as e:
        print("Обработка не удалась, fallback:", e)
        return [func(x) for x in seq]

def process_single_value(value: str, translit: bool=True) -> str:
    global search_struct
    try:
        if search_struct is None:
            update_search_struct()
        return process_text_fast_optimized(value, search_struct, translit_allowed=translit)
    except:
        return value

def process_dataframe_column(df: "pd.DataFrame", col: str, translit_allowed: bool=True, requested_workers: Optional[int]=None) -> List[str]:
    data_list = df[col].astype(str).tolist()
    fn = lambda v: process_single_value(v, translit_allowed)
    return safe_thread_map(fn, data_list, requested_workers=requested_workers or (os.cpu_count() or 1))

# ==========================
# Основная UI функция
def run_streamlit_app() -> None:
    global search_struct
    if st is None:
        return
    st.set_page_config(page_title="Автообработка (ускорено)", layout="wide")
    st.title("Распознавание брендов/моделей (ускорено)")
    st.markdown("Используется Aho-Corasick (если установлен) или Trie для быстрого поиска в большом словаре.")

    sidebar = st.sidebar
    threshold = sidebar.slider("Порог для автодобавления", 0.6, 0.99, 0.85, 0.01)
    translit_allowed = sidebar.checkbox("Автотранслитерация (латиница → кириллица)", value=True)

    sidebar.header("Словарь / дополнения")
    dict_file = sidebar.file_uploader("Файл словаря (json/csv/xlsx)", type=["json", "csv", "xls", "xlsx"])
    dict_url = sidebar.text_input("URL словаря (json/csv/xlsx)")
    if sidebar.button("Загрузить словарь"):
        loaded = {}
        if dict_file is not None:
            try:
                loaded = load_dictionary(source=dict_file.name, fileobj=dict_file)
            except Exception as e:
                st.error(f"Ошибка загрузки файла: {e}")
        elif dict_url:
            try:
                loaded = load_dictionary(source=dict_url)
            except Exception as e:
                st.error(f"Ошибка загрузки по URL: {e}")
        if loaded:
            for k, v in loaded.items():
                car_brands_models[k] = v
            added_pairs.update(loaded)
            save_additions()
            update_search_struct()
            st.success(f"Загружено {len(loaded)} пар")
        else:
            st.info("Словарь не загружен или пуст")

    sidebar.header("Добавить пару вручную")
    new_k = sidebar.text_input("Ключ (англ)")
    new_v = sidebar.text_input("Русское название")
    if sidebar.button("Добавить пару"):
        if new_k and new_v:
            car_brands_models[new_k] = new_v
            added_pairs[new_k] = new_v
            save_additions()
            update_search_struct()
            st.success(f"Добавлено: {new_k} → {new_v}")
        else:
            st.error("Заполните оба поля")

    # Построим структуру поиска один раз перед обработкой
    if search_struct is None:
        update_search_struct()

    uploaded = st.file_uploader("Выберите CSV/XLSX", type=["csv", "xls", "xlsx"])
    external_url = sidebar.text_input("URL внешних данных (CSV/XLSX) — необязательно")
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

    if st.button("Обработать данные"):
        ext_df = load_external_data(external_url) if external_url else pd.DataFrame()
        series = df[col]
        dataset_words = extract_words_from_series(series)
        external_words = extract_words_from_series(ext_df.stack()) if not ext_df.empty else set()
        base_keys = set(car_brands_models.keys())
        candidates = (dataset_words | external_words) - base_keys

        # Обновление дополнений
        additions = prepare_additions_fast(base_keys, candidates, threshold=threshold)
        if additions:
            for k, v in additions.items():
                car_brands_models[k] = v
            added_pairs.update(additions)
            save_additions()
            update_search_struct()

        # Обработка строки через safe_thread_map
        processed_values = process_dataframe_column(df, col, translit_allowed=translit_allowed)
        df["Обработанное"] = processed_values

        def highlight_using_matches(text: str, matches: List[Tuple[int,int,str,str]]) -> str:
            if not matches:
                return html.escape(text)
            parts = []
            last = 0
            for s,e,orig,ru in matches:
                parts.append(html.escape(text[last:s]))
                fragment = html.escape(text[s:e+1])
                parts.append(f"<mark style='background:#fffd8a'>{fragment} ({html.escape(ru)})</mark>")
                last = e + 1
            parts.append(html.escape(text[last:]))
            return "".join(parts)

        # Создаем HTML таблицу
        def create_html_table(df: pd.DataFrame, struct: Dict[str, Any]) -> str:
            html_rows = ""
            for idx in df.index[:200]:
                original = str(df.at[idx, "Исходное"])
                matches = get_matches(original, struct)
                highlighted_value = highlight_using_matches(original, matches)
                style = "background-color:#ffff99" if "<mark" in highlighted_value else ""
                icon = "🔍" if "<mark" in highlighted_value else "⚪"
                html_rows += (
                    f"<tr>"
                    f"<td style='padding:6px;border:1px solid #ddd; {style}' title='Исходное: {html.escape(original, quote=False)}'><code>{html.escape(original, quote=False)}</code></td>"
                    f"<td style='padding:6px;border:1px solid #ddd'>{icon} {highlighted_value}</td>"
                    f"</tr>"
                )
            return (
                "<table style='width:100%;border-collapse:collapse'>"
                "<thead><tr><th>Исходное</th><th>Подсветка</th></tr></thead>"
                "<tbody>" + html_rows + "</tbody></table>"
            )

        html_table = create_html_table(df, search_struct)
        st.markdown(html_table, unsafe_allow_html=True)

        with st.expander("Полная таблица с исходным и обработанным"):
            st.dataframe(df[["Исходное", "Обработанное"]])

        # Экспорт
        export_format = st.radio("Формат экспорта", ("CSV", "Excel"))
        if export_format == "Excel":
            buf = io.BytesIO()
            df.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button("Скачать Excel", buf, file_name="result.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            csv_bytes = df.to_csv(index=False, encoding=CSV_ENCODING).encode(CSV_ENCODING)
            st.download_button("Скачать CSV", csv_bytes, file_name="result.csv", mime="text/csv")

# ===========================
# CLI режим
def main():
    import sys, argparse
    parser = argparse.ArgumentParser(description="Автообработка (ускорено)")
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

    if "streamlit" in sys.argv[0] or (st is not None and args.input is None and args.column is None):
        run_streamlit_app()
        return

    if not args.input or not args.column:
        print("Укажите --input и --column, или --list")
        return

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

    if args.dict:
        loaded = load_dictionary(source=args.dict)
        if loaded:
            for k, v in loaded.items():
                car_brands_models[k] = v
            added_pairs.update(loaded)

    # Построение структуры поиска
    global search_struct
    search_struct = build_final_struct_fast(car_brands_models, added_pairs)

    # Обработка
    series = df[args.column]
    dataset_words = extract_words_from_series(series)
    external_words = set()
    if args.external:
        ext_df = load_external_data(args.external)
        external_words = extract_words_from_series(ext_df.stack())

    base_keys = set(car_brands_models.keys())
    candidates = (dataset_words | external_words) - base_keys
    additions = prepare_additions_fast(base_keys, candidates, threshold=0.85)
    if additions:
        for k, v in additions.items():
            car_brands_models[k] = v
        added_pairs.update(additions)
        save_additions()
        search_struct = build_final_struct_fast(car_brands_models, added_pairs)

    df["Исходное"] = df[args.column]
    df["Обработанное"] = df[args.column].astype(str).apply(lambda v: process_text_fast_optimized(v, search_struct, translit_allowed=True))

    output_path = args.output or ("result.xlsx" if args.input.lower().endswith(('.xls', '.xlsx')) else "result.csv")
    try:
        if output_path.lower().endswith(('.xls', '.xlsx')):
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False, encoding=CSV_ENCODING)
        print("Результат сохранен:", output_path)
    except Exception as e:
        print("Ошибка при сохранении:", e)

if __name__ == "__main__":
    main()
