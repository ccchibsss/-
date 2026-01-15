#!/usr/bin/env python3
import io
import os
import json
from typing import Dict, Tuple
import pandas as pd
import streamlit as st
import html

# Импорт морфологического анализатора (не обязательно)
try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

# --- Словарь транслитерации ---
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

# --- Объявление словаря марок и моделей ---
car_brands_models: Dict[str, str] = {
    "Acura": "Акура",
"Integra": "Интегра",
"MDX": "МДХ",
"RDX": "РДХ",
"RSX": "РСХ",
"TLX": "ТЛКС",

# Alfa Romeo
"4C": "4C",
"Alfa Romeo": "Альфа Ромео",
"Giulia": "Джулия",
"Stelvio": "Стельвио",
"Tonale": "Тонале",

# Audi
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

# Aurus
"Aurus": "Аурус",
"Aurus Komendant": "Аурус Командант",
"Aurus Senat": "Аурус Сенат",

# Baojun
"Baojun": "Баоцзюнь",
"Baojun 510": "Баоцзюнь 510",
"Baojun 530": "Баоцзюнь 530",
"Baojun RC-6": "Баоцзюнь RC-6",

# BMW
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

# BYD
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

# Cadillac
"Cadillac": "Кадиллак",
"Escalade": "Эскадил",

# Chery
"Chery": "Черри",
"Chery Arrizo 5": "Черри Аризо 5",
"Chery QQ": "Черри QQ",
"Chery Tiggo 2": "Черри Тигго 2",
"Chery Tiggo 3": "Черри Тигго 3",
"Chery Tiggo 7": "Черри Тигго 7",
"Chery Tiggo 8": "Черри Тигго 8",

# Chevrolet
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

# Dodge
"Challenger": "Челленджер",
"Charger": "Чарджер",
"Dodge": "Додж",

# EVolution
"EVolution": "Эволюция",

# FAW
"FAW": "Фав",

# Ferrari
"296 GTB": "296 GTB",
"488": "488",
"F8 Tributo": "F8 Трибуто",
"Ferrari": "Феррари",
"Roma": "Рома",
"SF90": "SF90",

# Fiat
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

# Ford
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

# GAZ
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

# Geely
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

# GMC
"GMC": "ДжиЭмСи",
"Sierra": "Сиерра",

# Great Wall
"Great Wall": "Грейт Уолл",

# Haval
"Haval": "Хавал",
"Haval F7": "Хавал F7",
"Haval H2": "Хавал H2",
"Haval H5": "Хавал H5",
"Haval H6": "Хавал H6",
"Haval H9": "Хавал Н9",
"Haval Jolion": "Хавал Джолион",

# Honda
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

# Hongqi
"Hongqi": "Хунци",

# Hyundai
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

# Isuzu
"D-Max": "Ди-Макс",
"Isuzu": "Исузу",
"Isuzu N-Series": "Исузу N-Серия",

# JAC
"JAC": "Джак",
"JAC Refine S4": "Джак Рефайн S4",
"JAC S2": "Джак S2",
"JAC iEV": "Джак iEV",
"Refine": "Рефайн",

# Jaguar
"Jaguar": "Ягуар",

# Jeep
"Grand Cherokee": "Гранд Чероки",
"Jeep": "Джип",
"Wrangler": "Рэнглер",

# KAMAZ
"KAMAZ": "КамАЗ",
"KAMAZ Electric": "КамАЗ электромобиль",
"KAMAZ Trucks": "КамАЗ грузовики",

# Kia
"Carnival": "Карнавал",
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

# Lada
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

# Lamborghini
"Aventador": "Авендадор",
"Huracan": "Уракан",
"Lamborghini": "Ламборгини",
"Sián": "Сиан",
"Urus": "Урус",

# Lancia
"Lancia": "Ланча",

# Land Rover
"Discovery": "Дискавери",
"Land Rover": "Ленд Ровер",
"Range Rover": "Рендж Ровер",

# Levdeo
"Levdeo": "Левдео",

# Lifan
"F3": "Ф3",
"F7": "Ф7",
"Lifan": "Лифан",
"Lifan 820": "Лифан 820",
"Lifan KPR": "Лифан КРП",
"Lifan Myway": "Лифан Майвэй",
"Lifan Solano": "Лифан Солано",
"Lifan X60": "Лифан X60",

# Lucid
"Air": "Эйр",
"Lucid": "Лусид",

# Lynk & Co
"Lynk & Co": "Линк & Ко",
"Lynk & Co 01": "Линк & Ко 01",
"Lynk & Co 03": "Линк & Ко 03",
"Lynk & Co 05": "Линк & Ко 05",

# Maserati
"Ghibli": "Гибли",
"GranTurismo": "Гран Туризмо",
"Levante": "Леванте",
"MC20": "MC20",
"Maserati": "Мазерати",
"Quattroporte": "Кваттропорте",

# Mazda
"BT-50": "БТ-50",
"CX-3": "Кс 3",
"CX-5": "Кс 5",
"CX-9": "Кс 9",
"Mazda": "Мазда",
"Mazda CX-30": "Мазда CX-30",
"Mazda CX-5": "Мазда CX-5",
"Mazda MX-30": "Мазда MX-30",
"Mazda2": "Мазда 2",
"Mazda3": "Мазда 3",
"Mazda6": "Мазда 6",
"MX-30": "Мазда MX-30",
"MX-5": "МХ 5",
"RX-8": "РХ 8",

# Mercedes-Benz
"A-Class": "А-Класс",
"AMG GT": "АМГ ГТ",
"B-Class": "Б-Класс",
"C-Class": "С-Класс",
"CLA": "CLA",
"CLS": "ЦЛС",
"Citan": "Ситан",
"E-Class": "Е-Класс",
"EQC": "ЭКВЦ",
"G-Class": "Г-Класс",
"GLA": "GLA",
"GLC": "ГЛЦ",
"GLE": "ГЛЕ",
"GLE Coupe": "ГЛЕ Купе",
"GLS": "ГЛС",
"Mercedes": "Мерседес",
"Mercedes-Benz": "Мерседес-Бенц",
"S-Class": "Си-Класс",
"SL-Class": "СЛ-Класс",
"Sprinter": "Спринтер",
"V-Class": "В-Класс",
"Vito": "Вито",
"eVito": "еВито",
"eVito Tourer": "еВито Турайер",

# Maybach
"Maybach": "Майбах",

# MG
"MG": "МГ",

# Mini
"Cooper": "Купер",
"Mini": "Мини",
"Mini Cooper": "Мини Купер",

# Mitsubishi
"ASX": "АСХ",
"Delica": "Делика",
"Eclipse": "Иклипс",
"Eclipse Cross": "Иклепс Кросс",
"Galant": "Галант",
"L200": "L200",
"Lancer": "Лансер",
"Mitsubishi": "Мицубиси",
"Mitsubishi L300": "Л300",
"Outlander": "Аутлендер",
"Pajero": "Паджеро",

# Moskvitch
"Moskvitch": "Москвич",
"Moskvitch 3": "Москвич 3",
"Moskvitch 403": "Москвич 403",
"Moskvitch 412": "Москвич 412",
"Moskvitch Aleko": "Москвич Алеко",
"Moskvitch EV": "Москвич электромобиль",

# Neta
"Neta": "Нета",

# NIO
"EC6": "Нио EC6",
"ES6": "Нио ES6",
"ES7": "ES7",
"ES8": "Нио ES8",
"ET7": "Нио ET7",
"NIO": "Нио",
"NIO EC6": "Нио EC6",
"NIO ES6": "Нио ES6",
"NIO ES8": "Нио ES8",
"NIO ET7": "Нио ET7",

# Nissan
"370Z": "370З",
"Altima": "Альтима",
"Avalon": "Эвалон",
"e-NV200": "е-НВ200",
"GT-R": "ГТ-Р",
"Juke": "Джук",
"Leaf": "Лиф",
"Maxima": "Максима",
"Murano": "Муранo",
"Navara": "Навара",
"Nissan": "Ниссан",
"Nissan NV200": "НВ200",
"Nissan Patrol Y62": "Патрол Y62",
"NV300": "НВ300",
"NV400": "НВ400",
"Pathfinder": "Патфайндер",
"Patrol": "Патрол",
"Qashqai": "Кашкай",
"Rogue": "Роудж",
"Sentra": "Сентра",
"Tacoma": "Такома",
"Titan": "Титан",
"Tundra": "Тундра",
"Venza": "Венза",
"X-Trail": "Икс-Трэйл",

# Opel
"Astra": "Астра",
"Combo": "Комбо",
"Corsa": "Корса",
"Crossland": "Кроссленд",
"Grandland": "Грандленд",
"Insignia": "Инсигния",
"Mokka": "Мокка",
"Opel": "Опель",

# Ora
"Ora": "Ора",

# Peugeot
"208": "208",
"3008": "3008",
"308": "308",
"5008": "5008",
"508": "508",
"Boxer": "Боксер",
"Partner": "Партнёр",
"Peugeot": "Пежо",
"Peugeot Boxer": "Пежо Боксер",
"Peugeot Partner": "Пежо Партнёр",
"Rifter": "Рифтер",
"Traveller": "Травеллер",

# Polestar
"Polestar": "Полистар",
"Polestar 2": "Полистар 2",
"Polestar 3": "Полистар 3",

# Porsche
"911": "911",
"Cayman": "Кайман",
"Macan": "Макан",
"Porsche": "Порше",
"Taycan": "Тайкан",

# Renault
"Captur": "Каптюр",
"Clio": "Клио",
"Duster": "Дастер",
"Kangoo": "Кангру",
"Kangoo Express": "Кангру Экспресс",
"Kangoo Van": "Кангру Ван",
"Kangoo ZE": "Кангру ЗЕ",
"Koleos": "Колеос",
"Logan": "Логан",
"Master": "Мастер",
"Megane": "Меган",
"Renault": "Рено",
"Renault Kangoo Express": "Кангру Экспресс",
"Renault Master": "Мастер",
"Renault Master Van": "Мастер Фургон",
"Renault Trafic Passenger": "Трафик Пассенджер",
"Sandero": "Сандеро",
"Trafic": "Трафик",

# Rivian
"R1T": "R1T",
"Rivian": "Ривиан",

# Roewe
"Roewe": "Роу",

# Rostec Electric
"Rostec Electric": "Ростех электромобиль",

# SAIC
"SAIC": "САЙК",

# Seat
"Cupra": "Купра",
"Seat": "Сеат",

# Skoda
"Fabia": "Фабия",
"Karoq": "Кароак",
"Kodiaq": "Кодьяк",
"Octavia": "Октавия",
"Skoda": "Шкода",
"Skoda Enyaq": "Еняк",
"Superb": "Суперб",
"Yeti": "Йети",

# Smart
"Smart": "Смарт",
"Smart ForTwo": "Смарт Фор Ту",

# Subaru
"Ascent": "Асцент",
"BRZ": "BRZ",
"Crosstrek": "Кросстрек",
"Forester": "Форестер",
"Impreza": "Импреза",
"Legacy": "Легаси",
"Outback": "Аутбек",
"Subaru": "Субару",
"WRX": "ВРХ",
"XV": "Икс ВИ",

# Suzuki
"Ciaz": "Циаз",
"Ignis": "Игнис",
"Jimny": "Джимни",
"Suzuki": "Сузуки",
"Suzuki Carry": "Сузуки Кэрри",
"SX4": "ЭС 4",
"Swift": "Свифт",
"Vitara": "Витара",

# Tesla
"Cybertruck": "Кибертрак",
"Model 3": "Модель 3",
"Model S": "Модель S",
"Model X": "Модель X",
"Model Y": "Модель Y",
"Roadster": "Родстер",
"Semi": "Трейлер Semи",
"Tesla": "Тесла",
"Tesla Model Plaid": "Тесла Модель Плайд",

# Toyota
"Avensis": "Авенсис",
"Camry": "Камри",
"Corolla": "Королла",
"Corolla Cross": "Королла Кросс",
"Dyna": "Дайна",
"HiAce": "ХайЭйс",
"Highlander": "Хайлендер",
"Hilux": "Хайлюкс",
"Land Cruiser": "Ленд Крузер",
"Mirai": "Мираи",
"Prius": "Приус",
"Proace": "Проэйс",
"RAV4": "Рав 4",
"Sequoia": "Секвоия",
"Sienta": "Сента",
"Tacoma": "Такома",
"Toyota": "Тойота",
"Toyota Hiace Commuter": "ХайЭйс Комьютер",
"Toyota Proace City": "Проэйс Сити",
"Tundra": "Тундра",
"Venza": "Венза",
"Vios": "Виос",
"Yaris": "Ярис",
"C-HR": "C-HR",

# UAZ
"UAZ": "УАЗ",
"UAZ Cargo": "УАЗ Грузовик",
"UAZ Hunter": "УАЗ Хантер",
"UAZ Patriot": "УАЗ Патриот",
"UAZ Pickup": "УАЗ Пикап",
"UAZ Profi": "УАЗ Профи",

# Vauxhall
"Vauxhall": "Воксхолл",
"Vauxhall Astra": "Воксхолл Астра",
"Vauxhall Corsa": "Воксхолл Корса",

# Volkswagen
"Arteon": "Артеон",
"Caddy": "Кэдди",
"Crafter": "Крафтер",
"Golf": "Гольф",
"ID.3": "АйДи.3",
"ID.4": "АйДи.4",
"ID.Buzz": "АйДи.Базз",
"ID. Buzz": "АйДи Базз",
"Jetta": "Джетта",
"Multivan": "Мультивэн",
"Passat": "Пассат",
"Polo": "Поло",
"Scirocco": "Широкко",
"T-Roc": "Т-Рок",
"Tiguan": "Тигуан",
"Touareg": "Туарег",
"Transporter": "Транспортер",
"Up!": "Ап!",
"Volkswagen": "Фольксваген",
"Volkswagen Amarok": "Фольксваген Амарок",
"Volkswagen Caddy": "Фольксваген Кэдди",
"Volkswagen Caravelle": "Каравелле",
"Volkswagen Transporter": "Фольксваген Транспортер",

# Volvo
"S60": "S60",
"S90": "S90",
"V60": "V60",
"Volvo": "Вольво",
"XC40": "XC40",
"XC60": "XC60",
"XC90": "XC90",

# Wey
"Wey": "Вей",

# Wuling
"Wuling": "Вулинг",
"Wuling Hongguang": "Вулинг Хонггуан",
"Wuling Rongguang": "Вулинг Жунгуан",
"Wuling Sunshine": "Вулинг Саншайн",

# XPeng
"G3": "ХПэнг G3",
"G9": "ХПэнг G9",
"P7": "ХПэнг P7",
"XPeng": "ХПэнг",
"XPeng G3": "ХПэнг G3",
"XPeng G9": "ХПэнг G9",
"XPeng P7": "ХПэнг P7",

# Yema
"Yema": "Йема",

# ZAZ
"ZAZ": "Заз",

# Zetta
"Zetta": "Зетта",

# Общие категории
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
}

# --- Загрузка файла словаря при запуске ---
ADDITIONS_FILE = "additional_brands.json"

if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            saved_dict = json.load(f)
            if isinstance(saved_dict, dict):
                car_brands_models.update({str(k): str(v) for k, v in saved_dict.items()})
    except Exception:
        pass

def save_dictionary_to_file(dictionary: Dict[str, str], filename: str = ADDITIONS_FILE):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(dictionary, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.exception(f"Ошибка при сохранении файла: {e}")

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
            df = pd.read_csv(io.StringIO(dict_bytes.decode("utf-8")))
            if len(df.columns) >= 2:
                loaded_dict = {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
        elif filename_lower.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(dict_bytes), engine='openpyxl')
            if len(df.columns) >= 2:
                loaded_dict = {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
        else:
            st.error("Некорректный тип файла.")
            return
        if loaded_dict:
            car_brands_models.update(loaded_dict)
            save_dictionary_to_file(car_brands_models)
            st.success("Словарь обновлён из файла и сохранён.")
    except Exception as e:
        st.error(f"Ошибка при загрузке файла словаря: {e}")

def process_text(
    text: str,
    struct: dict,
    dict_brands_models: Dict[str, str],
    translit_enabled: bool
) -> Tuple[str, str]:
    """Обработка текста: поиск по словарю и транслиту, формирование строки без подсветки."""
    if not text:
        return text, ""
    search_terms = list(dict_brands_models.keys())

    # Создаем список для поиска: исходный текст и транслит
    texts_for_search = [text]
    translit_text = ''
    if translit_enabled:
        translit_text = transliterate(text, 'lat2cyr')
        texts_for_search.append(translit_text)

    matches_info = []
    for t in texts_for_search:
        t_lower = t.lower()
        for word in search_terms:
            word_lower = word.lower()
            start_idx = t_lower.find(word_lower)
            if start_idx != -1:
                end_idx = start_idx + len(word_lower)
                # Определяем позицию в оригинальном тексте
                if t is translit_text:
                    start_in_orig = 0
                    end_in_orig = len(text)
                else:
                    start_in_orig = start_idx
                    end_in_orig = end_idx
                matches_info.append((start_in_orig, end_in_orig, word))
    # Формируем строку без подсветки
    if matches_info:
        parts = []
        for start, end, w in matches_info:
            trans_word = dict_brands_models.get(w, "")
            original_segment = text[start:end]
            # Формируем: "оригинал/перевод"
            parts.append(f"{original_segment}/{trans_word}")
        translations_str = " / ".join(parts)
        result_str = f"{text} - ({translations_str})"
    else:
        result_str = text
    # Возвращаем обработанный текст и пустую строку вместо подсветки
    return result_str, ""

def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str, dict_brands_models: Dict[str, str], translit_enabled: bool) -> pd.DataFrame:
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.xlsx', '.xls']:
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
        except:
            df = pd.read_excel(io.BytesIO(file_bytes))
    elif ext == '.csv':
        try:
            df = pd.read_csv(io.StringIO(file_bytes.decode('utf-8')))
        except:
            df = pd.read_csv(io.StringIO(file_bytes.decode('cp1251', errors='replace')))
    else:
        df = pd.DataFrame()

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

    # --- Настройки словаря ---
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px">
        <h2 style="color:white;margin:0">🛠️ Настройки словаря</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Редактирование словаря марок и моделей автомобилей</p>
        </div>
        """,
        unsafe_allow_html=True,
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

    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px;margin-top:20px">
        <h2 style="color:white;margin:0">🚗 Обработка данных</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Загрузите файл (Excel или CSV), поиск и обработка</p>
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
