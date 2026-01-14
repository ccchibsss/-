#!/usr/bin/env python3
# Улучшенное, оптимизированное и более красочное приложение Streamlit для замены и предварительного просмотра брендов/моделей авто
import io
import os
import json
from functools import lru_cache
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st

# Необязательный морфологический анализатор (если есть — используется для склонения)
try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

CSV_ENCODING = "utf-8-sig"
ADDITIONS_FILE = "additional_brands.json"

# Исходный словарь брендов и моделей (может быть расширен пользователем)
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
"Trash Collector": "Мусоровоз",
}

# Загрузка сохранённых дополнений, если есть
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                car_brands_models.update({str(k): str(v) for k, v in loaded.items()})
    except Exception:
        pass

@lru_cache(maxsize=20000)
def decline_word_cached(word: str) -> str:
    """Возвращает склонённое слово в именительном падеже (если есть pymorphy2)."""
    if not word or morph is None:
        return word
    try:
        p = morph.parse(word)[0]
        inf = p.inflect({"nomn"})
        return inf.word if inf else p.word
    except Exception:
        return word

# ---------------- ПостроениеTrie и быстрый поиск ----------------
def build_final_struct(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Строит структуру trie и метаданные для быстрого поиска подстрок (без учёта регистра).
    Возвращает словарь с trie, отображением и максимальной длиной поиска.
    """
    final = {**(base_map or {})}
    if additions:
        final.update(additions)

    # Убираем пустые или некорректные ключи
    final = {k: v for k, v in final.items() if isinstance(k, str) and k.strip()}

    if not final:
        return {"trie": None, "map": {}, "max_len": 0}

    mapping: Dict[str, Tuple[str, str]] = {}
    max_len = 0

    # Создаём отображение по нижнему регистру
    for k, v in final.items():
        lk = k.lower()
        display = v if v is not None else k
        display_decl = decline_word_cached(str(display))
        mapping[lk] = (k, display_decl)
        if len(lk) > max_len:
            max_len = len(lk)

    # Строим trie
    trie = {}
    END = "_end_"
    for lk, pair in mapping.items():
        node = trie
        for ch in lk:
            node = node.setdefault(ch, {})
        node[END] = (lk, pair)

    return {"trie": trie, "map": mapping, "max_len": max_len}

def _is_word_char(c: str) -> bool:
    """Проверка, является ли символ частью слова."""
    return c.isalnum() or c == "_"

def get_matches(text: str, struct: Dict[str, Any]) -> List[Tuple[int, int, str, str]]:
    """
    Находит все совпадения в тексте по trie.
    Возвращает список кортежей: (start_idx, end_idx_невключительно, оригинальный_ключ, отображение).
    Соблюдаются границы слова, совпадение не должно быть внутри другого слова.
    """
    if not text or not struct or struct.get("trie") is None:
        return []

    trie = struct["trie"]
    text_l = text.lower()
    n = len(text_l)
    max_len = struct.get("max_len", n)
    END = "_end_"
    matches: List[Tuple[int, int, str, str]] = []

    for i in range(n):
        node = trie
        for j in range(i, min(n, i + max_len)):
            ch = text_l[j]
            if ch not in node:
                break
            node = node[ch]
            if END in node:
                lk, (orig_key, disp) = node[END]
                start = i
                end = j + 1  # исключение
                # Проверка границ слова
                if start > 0 and _is_word_char(text[start - 1]):
                    continue
                if end < n and _is_word_char(text[end]):
                    continue
                matches.append((start, end, orig_key, disp))
    # Сортировка и фильтрация перекрывающихся совпадений
    matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    filtered = []
    last_end = -1
    for s, e, ok, du in matches:
        if s >= last_end:
            filtered.append((s, e, ok, du))
            last_end = e
    return filtered

# ---------------- Визуализация и подсветка ----------------
HIGHLIGHT_STYLE = 'background:#ffd54f;color:#000;border-radius:4px;padding:1px 4px;'

def highlight_html(text: str, matches: List[Tuple[int, int, str, str]]) -> str:
    """Возвращает HTML с подсвеченными совпадениями."""
    if not matches:
        return escape_html(text)
    out = []
    last = 0
    for s, e, orig, disp in matches:
        out.append(escape_html(text[last:s]))
        snippet = escape_html(text[s:e])
        out.append(f'<mark style="{HIGHLIGHT_STYLE}">{snippet}</mark>')
        last = e
    out.append(escape_html(text[last:]))
    return "".join(out)

def escape_html(s: str) -> str:
    """Экранирование специальных HTML символов."""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))

def process_text(text: str, struct: Dict[str, Any]) -> Tuple[str, str]:
    """
    Обрабатывает текст:
    - возвращает (обычный_текст, html_подсветка)
    """
    if not text:
        return text, ""
    matches = get_matches(text, struct)
    html = highlight_html(text, matches)
    # Можно заменить совпадения на нормализованные формы, если нужно
    return text, html

# ---------------- Загрузка словарей ----------------
def load_dictionary(source: Optional[str] = None, fileobj: Optional[io.BytesIO] = None) -> Dict[str, str]:
    """
    Загружает словарь из файла (upload), URL или файла.
    Поддерживаются форматы JSON, CSV, Excel.
    """
    try:
        if fileobj is not None:
            # преобразуем в BytesIO
            if hasattr(fileobj, "read"):
                data = fileobj.read()
                b = io.BytesIO(data)
                name = getattr(fileobj, "name", "") or source or ""
            else:
                b = fileobj
                name = source or ""
            if name.lower().endswith(".json"):
                text = b.getvalue().decode("utf-8")
                obj = json.loads(text)
                return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
            if name.lower().endswith(".csv"):
                df = pd.read_csv(b)
            else:
                df = pd.read_excel(b)
            if len(df.columns) >= 2:
                return {str(k): str(v) for k, v in zip(df.iloc[:, 0], df.iloc[:, 1])}
            return {}
        if source:
            if source.startswith("http"):
                r = requests.get(source, timeout=10)
                r.raise_for_status()
                if source.lower().endswith(".json"):
                    obj = r.json()
                    return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
                if source.lower().endswith(".csv"):
                    df = pd.read_csv(io.StringIO(r.text))
                else:
                    df = pd.read_excel(io.BytesIO(r.content))
                if len(df.columns) >= 2:
                    return {str(k): str(v) for k, v in zip(df.iloc[:, 0], df.iloc[:, 1])}
            else:
                # локальный файл
                if source.lower().endswith(".json"):
                    with open(source, "r", encoding="utf-8") as f:
                        obj = json.load(f)
                        return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
                if source.lower().endswith(".csv"):
                    df = pd.read_csv(source)
                else:
                    df = pd.read_excel(source)
                if len(df.columns) >= 2:
                    return {str(k): str(v) for k, v in zip(df.iloc[:, 0], df.iloc[:, 1])}
    except Exception:
        return {}
    return {}

# ---------------- Обработка файла для поиска ----------------
def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str, struct: Dict[str, Any]) -> pd.DataFrame:
    """
    Загружает файл, обрабатывает указанный столбец, добавляя колонку с подсветкой.
    Возвращает DataFrame с новым столбцом для HTML-превью.
    """
    stream = io.BytesIO(file_bytes)
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(stream)
    else:
        df = pd.read_excel(stream)

    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден в файле.")

    # Обработка текста в столбце
    series = df[col_name].fillna("").astype(str)
    plain_list = []
    html_list = []

    for txt in series:
        plain, html = process_text(txt, struct)
        plain_list.append(plain)
        html_list.append(html)

    df[col_name] = plain_list
    preview_col = f"{col_name}_preview_html"
    df[preview_col] = html_list

    return df

# ---------------- UI Streamlit ----------------
def run():
    # Настройка страницы
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px">
        <h2 style="color:white;margin:0">🚗 Обработка данных и расширение словаря</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Быстрая подстановка и визуальный просмотр совпадений</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.write("")  # отступ

    # Локальный базовый словарь (можно редактировать)
    base_dict = car_brands_models.copy()
    struct = build_final_struct(base_dict)

    # Боковая панель: управление словарём
    with st.expander("🛠️ Настройки словаря", expanded=True):
        left, right = st.columns([2, 1])
        with left:
            uploaded_dict = st.file_uploader("📁 Загрузить файл словаря (json/csv/xlsx)", type=["json", "csv", "xls", "xlsx"])
            dict_url = st.text_input("🌐 Или указать URL (json/csv/xlsx)")
            if st.button("🔄 Загрузить/Обновить словарь"):
                with st.spinner("Загрузка..."):
                    loaded = {}
                    try:
                        if uploaded_dict:
                            loaded = load_dictionary(source=uploaded_dict.name, fileobj=uploaded_dict)
                        elif dict_url:
                            loaded = load_dictionary(source=dict_url)
                        if loaded:
                            # Обновление словаря
                            base_dict.update(loaded)
                            # Сохранение дополнений
                            with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
                                json.dump({str(k): str(v) for k, v in base_dict.items()}, f, ensure_ascii=False, indent=2)
                            # Обновление структуры
                            struct = build_final_struct(base_dict)
                            st.success(f"Словарь обновлён: +{len(loaded)} пар")
                        else:
                            st.info("Ничего не загружено (проверьте формат)")
                    except Exception as e:
                        st.error(f"Ошибка: {e}")

        with right:
            st.markdown("#### Текущий словарь (часть)")
            if base_dict:
                df_dict = pd.DataFrame(list(base_dict.items()), columns=["Ключ", "Значение"]).head(200)
                st.dataframe(df_dict)

            st.markdown("#### Добавить вручную")
            new_k = st.text_input("🔑 Новый ключ", key="nk")
            new_v = st.text_input("📝 Новое значение", key="nv")
            if st.button("➕ Добавить пару вручную"):
                if new_k and new_v:
                    base_dict[new_k] = new_v
                    # Обновление файла и структуры
                    with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
                        json.dump({str(k): str(v) for k, v in base_dict.items()}, f, ensure_ascii=False, indent=2)
                    struct = build_final_struct(base_dict)
                    st.success(f"Добавлено: '{new_k}':'{new_v}'")
                else:
                    st.warning("Заполните оба поля")

    st.markdown("---")

    # Обработка файла
    with st.expander("📝 Обработка файла", expanded=True):
        uploaded = st.file_uploader("📂 Выберите CSV/XLSX файл для обработки", type=["csv", "xls", "xlsx"])
        col_name = st.text_input("🔤 Имя столбца для обработки (или оставьте пустым для выбора)")

        # Предварительный просмотр колонок, если файл загружен без указания столбца
        if uploaded and not col_name:
            try:
                if uploaded.name.lower().endswith(".csv"):
                    df0 = pd.read_csv(uploaded, nrows=0)
                else:
                    df0 = pd.read_excel(uploaded, nrows=0)
                cols = df0.columns.tolist()
                if cols:
                    col_name = st.selectbox("📑 Выберите столбец", cols)
            except Exception:
                pass

        if uploaded and col_name:
            try:
                with st.spinner("Обрабатываем..."):
                    bytes_data = uploaded.read()
                    struct = build_final_struct(base_dict)
                    df_res = process_file_for_processing(bytes_data, uploaded.name, col_name, struct)
                st.success("✅ Обработка завершена")

                st.markdown("### Превью (с подсветкой совпадений)")
                # Отображение превью с подсветкой
                preview_col = f"{col_name}_preview_html"
                show_df = df_res.head(50).copy()
                html_table = show_df.to_html(escape=False, index=False)
                st.markdown(html_table, unsafe_allow_html=True)

                st.markdown("### Скачать результат")
                buf = io.BytesIO()
                if uploaded.name.lower().endswith(".csv"):
                    df_res.to_csv(buf, index=False, encoding=CSV_ENCODING)
                    buf.seek(0)
                    st.download_button("⬇️ Скачать CSV", buf, file_name="result.csv", mime="text/csv")
                else:
                    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                        df_res.to_excel(writer, index=False)
                    buf.seek(0)
                    st.download_button("⬇️ Скачать XLSX", buf, file_name="result.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                st.error(f"Ошибка обработки: {e}")

    st.markdown("---")
    st.caption("Подсветка не влияет на экспортируемые данные — она предназначена только для визуальной проверки.")

if __name__ == "__main__":
    run()
