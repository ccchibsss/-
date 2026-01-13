#!/usr/bin/env python3
# Полный исправленный и улучшенный код

from __future__ import annotations
import io
import os
import re
import json
import requests
import pandas as pd
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Optional, Dict, Set, List, Any, Union

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

# Базовые данные (обновлены и без дублирования)
car_brands_models = {
    # Acura
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
"Truck": "Грузовик",
"UTV": "Универсальное транспортное средство",
"Utility Vehicle": "Спецтехника",
"Vintage Car": "Винтажный автомобиль"
}

# Глобальные переменные для дополнений
added_pairs: Dict[str, str] = {}

# Загрузка дополнительных данных
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
    if not word or not morph:
        return word
    try:
        p = morph.parse(word)[0]
        inf = p.inflect({"nomn"})
        return inf.word if inf else p.word
    except Exception:
        return word

# Правила транслитерации
LAT_TO_CYR_RULES = [
    ("shch", "щ"), ("sht", "шт"), ("sci", "щи"), ("sch", "щ"),
    ("oye", "ое"), ("oyu", "ою"), ("iya", "ия"), ("iye", "ие"),
    ("aye", "ая"), ("ayu", "аю"), ("eyu", "ею"), ("iu", "ю"),
    ("ia", "ия"), ("ya", "я"), ("yo", "ё"), ("yu", "ю"),
    ("zh", "ж"), ("ge", "ж"), ("j", "ж"), ("g", "ж"),
    ("kh", "х"), ("h", "х"), ("x", "х"),
    ("ts", "ц"), ("tz", "ц"),
    ("ch", "ч"),
    ("sh", "ш"),
    ("ye", "е"), ("i", "и"), ("j", "й"),
    ("ju", "ю"), ("ja", "я"),
    ("a", "а"), ("b", "б"), ("v", "в"), ("g", "г"), ("d", "д"),
    ("e", "е"), ("z", "з"), ("i", "и"), ("k", "к"), ("l", "л"),
    ("m", "м"), ("n", "н"), ("o", "о"), ("p", "п"), ("r", "р"),
    ("s", "с"), ("t", "т"), ("u", "у"), ("f", "ф"), ("y", "ы"),
    ("j", "й"), ("'", "ь"), ('"', "ъ"),
    ("x", "кс"), ("q", "к"), ("w", "в")
]
# Сортировка для правильной обработки
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
        brand = re.sub(r'^[\(\s]+|[\)\s]+', '', brand_candidate)
        model = re.sub(r'^[\(\s]+|[\)\s]+', '', model_candidate)

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

def load_dictionary(source: Optional[str] = None, fileobj: Optional[io.BytesIO] = None) -> Dict[str, str]:
    """Загрузка словаря из файла или URL"""
    result: Dict[str, str] = {}
    try:
        if fileobj is not None:
            # Загрузка из файла streamlit или файла
            if source and source.lower().endswith('.json'):
                content = fileobj.getvalue().decode('utf-8')
                data = json.loads(content)
            elif source and source.lower().endswith(('.csv', '.xls', '.xlsx')):
                if source.lower().endswith('.csv'):
                    df = pd.read_csv(fileobj)
                else:
                    df = pd.read_excel(fileobj)
                if len(df.columns) >= 2:
                    result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        elif source:
            # Загрузка из URL или файла
            if source.startswith('http'):
                resp = requests.get(source, timeout=10)
                if source.lower().endswith('.json'):
                    data = resp.json()
                else:
                    if source.lower().endswith('.csv'):
                        df = pd.read_csv(io.StringIO(resp.text))
                    else:
                        df = pd.read_excel(io.BytesIO(resp.content))
                    if len(df.columns) >= 2:
                        result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
            else:
                # Локальный файл
                if source.lower().endswith('.json'):
                    with open(source, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                elif source.lower().endswith('.csv'):
                    df = pd.read_csv(source)
                else:
                    df = pd.read_excel(source)
                if len(df.columns) >= 2:
                    result = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        return {str(k): str(v) for k, v in result.items()}
    except Exception:
        return {}

def prepare_additions_fast(base_keys: set, candidates: set, threshold: float = 0.85) -> Dict[str, str]:
    """Автоматическое добавление по сходству"""
    additions: Dict[str, str] = {}
    for candidate in candidates:
        for base in base_keys:
            similarity_full = SequenceMatcher(None, candidate, base).ratio()
            if similarity_full >= threshold:
                additions[candidate] = base
                break
            else:
                # сравнение по буквам
                candidate_letters = sorted(candidate.lower())
                base_letters = sorted(base.lower())
                similarity_letters = SequenceMatcher(None, "".join(candidate_letters), "".join(base_letters)).ratio()
                if similarity_letters >= threshold:
                    additions[candidate] = base
                    break
    return additions

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
                loaded = load_dictionary(source=dict_file.name, fileobj=dict_file)
            except Exception as e:
                st.error(f"Не удалось загрузить файл словаря: {e}")
        elif dict_url:
            try:
                loaded = load_dictionary(source=dict_url)
            except Exception as e:
                st.error(f"Не удалось загрузить словарь по URL: {e}")
        if loaded:
            for k, v in loaded.items():
                car_brands_models[k] = v
            added_pairs.update(loaded)
            save_additions()
            st.success(f"Загружено {len(loaded)} пар из словаря")
        else:
            st.info("Словарь не загружен или пустой")

    sidebar.header("Добавить пару вручную")
    new_k = sidebar.text_input("Ключ (англ)")
    new_v = sidebar.text_input("Русское название")
    if sidebar.button("Добавить пару"):
        if new_k and new_v:
            car_brands_models[new_k] = new_v
            added_pairs[new_k] = new_v
            save_additions()
            st.success(f"Добавлено: {new_k} → {new_v}")
        else:
            st.error("Поля обязательны")

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

        # Автоматическое добавление с новой логикой
        additions = prepare_additions_fast(base_keys, candidates, threshold=threshold)
        if additions:
            for k, v in additions.items():
                car_brands_models[k] = v
            added_pairs.update(additions)
            save_additions()

        final_struct = build_final_struct(car_brands_models, additions)
        pattern = final_struct.get("pattern")
        mapping = final_struct.get("map", {})

        # Обработка данных
        df["Исходное"] = df[col]
        df["Обработанное"] = df[col].astype(str).apply(
            lambda v: process_text_fast(v, final_struct, translit_allowed=translit_allowed)
        )

        # Создаем HTML таблицу с подсветкой
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
                highlighted_value = pattern.sub(replacer, str(original)) if pattern else str(original)
                style = "background-color:#ffff99" if "<mark" in highlighted_value else ""
                icon = "🔍" if "<mark" in highlighted_value else "⚪"
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

        with st.expander("Полная таблица с исходным и обработанным"):
            st.dataframe(df[["Исходное", "Обработанное"]])

        # Экспорт
        export_format = st.radio("Формат экспорта", ("CSV", "Excel"))
        if export_format == "Excel":
            buf = io.BytesIO()
            df.to_excel(buf, index=False)
            buf.seek(0)
            st.download_button("Скачать Excel", buf, file_name="result.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
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
    
    # Чтение файла
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

    # Внешние данные
    ext_df = load_external_data(args.external) if args.external else pd.DataFrame()

    # Подготовка данных
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
    df["Обработанное"] = df[args.column].astype(str).apply(
        lambda v: process_text_fast(v, final_struct, translit_allowed=True)
    )

    # Сохранение результата
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
