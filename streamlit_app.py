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
    # BMW
    "BMW": "БМВ",
    "1 Series": "1 Серия",
    "2 Series": "2 Серия",
    "3 Series": "3 Серия",
    "4 Series": "4 Серия",
    "5 Series": "5 Серия",
    "6 Series": "6 Серия",
    "7 Series": "7 Серия",
    "8 Series": "8 Серия",
    "X1": "Икс 1",
    "X2": "Икс 2",
    "X3": "Икс 3",
    "X4": "Икс 4",
    "X5": "Икс 5",
    "X6": "Икс 6",
    "X7": "Икс 7",
    "Z4": "Зет 4",
    "M3": "Эм 3",
    "M5": "Эм 5",
    "M2": "Эм 2",
    "M4": "Эм 4",

    # Mercedes-Benz
    "Mercedes-Benz": "Мерседес-Бенц",
    "Mercedes": "Мерседес",
    "A-Class": "А-Класс",
    "B-Class": "Б-Класс",
    "C-Class": "С-Класс",
    "E-Class": "Е-Класс",
    "S-Class": "Си-Класс",
    "CLA": "CLA",
    "GLA": "GLA",
    "GLC": "ГЛЦ",
    "GLE": "ГЛЕ",
    "GLS": "ГЛС",
    "G-Class": "Г-Класс",
    "CLS": "ЦЛС",
    "Vito": "Вито",
    "eVito": "еВито",
    "Sprinter": "Спринтер",
    "Citan": "Ситан",
    "V-Class": "В-Класс",

    # ГАЗ
    "GAZ": "Газ",
    "Gazel": "ГАЗель",
    "Gazel Business": "ГАЗель Бизнес",
    "Gazon Next": "Газон Некст",
    "GAZelle": "ГАЗель",
    "GAZelle Next": "ГАЗель Некст",
    "Sobol": "Соболь",
    "Sobol 4x4": "Соболь 4х4",

    # ЗАЗ
    "ZAZ": "Заз",

    # ВАЗ / Lada
    "Vaz": "Ваз",
    "Lada": "Лада",
    "Vesta": "Веста",
    "Granta": "Гранта",
    "Kalina": "Калина",
    "Niva": "Нива",
    "Lada Priora": "Лада Приора",
    "Lada 4x4": "Лада 4х4",
    "Lada XRay": "Лада Xray",
    "Lada Vesta SW": "Лада Веста Универсал",
    "Lada Vesta Cross": "Лада Веста Кросс",
    "Lada Granta Sedan": "Лада Гранта седан",
    "Lada Granta Liftback": "Лада Гранта хэтчбек",
    "Lada Niva Travel": "Лада Нива Тревел",
    "Lada Vesta Sport": "Лада Веста Спорт",
    "Lada XRAY Cross": "Лада ХРей Кросс",
    "Lada Granta Cross": "Лада Гранта Кросс",

    # UAZ
    "UAZ": "УАЗ",
    "UAZ Patriot": "УАЗ Патриот",
    "UAZ Hunter": "УАЗ Хантер",
    "UAZ Pickup": "УАЗ Пикап",
    "UAZ Profi": "УАЗ Профи",
    "UAZ Cargo": "УАЗ Грузовик",

    # Китайские бренды
    "BYD": "Байджи",
    "BYD Han": "Байджи Хан",
    "BYD Tang": "Байджи Танг",
    "BYD Song": "Байджи Сонг",
    "BYD Dolphin": "Байджи Дельфин",
    "BYD Atto 3": "Атто 3",
    "BYD Seal": "Байджи Сил",
    "BYD Yuan": "Байджи Юань",
    "BYD Qin": "Байджи Цин",
    "BYD Yuan EV": "Байджи Юань ЕВ",
    "Geely": "Джили",
    "Geely Atlas": "Джили Атлас",
    "Geely Coolray": "Джили Кулрэй",
    "Geely Emgrand": "Джили Эмгранд",
    "Geely Binrui": "Джили Бинрай",
    "Geely Atlas Pro": "Джили Атлас Про",
    "Geely Geometry": "Джили Геометрия",
    "Geely Preface": "Джили Префейс",
    "Haval": "Хавал",
    "Haval Jolion": "Хавал Джолион",
    "Haval H9": "Хавал Н9",
    "Haval H6": "Хавал H6",
    "Haval F7": "Хавал F7",
    "Haval H2": "Хавал H2",
    "Haval H5": "Хавал H5",
    "Lifan": "Лифан",
    "Lifan X60": "Лифан X60",
    "Lifan Myway": "Лифан Майвэй",
    "Lifan Solano": "Лифан Солано",
    "Lifan 820": "Лифан 820",
    "Lifan KPR": "Лифан КРП",
    "Chery": "Черри",
    "Chery Tiggo 2": "Черри Тигго 2",
    "Chery Tiggo 7": "Черри Тигго 7",
    "Chery Arrizo 5": "Черри Аризо 5",
    "Chery Tiggo 8": "Черри Тигго 8",
    "Chery QQ": "Черри QQ",
    "Chery Tiggo 3": "Черри Тигго 3",
    "SAIC": "САЙК",
    "MG": "МГ",
    "Roewe": "Роу",
    "Baojun": "Баоцзюнь",
    "Baojun 530": "Баоцзюнь 530",
    "Baojun 510": "Баоцзюнь 510",
    "Baojun RC-6": "Баоцзюнь RC-6",
    "Wuling": "Вулинг",
    "Wuling Hongguang": "Вулинг Хонггуан",
    "Wuling Rongguang": "Вулинг Жунгуан",
    "Wuling Sunshine": "Вулинг Саншайн",
    "JAC": "Джак",
    "JAC S2": "Джак S2",
    "JAC Refine S4": "Джак Рефайн S4",
    "JAC iEV": "Джак iEV",
    "NIO": "Нио",
    "NIO ES6": "Нио ES6",
    "NIO EC6": "Нио EC6",
    "NIO ET7": "Нио ET7",
    "NIO ES8": "Нио ES8",
    "XPeng": "ХПэнг",
    "XPeng P7": "ХПэнг P7",
    "XPeng G3": "ХПэнг G3",
    "XPeng G9": "ХПэнг G9",
    "Lynk & Co": "Линк & Ко",
    "Lynk & Co 01": "Линк & Ко 01",
    "Lynk & Co 03": "Линк & Ко 03",
    "Lynk & Co 05": "Линк & Ко 05",

    # Volkswagen
    "Volkswagen": "Фольксваген",
    "Golf": "Гольф",
    "Polo": "Поло",
    "Passat": "Пассат",
    "Tiguan": "Тигуан",
    "Touareg": "Туарег",
    "Jetta": "Джетта",
    "Arteon": "Артеон",
    "T-Roc": "Т-Рок",
    "Scirocco": "Широкко",
    "ID.3": "АйДи.3",
    "ID.4": "АйДи.4",
    "ID. Buzz": "АйДи Базз",
    "Up!": "Ап!",

    # Audi
    "Audi": "Ауди",
    "A1": "А1",
    "A3": "А3",
    "A4": "А4",
    "A6": "А6",
    "A8": "А8",
    "Q3": "Кью 3",
    "Q5": "Кью 5",
    "Q7": "Кью 7",
    "Q8": "Кью 8",
    "TT": "ТТ",
    "R8": "R8",
    "e-tron": "Е-Трон",
    "SQ5": "СКу 5",
    "SQ7": "СКу 7",
    "RS3": "РС 3",
    "RS5": "РС 5",
    "RS7": "РС 7",

    # Peugeot
    "Peugeot": "Пежо",
    "208": "208",
    "308": "308",
    "508": "508",
    "3008": "3008",
    "5008": "5008",
    "Partner": "Партнёр",
    "Peugeot Partner": "Пежо Партнёр",
    "Boxer": "Боксер",
    "Peugeot Boxer": "Пежо Боксер",
    "Rifter": "Рифтер",
    "Traveller": "Травеллер",

    # Renault
    "Renault": "Рено",
    "Clio": "Клио",
    "Megane": "Меган",
    "Captur": "Каптюр",
    "Kangoo": "Кангру",
    "Kangoo Van": "Кангру Ван",
    "Kangoo Express": "Кангру Экспресс",
    "Kangoo ZE": "Кангру ЗЕ",
    "Trafic": "Трафик",
    "Master": "Мастер",
    "Renault Master": "Мастер",
    "Renault Master Van": "Мастер Фургон",
    "Renault Kangoo Express": "Кангру Экспресс",
    "Renault Trafic Passenger": "Трафик Пассенджер",
    "Koleos": "Колеос",
    "Duster": "Дастер",
    "Sandero": "Сандеро",
    "Logan": "Логан",

    # Fiat
    "Fiat": "Фиат",
    "Panda": "Панда",
    "500": "500",
    "Tipo": "Типо",
    "Ducato": "Дукато",
    "Ducato Maxi": "Дукато Макси",
    "Fiat Ducato Maxi": "Дукато Макси",
    "Doblo": "Добло",
    "Fiorino": "Фиорино",
    "Talento": "Таленто",
    "Fiat Professional": "Фиат Профешионал",

    # Lancia
    "Lancia": "Ланча",

    # Alfa Romeo
    "Alfa Romeo": "Альфа Ромео",
    "Giulia": "Джулия",
    "Stelvio": "Стельвио",
    "Tonale": "Тонале",
    "4C": "4C",

    # Suzuki
    "Suzuki": "Сузуки",
    "Swift": "Свифт",
    "Ignis": "Игнис",
    "Vitara": "Витара",
    "Suzuki Carry": "Сузуки Кэрри",

    # Honda
    "Honda": "Хонда",
    "Accord": "Акорд",
    "Civic": "Сивик",
    "Fit": "Фит",
    "Jazz": "Джаз",
    "CR-V": "КР-В",
    "HR-V": "ХР-В",
    "Pilot": "Пилот",
    "Odyssey": "Одиссей",

    # Mitsubishi
    "Mitsubishi": "Мицубиси",
    "Outlander": "Аутлендер",
    "Pajero": "Паджеро",
    "Eclipse": "Иклипс",
    "Lancer": "Лансер",
    "ASX": "АСХ",
    "Delica": "Делика",
    "Galant": "Галант",

    # Lexus
    "Lexus": "Лексус",
    "RX": "РХ",
    "ES": "ЭС",
    "NX": "НКС",
    "UX": "ЮКС",
    "LS": "ЛС",
    "LX": "ЛКС",
    "RC": "РЦ",

    # Acura
    "Acura": "Акура",
    "TLX": "ТЛКС",
    "MDX": "МДХ",
    "RDX": "РДХ",
    "Integra": "Интегра",
    "RSX": "РСХ",

    "BMW": "БМВ",
    "1 Series": "1 Серия",
    "2 Series": "2 Серия",
    "3 Series": "3 Серия",
    "4 Series": "4 Серия",
    "5 Series": "5 Серия",
    "6 Series": "6 Серия",
    "7 Series": "7 Серия",
    "8 Series": "8 Серия",
    "X1": "Икс 1",
    "X2": "Икс 2",
    "X3": "Икс 3",
    "X4": "Икс 4",
    "X5": "Икс 5",
    "X6": "Икс 6",
    "X7": "Икс 7",
    "Z4": "Зет 4",
    "M3": "Эм 3",
    "M5": "Эм 5",
    "M2": "Эм 2",
    "M4": "Эм 4",

    # Mercedes-Benz
    "Mercedes-Benz": "Мерседес-Бенц",
    "Mercedes": "Мерседес",
    "A-Class": "А-Класс",
    "B-Class": "Б-Класс",
    "C-Class": "С-Класс",
    "E-Class": "Е-Класс",
    "S-Class": "Си-Класс",
    "CLA": "CLA",
    "GLA": "GLA",
    "GLC": "ГЛЦ",
    "GLE": "ГЛЕ",
    "GLS": "ГЛС",
    "G-Class": "Г-Класс",
    "CLS": "ЦЛС",
    "Vito": "Вито",
    "eVito": "еВито",
    "Sprinter": "Спринтер",
    "Citan": "Ситан",
    "V-Class": "В-Класс",

    # Toyota
    "Toyota": "Тойота",
    "Corolla": "Королла",
    "Camry": "Камри",
    "RAV4": "Рав 4",
    "Prius": "Приус",
    "Land Cruiser": "Ленд Крузер",
    "Yaris": "Ярис",
    "Highlander": "Хайлендер",
    "Hilux": "Хайлюкс",
    "Sienta": "Сента",
    "Avensis": "Авенсис",
    "HiAce": "ХайЭйс",
    "Proace": "Проэйс",
    "Dyna": "Дайна",
    "Toyota Hiace Commuter": "ХайЭйс Комьютер",
    "Toyota Proace City": "Проэйс Сити",
    "Corolla Cross": "Королла Кросс",
    "C-HR": "C-HR",

    # Mazda
    "Mazda": "Мазда",
    "Mazda3": "Мазда 3",
    "Mazda6": "Мазда 6",
    "Mazda2": "Мазда 2",
    "Mazda CX-30": "Мазда CX-30",
    "Mazda CX-5": "Мазда CX-5",
    "MX-5": "МХ 5",
    "MX-30": "Мазда MX-30",

    # Subaru
    "Subaru": "Субару",
    "Impreza": "Импреза",
    "Forester": "Форестер",
    "Outback": "Аутбек",
    "XV": "Икс ВИ",
    "BRZ": "BRZ",
    "Crosstrek": "Кросстрек",
    "Legacy": "Легаси",

    # Kia
    "Kia": "Киа",
    "Rio": "Рио",
    "Ceed": "Сид",
    "Sportage": "Спортейдж",
    "Sorento": "Соренто",
    "Soul": "Соул",
    "Optima": "Оптима",
    "Carnival": "Карнавал",
    "Stinger": "Стингер",
    "Kia Stonic": "Стонік",
    "Kia Seltos": "Селтос",
    "Kia EV6": "Киа EV6",
    "Kia EV9": "Киа EV9",

    # Hyundai
    "Hyundai": "Хёндай",
    "Elantra": "Элантра",
    "Sonata": "Соната",
    "Tucson": "Тусон",
    "Santa Fe": "Санта Фе",
    "Kona": "Кона",
    "Kona Electric": "Кона Электрик",
    "Palisade": "Палисад",
    "i30": "i30",
    "i20": "i20",
    "i4": "i4",
    "iX": "iX",
    "Hyundai Ioniq": "Ионик",
    "Ioniq 5": "Ионик 5",
    "Ioniq 6": "Ионик 6",
    "Hyundai Santa Cruz": "Санта Крус",

    # BYD
    "BYD": "Байджи",
    "Han": "Хан",
    "Tang": "Танг",
    "Song": "Сонг",
    "Dolphin": "Дельфин",
    "BYD Tang EV": "Танг ЕВ",
    "BYD Atto 3": "Атто 3",

    # Geely
    "Geely": "Джили",
    "Atlas": "Атлас",
    "Tiggo": "Тигго",
    "Tiggo 7": "Тигго 7",
    "Coolray": "Кулрэй",
    "Emgrand": "Эмгранд",
    "Binrui": "Бинрай",

    # Chery
    "Chery": "Черри",
    "Arrizo": "Аризо",
    "Exeed": "Эксид",

    # JAC
    "JAC": "Джак",
    "Refine": "Рефайн",

    # Lifan
    "Lifan": "Лифан",
    "F3": "Ф3",
    "F7": "Ф7",
    "Baojun": "Баоцзюнь",

    # Hongqi
    "Hongqi": "Хунци",
    "FAW": "Фав",
    "Bestune": "Бестюн",
    "Levdeo": "Левдео",
    "Wey": "Вей",
    "Yema": "Йема",

    # Лада
    "Lada": "Лада",
    "Vesta": "Веста",
    "Granta": "Гранта",
    "Kalina": "Калина",
    "Niva": "Нива",
    "Lada Priora": "Лада Приора",
    "Lada 4x4": "Лада 4х4",
    "Lada XRay": "Лада Xray",

    # UAZ
    "UAZ": "УАЗ",
    "UAZ Patriot": "УАЗ Патриот",
    "UAZ Hunter": "УАЗ Хантер",

    # ГАЗ
    "Gaz": "Газ",
    "GAZelle": "ГАЗель",
    "GAZelle Next": "ГАЗель Некст",
    "Sobol": "Соболь",
    "Sobol 4x4": "Соболь 4х4",

    # ЗАЗ
    "ZAZ": "Заз",

    # ВАЗ
    "Vaz": "Ваз",

    # Audi
    "Audi": "Ауди",
    "A1": "А1",
    "A3": "А3",
    "A4": "А4",
    "A6": "А6",
    "A8": "А8",
    "TT": "ТТ",
    "Q3": "Кью 3",
    "Q5": "Кью 5",
    "Q7": "Кью 7",
    "Q8": "Кью 8",
    "RS3": "Эр Эс 3",
    "RS5": "Эр Эс 5",
    "Q4 e-tron": "Кью 4 Етрэн",
    "RS Q3": "RS Кью 3",
    "e-tron GT": "Етрэн ГТ",
    "A5": "А 5",
    "A7": "А 7",
    "R8": "R8",

    # Volkswagen
    "Volkswagen": "Фольксваген",
    "Golf": "Гольф",
    "Polo": "Поло",
    "Passat": "Пассат",
    "Tiguan": "Тигуан",
    "Touareg": "Туарег",
    "Jetta": "Джетта",
    "Arteon": "Артеон",
    "Transporter": "Транспортер",
    "Caddy": "Кэдди",
    "Crafter": "Крафтер",
    "Volkswagen Caravelle": "Каравелле",
    "Multivan": "Мультивэн",
    "ID.3": "АйДи.3",
    "ID.4": "АйДи.4",
    "ID.Buzz": "АйДи.Базз",

    # Skoda
    "Skoda": "Шкода",
    "Octavia": "Октавия",
    "Superb": "Суперб",
    "Kodiaq": "Кодьяк",
    "Karoq": "Кароак",
    "Fabia": "Фабия",
    "Yeti": "Йети",
    "Skoda Enyaq": "Еняк",

    # Ford
    "Ford": "Форд",
    "Mustang": "Мустанг",
    "Ranger": "Рейнджер",
    "Bronco": "Бронко",
    "Transit": "Транзит",
    "Transit Custom": "Транзит Кастом",
    "Transit Connect": "Транзит Коннект",
    "Ford Transit Van": "Транзит Фургон",
    "Ford Courier": "Форд Курьер",
    "Ford Galaxy": "Форд Гэлакси",
    "e-Transit": "е-Транзит",
    "eSprinter": "еСпринтер",
    "eVito Tourer": "еВито Турайер",
    # Chevrolet
    "Chevrolet": "Шевроле",
    "Aveo": "Авео",
    "Lacetti": "Лачетти",
    "Malibu": "Мальбу",
    "Cruz": "Круз",
    "Equinox": "Экуинокс",
    "Blazer": "Блейзер",
    "Tahoe": "Тахо",
    "Silverado": "Сильверадо",
    "Chevrolet Express": "Экспресс",
    "Bolt EV": "Болт ЕВ",
    "Traverse": "Трэверс",
    "Spark": "Спарк",
    # Peugeot
    "Peugeot": "Пежо",
    "208": "208",
    "308": "308",
    "508": "508",
    "3008": "3008",
    "5008": "5008",
    "Partner": "Партнёр",
    "Peugeot Partner": "Пежо Партнёр",
    "Boxer": "Боксер",
    "Peugeot Boxer": "Пежо Боксер",
    "Rifter": "Рифтер",
    "Traveller": "Травеллер",
    # Renault
    "Renault": "Рено",
    "Clio": "Клио",
    "Megane": "Меган",
    "Captur": "Каптюр",
    "Kangoo": "Кангру",
    "Kangoo Van": "Кангру Ван",
    "Kangoo Express": "Кангру Экспресс",
    "Kangoo ZE": "Кангру ЗЕ",
    "Trafic": "Трафик",
    "Master": "Мастер",
    "Renault Master": "Мастер",
    "Renault Master Van": "Мастер Фургон",
    "Renault Kangoo Express": "Кангру Экспресс",
    "Renault Trafic Passenger": "Трафик Пассенджер",
    "Koleos": "Колеос",
    "Duster": "Дастер",
    "Sandero": "Сандеро",
    "Logan": "Логан",
    # Fiat
    "Fiat": "Фиат",
    "Panda": "Панда",
    "500": "500",
    "Tipo": "Типо",
    "Ducato": "Дукато",
    "Ducato Maxi": "Дукато Макси",
    "Fiat Ducato Maxi": "Дукато Макси",
    "Doblo": "Добло",
    "Fiorino": "Фиорино",
    "Talento": "Таленто",
    "Fiat Professional": "Фиат Профешионал",
    # Lancia
    "Lancia": "Ланча",
    # Alfa Romeo
    "Alfa Romeo": "Альфа Ромео",
    "Giulia": "Джулия",
    "Stelvio": "Стельвио",
    "Tonale": "Тонале",
    "4C": "4C",
    # Suzuki
    "Suzuki": "Сузуки",
    "Swift": "Свифт",
    "Ignis": "Игнис",
    "Vitara": "Витара",
    "Suzuki Carry": "Сузуки Кэрри",
    # Honda
    "Honda": "Хонда",
    "Accord": "Акорд",
    "Civic": "Сивик",
    "Fit": "Фит",
    "Jazz": "Джаз",
    "CR-V": "CR-V",
    "HR-V": "HR-V",
    "Pilot": "Пилот",
    "Odyssey": "Одиссея",
    # Mitsubishi
    "Mitsubishi": "Митсубиси",
    "Outlander": "Аутлендер",
    "Pajero": "Паджеро",
    "ASX": "ASX",
    "L200": "L200",
    "Mitsubishi L300": "Л300",
    "Eclipse Cross": "Иклепс Кросс",
    # Isuzu
    "Isuzu": "Исузу",
    "D-Max": "Ди-Макс",
    "Isuzu N-Series": "Исузу N-Серия",
    # Nissan
    "Nissan": "Ниссан",
    "Altima": "Альтима",
    "Sentra": "Сентра",
    "Maxima": "Максима",
    "Rogue": "Роудж",
    "X-Trail": "Икс-Трэйл",
    "Qashqai": "Кашкай",
    "Leaf": "Лиф",
    "Titan": "Титан",
    "Navara": "Навара",
    "Patrol": "Патрол",
    "Murano": "Муранo",
    "Avalon": "Эвалон",
    "Venza": "Венза",
    "Tacoma": "Такома",
    "Tundra": "Тундра",
    "Nissan NV200": "НВ200",
    "e-NV200": "е-НВ200",
    "NV300": "НВ300",
    "NV400": "НВ400",
    "Nissan Patrol Y62": "Патрол Y62",
    # Polestar
    "Polestar": "Полистар",
    "Polestar 2": "Полистар 2",
    "Polestar 3": "Полистар 3",
    # Lucid
    "Lucid": "Лусид",
    "Air": "Эйр",
    # Rivian
    "Rivian": "Ривиан",
    "R1T": "R1T",
    # NIO
    "NIO": "Нио",
    "ES6": "ES6",
    "ES7": "ES7",
    # XPeng
    "XPeng": "ХПэнг",
    "P7": "P7",
    # Tesla
    "Tesla": "Тесла",
    "Model S": "Модель S",
    "Model 3": "Модель 3",
    "Model X": "Модель X",
    "Model Y": "Модель Y",
    "Cybertruck": "Кибертрак",
    "Roadster": "Родстер",
    "Semi": "Трейлер Semи",
    "Tesla Model Plaid": "Тесла Модель Плайд",
    # Volvo
    "Volvo": "Вольво",
    "S60": "S60",
    "S90": "S90",
    "V60": "V60",
    "XC40": "XC40",
    "XC60": "XC60",
    "XC90": "XC90",
    # Seat
    "Seat": "Сеат",
    "Cupra": "Купра",
    # Porsche
    "Porsche": "Порше",
    "911": "911",
    "Cayman": "Кайман",
    "Macan": "Макан",
    "Taycan": "Тайкан",
    # Jaguar
    "Jaguar": "Ягуар",
    "Land Rover": "Ленд Ровер",
    "Range Rover": "Рендж Ровер",
    "Discovery": "Дискавери",
    # Mini
    "Mini": "Мини",
    "Cooper": "Купер",
    # Ferrari
    "Ferrari": "Феррари",
    "Roma": "Рома",
    "SF90": "SF90",
    "488": "488",
    "F8 Tributo": "F8 Трибуто",
    "296 GTB": "296 GTB",
    # Lamborghini
    "Lamborghini": "Ламборгини",
    "Huracan": "Уракан",
    "Urus": "Урус",
    "Aventador": "Авендадор",
    "Sián": "Сиан",
    # Maserati
    "Maserati": "Мазерати",
    "Ghibli": "Гибли",
    "Levante": "Леванте",
    "Quattroporte": "Кваттропорте",
    "MC20": "MC20",
    "GranTurismo": "Гран Туризмо",
    # GMC
    "GMC": "ДжиЭмСи",
    "Sierra": "Сиерра",
    # Cadillac
    "Cadillac": "Кадиллак",
    "Escalade": "Эскадил",
    # Dodge
    "Dodge": "Додж",
    "Challenger": "Челленджер",
    "Charger": "Чарджер",
    # Jeep
    "Jeep": "Джип",
    "Wrangler": "Рэнглер",
    "Grand Cherokee": "Гранд Чероки",
    # Great Wall
    "Great Wall": "Грейт Уолл",
    "Haval": "Хавал",
    "Haval H9": "Хавал Н9",
    "Ora": "Ора",
    "Neta": "Нета",
    "Wuling": "Вулинг",
    "Roewe": "Роу",
    # Commercial Vehicles and Utility Vehicles
    "Truck": "Грузовик",
    "Bus": "Автобус",
    "Trailer": "Прицеп",
    "Semi-trailer": "Полуприцеп",
    "Cargo Truck": "Грузовой автомобиль",
    "Reefer": "Изотермическая фура",
    "Tanker": "Автоцистерна",
    "Flatbed": "Платформа",
    "Dump Truck": "Самосвал",
    "Tipper": "Самосвальная техника",
    "Container Carrier": "Контейнеровоз",
    "Fire Engine": "Пожарная машина",
    "Ambulance": "Скорая помощь",
    "Mobile Crane": "Автомобильный кран",
    "Forklift": "Погрузчик",
    "Bulldozer": "Бульдозер",
    "Excavator": "Экскаватор",
    "Loader": "Погрузчик",
    "Crane Truck": "Кран-манипулятор",
    "Road Roller": "Каток дорожный",
    "Trash Collector": "Мусоровоз",
    "Snow Plow": "Снегоочистительная техника",
    "Utility Vehicle": "Спецтехника",
    "Construction Equipment": "Строительное оборудование",
    # Classic Cars
    "Classic Car": "Классический автомобиль",
    "Antique Car": "Антикварный автомобиль",
    "Muscle Car": "Мускул-кар",
    "Hot Rod": "Хотрод",
    "Convertible": "Кабриолет",
    "Retro Style": "Ретро-стиль",
    "Vintage Car": "Винтажный автомобиль",
    # Motorcycles
    "Motorcycle": "Мотоцикл",
    "Scooter": "Скутер",
    "Enduro Bike": "Эндуро",
    "Cruiser": "Круизер",
    "Touring Bike": "Туристический мотоцикл",
    "Sports Bike": "Спортбайк",
    "Off-Road Bike": "Внедорожный мотоцикл",
    "Dual Sport Bike": "Двухрежимный мотоцикл",
    "ATV": "Вездеход",
    "Quad Bike": "Квадроцикл",
    "Side-by-Side": "SSV (Side by Side)",
    "UTV": "Универсальное транспортное средство",
    "Three-Wheeler": "Трицикл",
    # Special Purpose Vehicles
    "Armored Car": "Бронированный автомобиль",
    "Security Vehicle": "Охрана и безопасность",
    "Medical Transport": "Медицинская перевозка",
    "Funeral Coach": "Катафалк",
    "Emergency Response": "Аварийно-спасательная служба",
    "Rescue Vehicle": "Спасательное транспортное средство",
    "Military Vehicle": "Военная техника",
    "Police Car": "Полиция",
    "Prison Transport": "Транспортировка заключенных",
    "Government Fleet": "Государственный автопарк",
    "Diplomatic Car": "Дипломатическое транспортное средство",
    # Локальные российские бренды
    "Vesta": "Веста",
    "Granta": "Гранта",
    "Kalina": "Калина",
    "Niva": "Нива",
    "Lada Priora": "Лада Приора",
    "Lada 4x4": "Лада 4х4",
    "Lada XRay": "Лада Xray",
    # UAZ
    "UAZ": "УАЗ",
    "UAZ Patriot": "УАЗ Патриот",
    "UAZ Hunter": "УАЗ Хантер",
    # ГАЗ
    "Gaz": "Газ",
    "GAZelle": "ГАЗель",
    "GAZelle Next": "ГАЗель Некст",
    "Sobol": "Соболь",
    "Sobol 4x4": "Соболь 4х4",
    # ЗАЗ
    "ZAZ": "Заз",
    # ВАЗ
    "Vaz": "Ваз"
    # Бренды и модели из Китая
    "BYD": "Байджи",
    "BYD Han": "Байджи Хан",
    "BYD Tang": "Байджи Танг",
    "BYD Song": "Байджи Сонг",
    "BYD Dolphin": "Байджи Дельфин",
    "BYD Atto 3": "Атто 3",
    "BYD Seal": "Байджи Сил",
    "BYD Yuan": "Байджи Юань",
    "BYD Qin": "Байджи Цин",
    "BYD Yuan EV": "Байджи Юань ЕВ",
    
    "Geely": "Джили",
    "Geely Atlas": "Джили Атлас",
    "Geely Coolray": "Джили Кулрэй",
    "Geely Emgrand": "Джили Эмгранд",
    "Geely Binrui": "Джили Бинрай",
    "Geely Atlas Pro": "Джили Атлас Про",
    "Geely Geometry": "Джили Геометрия",
    "Geely Preface": "Джили Префейс",
    
    "Haval": "Хавал",
    "Haval Jolion": "Хавал Джолион",
    "Haval H9": "Хавал Н9",
    "Haval H6": "Хавал H6",
    "Haval F7": "Хавал F7",
    "Haval H2": "Хавал H2",
    "Haval H5": "Хавал H5",
    
    "Lifan": "Лифан",
    "Lifan X60": "Лифан X60",
    "Lifan Myway": "Лифан Майвэй",
    "Lifan Solano": "Лифан Солано",
    "Lifan 820": "Лифан 820",
    "Lifan KPR": "Лифан КРП",
    
    "Chery": "Черри",
    "Chery Tiggo 2": "Черри Тигго 2",
    "Chery Tiggo 7": "Черри Тигго 7",
    "Chery Arrizo 5": "Черри Аризо 5",
    "Chery Tiggo 8": "Черри Тигго 8",
    "Chery QQ": "Черри QQ",
    "Chery Tiggo 3": "Черри Тигго 3",
    
    "SAIC": "САЙК",
    "MG": "МГ",
    "Roewe": "Роу",
    
    "Baojun": "Баоцзюнь",
    "Baojun 530": "Баоцзюнь 530",
    "Baojun 510": "Баоцзюнь 510",
    "Baojun RC-6": "Баоцзюнь RC-6",
    
    "Wuling": "Вулинг",
    "Wuling Hongguang": "Вулинг Хонггуан",
    "Wuling Rongguang": "Вулинг Жунгуан",
    "Wuling Sunshine": "Вулинг Саншайн",
    
    "JAC": "Джак",
    "JAC S2": "Джак S2",
    "JAC Refine S4": "Джак Рефайн S4",
    "JAC iEV": "Джак iEV",
    
    "NIO": "Нио",
    "NIO ES6": "Нио ES6",
    "NIO EC6": "Нио EC6",
    "NIO ET7": "Нио ET7",
    "NIO ES8": "Нио ES8",
    
    "XPeng": "ХПэнг",
    "XPeng P7": "ХПэнг P7",
    "XPeng G3": "ХПэнг G3",
    "XPeng G9": "ХПэнг G9",
    
    "Lynk & Co": "Линк & Ко",
    "Lynk & Co 01": "Линк & Ко 01",
    "Lynk & Co 03": "Линк & Ко 03",
    "Lynk & Co 05": "Линк & Ко 05",
    
    # Mercedes-Benz
    "Mercedes-Benz": "Мерседес-Бенц",
    "A-Class": "А-Класс",
    "B-Class": "Б-Класс",
    "C-Class": "С-Класс",
    "E-Class": "Е-Класс",
    "S-Class": "Си-Класс",
    "CLA": "CLA",
    "CLS": "CLS",
    "G-Class": "Г-Класс",
    "GLA": "ГЛА",
    "GLC": "ГЛЦ",
    "GLE": "ГЛЕ",
    "GLS": "ГЛС",
    "GLE Coupe": "ГЛЕ Купе",
    "EQC": "ЭКВЦ",
    "AMG GT": "АМГ ГТ",
    "SL-Class": "СЛ-Класс",
    "V-Class": "В-Класс",
    
    # Volkswagen
    "Volkswagen": "Фольксваген",
    "Golf": "Гольф",
    "Polo": "Поло",
    "Passat": "Пассат",
    "Tiguan": "Тигуан",
    "Touareg": "Туарег",
    "Jetta": "Джетта",
    "Arteon": "Артеон",
    "T-Roc": "Т-Рок",
    "Scirocco": "Широкко",
    "ID.3": "АйДи.3",
    "ID.4": "АйДи.4",
    "ID. Buzz": "АйДи Базз",
    "Up!": "Ап!",
    
    # Audi
    "Audi": "Ауди",
    "A1": "А1",
    "A3": "А3",
    "A4": "А4",
    "A6": "А6",
    "A8": "А8",
    "Q3": "Кью 3",
    "Q5": "Кью 5",
    "Q7": "Кью 7",
    "Q8": "Кью 8",
    "TT": "ТТ",
    "R8": "R8",
    "e-tron": "Е-Трон",
    "SQ5": "СКу 5",
    "SQ7": "СКу 7",
    "RS3": "РС 3",
    "RS5": "РС 5",
    "RS7": "РС 7",
    
    # BMW
    "BMW": "БМВ",
    "1 Series": "1 Серия",
    "2 Series": "2 Серия",
    "3 Series": "3 Серия",
    "4 Series": "4 Серия",
    "5 Series": "5 Серия",
    "6 Series": "6 Серия",
    "7 Series": "7 Серия",
    "8 Series": "8 Серия",
    "X1": "Икс 1",
    "X2": "Икс 2",
    "X3": "Икс 3",
    "X4": "Икс 4",
    "X5": "Икс 5",
    "X6": "Икс 6",
    "X7": "Икс 7",
    "Z4": "Зет 4",
    "M3": "Эм 3",
    "M5": "Эм 5",
    "M2": "Эм 2",
    "M4": "Эм 4",
    
    # Opel
    "Opel": "Опель",
    "Astra": "Астра",
    "Corsa": "Корса",
    "Insignia": "Инсигния",
    "Mokka": "Мокка",
    "Grandland": "Грандленд",
    "Crossland": "Кроссленд",
    "Combo": "Комбо",
    
    # Mercedes-Maybach
    "Maybach": "Майбах",
    
    # Smart
    "Smart": "Смарт",
    "Smart ForTwo": "Смарт Фор Ту",
    
    # Mini
    "Mini": "Мини",
    "Mini Cooper": "Мини Купер",
    
    # Volkswagen Commercial Vehicles
    "Volkswagen Transporter": "Фольксваген Транспортер",
    "Volkswagen Caddy": "Фольксваген Кэдди",
    "Volkswagen Amarok": "Фольксваген Амарок",
    
    # Opel/Vauxhall (для Британии)
    "Vauxhall": "Воксхолл",
    "Vauxhall Corsa": "Воксхолл Корса",
    "Vauxhall Astra": "Воксхолл Астра",
    
    # Toyota
    "Toyota": "Тойота",
    "Corolla": "Коромо́лла",
    "Camry": "Камри",
    "RAV4": "РАВ4",
    "Hilux": "Хайлюкс",
    "Yaris": "Ярис",
    "Prius": "Приус",
    "Highlander": "Хайлендер",
    "Venza": "Венза",
    "Land Cruiser": "Ленд Крузер",
    "Sienta": "Сиента",
    "C-HR": "ЦХР",
    "Vios": "Виос",
    "Sequoia": "Секвоия",
    "Mirai": "Мираи",
    
    # Honda
    "Honda": "Хонда",
    "Civic": "Сивик",
    "Accord": "Аккорд",
    "Fit": "Фит",
    "Jazz": "Джаз",
    "CR-V": "КР-В",
    "HR-V": "ХР-В",
    "Pilot": "Пилот",
    "Ridgeline": "Риджлайн",
    "NSX": "НСХ",
    "Odyssey": "Одиссей",
    
    # Nissan
    "Nissan": "Ниссан",
    "Altima": "Альтима",
    "Sentra": "Сентра",
    "Maxima": "Максима",
    "X-Trail": "Икс Трейл",
    "Juke": "Джук",
    "Qashqai": "Кашкай",
    "Leaf": "Лиф",
    "370Z": "370З",
    "GT-R": "ГТ-Р",
    "Titan": "Тайтан",
    "Pathfinder": "Патфайндер",
    "Murano": "Мурано",
    
    # Mazda
    "Mazda": "Мазда",
    "Mazda3": "Мазда 3",
    "Mazda6": "Мазда 6",
    "CX-3": "Кс 3",
    "CX-5": "Кс 5",
    "CX-9": "Кс 9",
    "MX-5": "МХ 5",
    "RX-8": "РХ 8",
    "BT-50": "БТ-50",
    
    # Subaru
    "Subaru": "Субару",
    "Impreza": "Импреза",
    "Forester": "Форестер",
    "Outback": "Аутбэк",
    "Legacy": "Леджаси",
    "BRZ": "БРЗ",
    "Ascent": "Асцент",
    "WRX": "ВРХ",
    
    # Mitsubishi
    "Mitsubishi": "Мицубиси",
    "Lancer": "Лансер",
    "Outlander": "Аутлендер",
    "Eclipse": "Иклипс",
    "Pajero": "Паджеро",
    "ASX": "АСХ",
    "Delica": "Делика",
    "Galant": "Галант",
    
    # Suzuki
    "Suzuki": "Сузуки",
    "Swift": "Свифт",
    "Vitara": "Витара",
    "Jimny": "Джимни",
    "SX4": "ЭС 4",
    "Ciaz": "Циаз",
    
    # Lexus (японский премиум бренд)
    "Lexus": "Лексус",
    "RX": "РХ",
    "ES": "ЭС",
    "NX": "НКС",
    "UX": "ЮКС",
    "LS": "ЛС",
    "LX": "ЛКС",
    "RC": "РЦ",
    "NX": "НКС",
    
    # Acura (американский бренд, связанный с Honda, но с японскими корнями)
    "Acura": "Акура",
    "TLX": "ТЛКС",
    "MDX": "МДХ",
    "RDX": "РДХ",
    "Integra": "Интегра",
    "RSX": "РСХ",
    
    # АвтоВАЗ (Lada)
    "Lada": "Лада",
    "Vesta": "Веста",
    "Granta": "Гранта",
    "Kalina": "Калина",
    "Largus": "Ларгус",
    "Niva": "Нива",
    "4x4": "Нива 4x4",
    "Lada XRay": "Лада ХРей",
    "Lada Priora": "Лада Приора",
    "Lada Samara": "Лада Самара",
    "Lada Vesta SW": "Лада Веста Универсал",
    "Lada Vesta Cross": "Лада Веста Кросс",
    "Lada Granta Sedan": "Лада Гранта седан",
    "Lada Granta Liftback": "Лада Гранта хэтчбек",
    "Lada 4x4 Urban": "Лада 4x4 Урбан",
    "Lada Largus Cross": "Лада Ларгус Кросс",
    
    # ГАЗ
    "GAZ": "ГАЗ",
    "Sobol": "Соболь",
    "Gazel": "ГАЗель",
    "Gazel Business": "ГАЗель Бизнес",
    "Gazon Next": "Газон Некст",
    "GAZ Volga": "Волга",
    "GAZ Sadko": "Садко",
    
    # УАЗ
    "UAZ": "УАЗ",
    "UAZ Hunter": "УАЗ Хантер",
    "UAZ Patriot": "УАЗ Патриот",
    "UAZ Pickup": "УАЗ Пикап",
    "UAZ Profi": "УАЗ Профи",
    "UAZ Cargo": "УАЗ Грузовик",
    
    # Москвич
    "Moskvitch": "Москвич",
    "Moskvitch 3": "Москвич 3",
    "Moskvitch 403": "Москвич 403",
    "Moskvitch 412": "Москвич 412",
    "Moskvitch Aleko": "Москвич Алеко",
    
    # Автомобили будущего и электромобили
    "Zetta": "Зетта",
    "EVolution": "Эволюция",
    "Moskvitch EV": "Москвич электромобиль",
    "KAMAZ Electric": "КамАЗ электромобиль",
    
    # КАМАЗ
    "KAMAZ": "КамАЗ",
    "KAMAZ Trucks": "КамАЗ грузовики",
    "KAMAZ Electric": "КамАЗ электромобиль",
    
    # Другие российские проекты
    "Aurus": "Аурус",
    "Aurus Senat": "Аурус Сенат",
    "Aurus Komendant": "Аурус Командант",
    
    # Новые российские электромобили и стартапы
    "Zetta": "Зетта",
    "EVolution": "Эволюция",
    "Rostec Electric": "Ростех электромобиль",
    
    # Разные модели и проекты
    "Lada Granta Cross": "Лада Гранта Кросс",
    "Lada Niva Travel": "Лада Нива Тревел",
    "Lada Vesta Sport": "Лада Веста Спорт",
    "Lada XRAY Cross": "Лада ХРей Кросс",

    # Модельные и проектные названия
    "Aurus": "Аурус",
    "Aurus Senat": "Аурус Сенат",
    "Aurus Komendant": "Аурус Командант",
    "Zetta": "Зетта",
    "EVolution": "Эволюция",
    "Moskvitch EV": "Москвич электромобиль",
    "KAMAZ": "КамАЗ",
    "KAMAZ Trucks": "КамАЗ грузовики",
    "KAMAZ Electric": "КамАЗ электромобиль",
    "Rostec Electric": "Ростех электромобиль"
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
