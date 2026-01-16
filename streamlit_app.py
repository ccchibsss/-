#!/usr/bin/env python3
import io
import os
import json
import tempfile
from typing import Dict

import pandas as pd
import streamlit as st
import re

# Константы
ADDITIONS_FILE = "additional_brands.json"

# Начальный словарь брендов (обязательно вставьте весь ваш словарь сюда)
car_brands_models = {
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

# Расширенный словарь автоматического перевода en→ru
en_to_ru_map: Dict[str, str] = {
    "acura": "Акура",
    "integra": "Интегра",
    "mdx": "МДХ",
    "rdx": "РДХ",
    "rsx": "РСХ",
    "tlx": "ТЛКС",
    "aston martin": "Астон Мартин",
    "bugatti": "Бугатти",
    "ferrari": "Феррари",
    "lamborghini": "Ламборгини",
    "mclaren": "Макларен",
    "porsche": "Порше",
    "mercedes": "Мерседес",
    "bmw": "БМВ",
    "audi": "Ауди",
}

# --- Транслитерация ---
_LAT_TO_CYR = {
    'Shch':'Щ','shch':'щ','SHCH':'Щ',
    'Yo':'Ё','yo':'ё','YO':'Ё',
    'Zh':'Ж','zh':'ж','ZH':'Ж',
    'Kh':'Х','kh':'х','KH':'Х',
    'Ts':'Ц','ts':'ц','TS':'Ц',
    'Ch':'Ч','ch':'ч','CH':'Ч',
    'Sh':'Ш','sh':'ш','SH':'Ш',
    'Yu':'Ю','yu':'ю','YU':'Ю',
    'Ya':'Я','ya':'я','YA':'Я',
    "Y'":"Ы","y'":"ы",
    "E'":"Э","e'":"э",
    'A':'А','a':'а','B':'Б','b':'б','V':'В','v':'в','G':'Г','g':'г',
    'D':'Д','d':'д','E':'Е','e':'е','Z':'З','z':'з','I':'И','i':'и',
    'Y':'Й','y':'й','K':'К','k':'к','L':'Л','l':'л','M':'М','m':'м',
    'N':'Н','n':'н','O':'О','o':'о','P':'П','p':'п','R':'Р','r':'р',
    'S':'С','s':'с','T':'Т','t':'т','U':'У','u':'у','F':'Ф','f':'ф',
}

_CYR_TO_LAT = {
    'А':'A','а':'a','Б':'B','б':'b','В':'V','в':'v','Г':'G','г':'g',
    'Д':'D','д':'d','Е':'E','е':'e','Ё':'Yo','ё':'yo','Ж':'Zh','ж':'zh',
    'З':'Z','з':'z','И':'I','и':'i','Й':'Y','й':'y','К':'K','к':'k',
    'Л':'L','л':'l','М':'M','м':'m','Н':'N','н':'n','О':'O','о':'o',
    'П':'P','п':'p','Р':'R','р':'r','С':'S','с':'s','Т':'T','т':'t',
    'У':'U','у':'u','Ф':'F','ф':'f','Х':'Kh','х':'kh','Ц':'Ts','ц':'ts',
    'Ч':'Ch','ч':'ch','Ш':'Sh','ш':'sh','Щ':'Shch','щ':'shch',
    'Ы':"Y'",'ы':"y'",'Э':"E'",'э':'e\'','Ю':'Yu','ю':'yu','Я':'Ya','я':'ya'
}

_LAT_KEYS = sorted(_LAT_TO_CYR.keys(), key=len, reverse=True)

def transliterate_latin_to_cyrillic(text: str) -> str:
    if not text:
        return text
    i = 0
    out = []
    L = len(text)
    while i < L:
        matched = False
        for k in _LAT_KEYS:
            if text.startswith(k, i):
                out.append(_LAT_TO_CYR[k])
                i += len(k)
                matched = True
                break
        if not matched:
            out.append(text[i])
            i += 1
    return ''.join(out)

def transliterate_cyrillic_to_latin(text: str) -> str:
    if not text:
        return text
    return ''.join(_CYR_TO_LAT.get(ch, ch) for ch in text)

def transliterate(text: str, direction: str = 'lat2cyr') -> str:
    if direction == 'lat2cyr':
        return transliterate_latin_to_cyrillic(text)
    if direction == 'cyr2lat':
        return transliterate_cyrillic_to_latin(text)
    return text

# --- Вспомогательные функции ---
def safe_save_json(dictionary: Dict, filename: str = ADDITIONS_FILE):
    try:
        dirn = os.path.dirname(filename) or '.'
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=dirn, delete=False) as tmp:
            json.dump(dictionary, tmp, ensure_ascii=False, indent=2)
            tmp_name = tmp.name
        os.replace(tmp_name, filename)
        print("Saved json to", filename)
    except Exception as e:
        print("Error saving json:", e)

def load_additional_dict(filename: str = ADDITIONS_FILE) -> Dict:
    if not os.path.exists(filename):
        return {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            obj = json.load(f)
            if isinstance(obj, dict):
                return {str(k): str(v) for k, v in obj.items()}
    except Exception:
        print("Error loading json")
    return {}

def detect_language(text: str) -> str:
    if not text:
        return 'ru'
    cyr = len(re.findall(r'[А-Яа-яЁё]', text))
    lat = len(re.findall(r'[A-Za-z]', text))
    return 'en' if lat > cyr else 'ru'

def preserve_case_replace(src: str, repl: str) -> str:
    if src.isupper():
        return repl.upper()
    if src.istitle():
        return repl.capitalize()
    return repl

_RE_TOK = re.compile(r'\w+|\s+|[^\w\s]+', flags=re.UNICODE)

# Ваша интегрированная функция обработки текста
def process_text(
    text: str,
    dict_brands_models: Dict,
    en_to_ru_map: Dict,
    translit_enabled: bool = True,
    enable_dict: bool = True,
    enable_en_ru: bool = True,
    enable_lat_cyr: bool = True
) -> str:
    if text is None:
        return ''
    original = text
    norm_map = {k.lower(): v for k, v in dict_brands_models.items()}

    tokens = _RE_TOK.findall(text)
    lang = detect_language(text)

    for i, tk in enumerate(tokens):
        if tk.strip() and tk.strip().isalnum():
            key = tk.lower()
            replacement: str = None

            # 1. Проверка в основном словаре
            if enable_dict and key in norm_map:
                replacement = preserve_case_replace(tk, norm_map[key])
            # 2. Проверка в en_to_ru_map для автоматического перевода
            elif enable_en_ru:
                if key in en_to_ru_map:
                    replacement = preserve_case_replace(tk, en_to_ru_map[key])
            # 3. Транслитерация (лат→кирилл)
            if not replacement and enable_lat_cyr and re.match(r'^[A-Za-z]+$', tk):
                trans = transliterate(tk, 'lat2cyr')
                if trans.lower() in norm_map:
                    replacement = preserve_case_replace(tk, norm_map[trans.lower()])
            if replacement is not None:
                tokens[i] = replacement

    joined = ''.join(tokens)

    # Финальный вывод
    if lang == 'en' and enable_lat_cyr:
        translit_text = transliterate(joined, 'lat2cyr')
        return f'"{original}" - ({translit_text})'
    else:
        return f'"{original}"'

# --- Основные функции обработки файлов ---
def read_dataframe_from_bytes(file_bytes: bytes, filename: str) -> pd.DataFrame:
    ext = os.path.splitext(filename)[1].lower()
    if ext in ('.xlsx', '.xls'):
        return pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    if ext == '.csv':
        for enc in ('utf-8', 'cp1251'):
            try:
                return pd.read_csv(io.StringIO(file_bytes.decode(enc)), dtype=str)
            except:
                continue
        return pd.read_csv(io.StringIO(file_bytes.decode('latin1')), dtype=str)
    return pd.DataFrame()

def process_file_for_processing(
    file_bytes: bytes,
    filename: str,
    col_name: str,
    dict_brands_models: Dict,
    translit_enabled: bool
) -> pd.DataFrame:
    df = read_dataframe_from_bytes(file_bytes, filename)
    if df.empty:
        raise ValueError("Файл не содержит данных или формат не поддерживается.")
    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден.")
    # Преобразуем все значения столбца в строки
    series = df[col_name].astype(str).fillna("")
    processed = [process_text(s, dict_brands_models, en_to_ru_map) for s in series]
    df_out = df.copy()
    df_out[col_name] = processed
    # Обеспечиваем типы строк для всех колонок
    for col in df_out.columns:
        df_out[col] = df_out[col].astype(str)
    return df_out

# Загружаем словарь при старте
car_brands_models.update(load_additional_dict(ADDITIONS_FILE))

# --- Streamlit UI ---
def run():
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")
    st.title("Обработка брендов/моделей")

    # Используем сессию для хранения словаря
    if 'car_brands_models' not in st.session_state:
        st.session_state['car_brands_models'] = car_brands_models.copy()

    # Флаги обработки
    enable_dict = st.checkbox("Обрабатывать по словарю", value=True)
    enable_en_ru = st.checkbox("Обрабатывать английский/русский", value=True)
    enable_lat_cyr = st.checkbox("Обрабатывать транслит (лат→кирилл)", value=True)

    # Загрузка файла словаря
    uploaded_dict_file = st.file_uploader("Загрузить файл словаря (JSON, CSV или XLSX)", type=["json", "csv", "xlsx"])
    if uploaded_dict_file:
        try:
            data = uploaded_dict_file.read()
            name = uploaded_dict_file.name.lower()
            new_dict = {}
            if name.endswith('.json'):
                obj = json.loads(data.decode('utf-8'))
                if isinstance(obj, dict):
                    new_dict = {str(k): str(v) for k, v in obj.items()}
            elif name.endswith('.csv'):
                df = pd.read_csv(io.StringIO(data.decode('utf-8')))
                if df.shape[1] >= 2:
                    new_dict = {str(k): str(v) for k, v in zip(df.iloc[:,0].astype(str), df.iloc[:,1].astype(str))}
            elif name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(data), engine='openpyxl')
                if df.shape[1] >= 2:
                    new_dict = {str(k): str(v) for k, v in zip(df.iloc[:,0].astype(str), df.iloc[:,1].astype(str))}
            if new_dict:
                st.session_state['car_brands_models'].clear()
                st.session_state['car_brands_models'].update(new_dict)
                safe_save_json(st.session_state['car_brands_models'], ADDITIONS_FILE)
                st.success("Словарь обновлён и сохранён.")
        except Exception as e:
            st.error(f"Ошибка при загрузке словаря: {e}")

    # Редактирование словаря вручную
    dict_text = "\n".join(f"{k},{v}" for k, v in st.session_state['car_brands_models'].items())
    edited_text = st.text_area("Редактировать словарь (каждая строка: латиница,кириллица)", value=dict_text, height=200)
    if st.button("Сохранить словарь"):
        new_dict = {}
        for line in edited_text.splitlines():
            if not line.strip():
                continue
            parts = line.split(",", 1)
            if len(parts) == 2:
                k, v = parts
                new_dict[k.strip()] = v.strip()
        if new_dict:
            st.session_state['car_brands_models'].clear()
            st.session_state['car_brands_models'].update(new_dict)
            safe_save_json(st.session_state['car_brands_models'], ADDITIONS_FILE)
            st.success("Словарь сохранён.")
        else:
            st.warning("Нет корректных строк для сохранения.")

    # Загрузка файлов
    uploaded_files = st.file_uploader("Загрузите один или несколько файлов", type=["xlsx", "xls", "csv"], accept_multiple_files=True)
    if uploaded_files:
        for uploaded_file in uploaded_files:
            try:
                file_bytes = uploaded_file.read()
                df_preview = read_dataframe_from_bytes(file_bytes, uploaded_file.name).head(5)
                if not df_preview.empty:
                    col_options = list(df_preview.columns)
                    col_name = st.selectbox(f"Выберите столбец для файла {uploaded_file.name}", options=col_options, key=uploaded_file.name)
                else:
                    col_name = st.text_input(f"Введите название столбца для файла {uploaded_file.name}", value="Название", key=uploaded_file.name)
                if st.button(f"Обработать {uploaded_file.name}"):
                    df_processed = process_file_for_processing(
                        file_bytes,
                        uploaded_file.name,
                        col_name,
                        st.session_state['car_brands_models'],
                        enable_lat_cyr
                    )
                    st.success(f"Обработка файла {uploaded_file.name} завершена")
                    st.dataframe(df_processed)
                    # Скачать как Excel
                    buf_xlsx = io.BytesIO()
                    df_processed.to_excel(buf_xlsx, index=False, engine='openpyxl')
                    buf_xlsx.seek(0)
                    st.download_button(f"Скачать {uploaded_file.name} как Excel", buf_xlsx, file_name=f"processed_{uploaded_file.name}", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    # Скачать как CSV
                    buf_csv = df_processed.to_csv(index=False).encode('utf-8')
                    st.download_button(f"Скачать {uploaded_file.name} как CSV", buf_csv, file_name=f"processed_{os.path.splitext(uploaded_file.name)[0]}.csv", mime="text/csv")
            except Exception as e:
                st.error(f"Ошибка при обработке файла {uploaded_file.name}: {e}")

if __name__ == "__main__":
    run()
