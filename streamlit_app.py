import streamlit as st
import pandas as pd
import io
import re
import requests
import pymorphy2
from difflib import SequenceMatcher

# Расширенный словарь марок и моделей автомобилей
car_brands_models = {
    # Немецкие бренды
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
    "M Series": "Эм Серия",
    "Mercedes-Benz": "Мерседес-Бенц",
    "A-Class": "А-Класс",
    "B-Class": "Б-Класс",
    "C-Class": "С-Класс",
    "E-Class": "Е-Класс",
    "S-Class": "Си-Класс",
    "GLC": "ГЛЦ",
    "GLE": "ГЛЕ",
    "GLS": "ГЛС",
    "G-Class": "Г-Класс",
    "CLS": "ЦЛС",
    "Vito": "Вито",
    "Sprinter": "Спритер",
    # Японские бренды
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
    "Mazda": "Мазда",
    "Mazda3": "Мазда 3",
    "Mazda6": "Мазда 6",
    "CX-3": "Кс 3",
    "CX-5": "Кс 5",
    "CX-9": "Кс 9",
    "MX-5": "МХ 5",
    "Subaru": "Субару",
    "Impreza": "Импреза",
    "Forester": "Форестер",
    "Outback": "Аутбек",
    "XV": "Икс ВИ",
    # Корея
    "Kia": "Киа",
    "Rio": "Рио",
    "Ceed": "Сид",
    "Sportage": "Спортейдж",
    "Sorento": "Соренто",
    "Soul": "Соул",
    "Optima": "Оптима",
    "Carnival": "Карнавал",
    "Stinger": "Стингер",
    "Hyundai": "Хёндай",
    "Elantra": "Элантра",
    "Sonata": "Соната",
    "Tucson": "Тусон",
    "Santa Fe": "Санта Фе",
    "Kona": "Кона",
    "Veloster": "Велюстер",
    # Китай
    "BYD": "БайДжи",
    "Han": "Хан",
    "Tang": "Танг",
    "Song": "Сонг",
    "Dolphin": "Дельфин",
    "F3": "Ф3",
    "F7": "Ф7",
    "Geely": "Джили",
    "Atlas": "Атлас",
    "Tiggo": "Тигго",
    "Coolray": "Кулрэй",
    "Emgrand": "Эмгранд",
    "Binrui": "Бинрай",
    "Chery": "Черри",
    "Tiggo 7": "Тигго 7",
    "Arrizo": "Аризо",
    "Exeed": "Эксид",
    "JAC": "Джак",
    "Refine": "Рефайн",
    "S2": "Эс 2",
    "S3": "Эс 3",
    "Megan": "Меган",
    "Lifan": "Лифан",
    "Baojun": "Баоцзюнь",
    "Hongqi": "Хунци",
    "FAW": "Фав",
    "Bestune": "Бестюн",
    "Levdeo": "Левдео",
    "Wey": "Вей",
    "Yema": "Йема",
    # Русские
    "Lada": "Лада",
    "Vesta": "Веста",
    "Granta": "Гранта",
    "Kalina": "Калина",
    "Niva": "Нива",
    "UAZ": "УАЗ",
    "Gaz": "Газ",
    "ZAZ": "Заз",
    "Vaz": "Ваз",
    "Lada Priora": "Лада Приора",
    "Lada 4x4": "Лада 4х4",
    "Lada XRay": "Лада Xray",
    # Европейские бренды
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
    "RS3": "Эр Эс 3",
    "RS5": "Эр Эс 5",
    "TT": "ТТ",
    "Volkswagen": "Фольксваген",
    "Golf": "Гольф",
    "Passat": "Пассат",
    "Tiguan": "Тигуан",
    "Touareg": "Туарег",
    "Jetta": "Джетта",
    "Arteon": "Артеон",
    "Skoda": "Шкода",
    "Octavia": "Октавия",
    "Superb": "Суперб",
    "Kodiaq": "Кодьяк",
    "Karoq": "Кароак",
    "Fabia": "Фабия",
    "Yeti": "Йети",
    # Американские
    "Ford": "Форд",
    "Fiesta": "Фиеста",
    "Focus": "Фокус",
    "Mustang": "Мустанг",
    "Ranger": "Рейнджер",
    "Bronco": "Бронко",
    "Chevrolet": "Шевроле",
    "Aveo": "Авео",
    "Lacetti": "Лачетти",
    "Malibu": "Мальбу",
    "Trailblazer": "Трейлблейзер",
    "Tahoe": "Тахо",
    "Silverado": "Сильверадо",
    # Франция
    "Peugeot": "Пежо",
    "208": "208",
    "308": "308",
    "508": "508",
    "3008": "3008",
    "5008": "5008",
    "Expert": "Эксперт",
    "Renault": "Рено",
    "Clio": "Клио",
    "Megane": "Меган",
    "Captur": "Каптюр",
    "Kangoo": "Кангру",
    "Koleos": "Колеос",
    "Duster": "Дастер",
    "Logan": "Логан",
    "Sandero": "Сандеро",
    # Италия
    "Fiat": "Фиат",
    "Panda": "Панда",
    "500": "500",
    "Tipo": "Типо",
    "Lancia": "Ланча",
    "Alfa Romeo": "Альфа Ромео",
    "Giulia": "Джулия",
    "Stelvio": "Стельвио",
    # Другие
    "Suzuki": "Сузуки",
    "Honda": "Хонда",
    "Hyundai": "Хёндай",
    "Subaru": "Субару",
    "Dacia": "Дачия",
    "SsangYong": "СангЁнг",
}

# Инициализация морфологического анализатора
morph = pymorphy2.MorphAnalyzer()

def latin_to_cyrillic(text: str) -> str:
    if not isinstance(text, str) or text == "":
        return text
    rules = [
        ("shch", "щ"), ("sch", "щ"), ("sht", "шт"),
        ("oye", "ое"), ("oyu", "ою"), ("iya", "ия"),
        ("iye", "ие"), ("aye", "ая"), ("ayu", "аю"),
        ("eyu", "ею"), ("iu", "ю"), ("ia", "ия"),
        ("yo", "ё"), ("yu", "ю"), ("ya", "я"),
        ("zh", "ж"), ("kh", "х"), ("ts", "ц"),
        ("ch", "ч"), ("sh", "ш"), ("ye", "е"),
        ("ja", "я"), ("ju", "ю"), ("je", "е"),
        ("a", "а"), ("b", "б"), ("v", "в"), ("g", "г"),
        ("d", "д"), ("e", "е"), ("z", "з"), ("i", "и"),
        ("k", "к"), ("l", "л"), ("m", "м"), ("n", "н"),
        ("o", "о"), ("p", "п"), ("r", "р"), ("s", "с"),
        ("t", "т"), ("u", "у"), ("f", "ф"), ("y", "ы"),
        ("j", "й"), ("'", "ь"), ('"', "ъ"),
        ("x", "кс"), ("q", "к"), ("w", "в"),
    ]
    def transliterate_word(word):
        lower = word.lower()
        res = ""
        i = 0
        while i < len(lower):
            for lat, cyr in rules:
                if lower.startswith(lat, i):
                    res += cyr
                    i += len(lat)
                    break
            else:
                res += lower[i]
                i += 1
        if word.isupper():
            return res.upper()
        elif word[0].isupper():
            return res.capitalize()
        else:
            return res
    parts = re.split(r'(\s+)', text)
    out_parts = []
    for part in parts:
        if re.search(r'[A-Za-z]', part):
            subparts = re.split(r'([^A-Za-z]+)', part)
            for sp in subparts:
                if re.search(r'[A-Za-z]', sp):
                    out_parts.append(transliterate_word(sp))
                else:
                    out_parts.append(sp)
        else:
            out_parts.append(part)
    return "".join(out_parts)

def contains_latin(text):
    return bool(re.search(r'[A-Za-z]', str(text)))

def contains_cyrillic(text):
    return bool(re.search(r'[\u0400-\u04FF]', str(text)))

def decline_word(word):
    if not word:
        return word
    parse = morph.parse(word)
    if parse:
        p = parse[0]
        inflected = p.inflect({'nomn'})
        if inflected:
            return inflected.word
        else:
            return p.word
    return word

def load_external_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            from io import StringIO
            data = StringIO(response.text)
            df = pd.read_csv(data)
            return df
        else:
            st.error(f"Ошибка при загрузке данных: статус {response.status_code}")
    except Exception as e:
        st.error(f"Ошибка при загрузке внешних данных: {e}")
    return pd.DataFrame()

def process_text_with_all_sources(text, dictionary, dataset, external_df=None):
    if not isinstance(text, str):
        return text
    # Обработка латиницы
    if contains_latin(text) and not contains_cyrillic(text):
        cyr_text = latin_to_cyrillic(text)
        cyr_text = decline_word(cyr_text)
        return f"{text} ({cyr_text})"
    # Обработка русских названий
    for eng_name, ru_name in dictionary.items():
        pattern = re.escape(eng_name)
        regex = re.compile(r'\b' + pattern + r'\b', re.IGNORECASE)
        text = regex.sub(lambda m: f"{m.group(0)} ({decline_word(ru_name)})", text)
    # Расширение из dataset
    if dataset is not None:
        all_texts = dataset.astype(str).str.cat(sep=' ')
        words_in_text = set(re.findall(r'\b\w+\b', all_texts))
        for word in words_in_text:
            if word not in dictionary:
                for known_name in list(dictionary.keys()):
                    ratio = SequenceMatcher(None, word.lower(), known_name.lower()).ratio()
                    if ratio > 0.8:
                        dictionary[word] = word
                        st.write(f"Добавлено из данных: {word}")
                        pattern_new = re.escape(word)
                        regex_new = re.compile(r'\b' + pattern_new + r'\b', re.IGNORECASE)
                        text = regex_new.sub(lambda m: f"{m.group(0)} ({decline_word(word)})", text)
    # Обработка внешних данных
    if external_df is not None:
        all_ext_texts = external_df.astype(str).str.cat(sep=' ')
        ext_words = set(re.findall(r'\b\w+\b', all_ext_texts))
        for word in ext_words:
            if word not in dictionary:
                for known_name in list(dictionary.keys()):
                    ratio = SequenceMatcher(None, word.lower(), known_name.lower()).ratio()
                    if ratio > 0.8:
                        dictionary[word] = word
                        st.write(f"Добавлено из внешних данных: {word}")
                        pattern_new = re.escape(word)
                        regex_new = re.compile(r'\b' + pattern_new + r'\b', re.IGNORECASE)
                        text = regex_new.sub(lambda m: f"{m.group(0)} ({decline_word(word)})", text)
    return text

def main():
    st.set_page_config(page_title="Автообработка расширенная", layout="wide")
    st.title("Обработка названий автомобилей с расширением словаря и внешними источниками")
    st.markdown("Загрузите файл, укажите URL внешнего источника CSV и произведите обработку.")

    uploaded_file = st.file_uploader("Выберите файл для обработки", type=["xlsx", "csv"])
    external_url = st.text_input("URL внешнего источника данных (CSV)")

    if uploaded_file:
        try:
            if uploaded_file.name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(uploaded_file)
            else:
                df = pd.read_csv(uploaded_file)
        except Exception as e:
            st.error(f"Ошибка чтения файла: {e}")
            return

        columns = df.columns.tolist()
        col_name = st.selectbox("Выберите столбец для обработки", columns)

        external_df = None
        if external_url:
            external_df = load_external_data(external_url)

        if st.button("Обработать"):
            dataset = df[col_name]
            def process_cell(value):
                if pd.isna(value):
                    return value
                text = str(value)
                result = process_text_with_all_sources(text, car_brands_models, dataset, external_df)
                return result

            df[col_name] = df[col_name].apply(process_cell)

            st.subheader("Обработанные данные")
            st.dataframe(df.head(200))

            # Экспорт
            export_format = st.radio("Формат экспорта", ["CSV", "Excel"])
            if export_format == "Excel":
                buf = io.BytesIO()
                df.to_excel(buf, index=False)
                buf.seek(0)
                st.download_button("Скачать Excel", buf, "result.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                buf = io.BytesIO()
                df.to_csv(buf, index=False)
                buf.seek(0)
                st.download_button("Скачать CSV", buf, "result.csv", mime="text/csv")

if __name__ == "__main__":
    main()
