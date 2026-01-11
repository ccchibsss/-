import streamlit as st
import pandas as pd
import io
import re

# Улучшенная функция транслитерации с латиницы в кириллицу (русский)
def latin_to_cyrillic(text: str) -> str:
    if not isinstance(text, str) or text == "":
        return text

    # упорядоченные соответствия (длинные пары сначала)
    mapping = [
       ("shch", "щ"), ("sch", "щ"), ("yo", "ё"), ("yu", "ю"), ("ya", "я"),
("zh", "ж"), ("kh", "х"), ("ts", "ц"), ("ch", "ч"), ("sh", "ш"),
("ye", "е"), ("ja", "я"), ("j", "й"), ("a", "а"), ("b", "б"),
("v", "в"), ("g", "г"), ("d", "д"), ("e", "е"), ("z", "з"),
("i", "и"), ("k", "к"), ("l", "л"), ("m", "м"), ("n", "н"),
("o", "о"), ("p", "п"), ("r", "р"), ("s", "с"), ("t", "т"),
("u", "у"), ("f", "ф"), ("y", "ы"), ("'", "ь"), ("\"", "ъ"),
("x", "кс"), ("q", "к"), ("w", "в"), ("c", "к"), ("h", "х"), ("ge", "ж")

    ]

    # Преобразование слова за словом, чтобы сохранять регистр
    def transliterate_word(word: str) -> str:
        lower = word.lower()
        i = 0
        res = ""
        while i < len(lower):
            matched = False
            for latin, cyr in mapping:
                if lower.startswith(latin, i):
                    res += cyr
                    i += len(latin)
                    matched = True
                    break
            if not matched:
                # остаются цифры, знаки и неизвестные символы - копируем как есть
                res += lower[i]
                i += 1

        # Восстановление регистра:
        if word.isupper():
            return res.upper()
        if word[0].isupper():
            # Сделать первую букву заглавной (остальное строчные)
            return res.capitalize()
        return res

    # Разбиваем на слова (с сохранением разделителей)
    parts = re.split(r'(\s+)', text)
    out_parts = []
    for part in parts:
        # слова и разделители
        if re.search(r'[A-Za-z]', part):
            # транслитерируем только если есть латинские буквы
            out = []
            # разделим на подслова по не-буквам чтобы оставить пунктуацию
            subparts = re.split(r'([^A-Za-z]+)', part)
            for sp in subparts:
                if re.search(r'[A-Za-z]', sp):
                    out.append(transliterate_word(sp))
                else:
                    out.append(sp)
            out_parts.append("".join(out))
        else:
            out_parts.append(part)
    return "".join(out_parts)

def contains_latin(s: str) -> bool:
    return bool(re.search(r'[A-Za-z]', str(s)))

def contains_cyrillic(s: str) -> bool:
    return bool(re.search(r'[\u0400-\u04FF]', str(s)))

def process_value_for_display(value):
    if pd.isna(value):
        return value
    s = str(value)
    # Если есть латиница — переводим в кириллицу и добавляем в скобках
    if contains_latin(s) and not contains_cyrillic(s):
        try:
            cyr = latin_to_cyrillic(s)
            if cyr and cyr != s:
                return f"{s} ({cyr})"
            else:
                return s
        except Exception:
            return s
    # Если уже кириллица — оставляем как есть
    return s

def main():
    st.set_page_config(page_title="Транслитерация Latin → Русский", layout="wide")
    st.title("Точнaя транслитерация с латиницы на русский")
    st.markdown(
        "Загрузите CSV или Excel. Инструмент переводит текст из латиницы в кириллицу (русский) и добавляет перевод в скобках: "
        "например 'Ivanov' → 'Ivanov (Иванов)'. Поддерживает большие CSV через чтение блоками."
    )

    uploaded_file = st.file_uploader("Загрузите файл (.xlsx или .csv)", type=["xlsx", "csv"])
    if not uploaded_file:
        st.info("Ожидание файла...")
        return

    file_name = uploaded_file.name.lower()
    is_excel = file_name.endswith(".xlsx") or file_name.endswith(".xls")
    is_csv = file_name.endswith(".csv")

    # Параметры
    if is_csv:
        encoding = st.selectbox("Кодировка CSV", ["utf-8", "cp1251", "latin1"], index=0)
        chunk_size = st.number_input("Размер блока (строк) при обработке CSV (0 = весь файл)", min_value=0, max_value=500000, value=10000, step=1000)
    else:
        encoding = None
        chunk_size = 0

    try:
        if is_excel:
            df = pd.read_excel(uploaded_file)
        else:
            # если chunk_size == 0 - читаем целиком
            if chunk_size and chunk_size > 0:
                reader = pd.read_csv(uploaded_file, encoding=encoding, chunksize=chunk_size)
                df = pd.concat(reader, ignore_index=True)
            else:
                df = pd.read_csv(uploaded_file, encoding=encoding)
    except Exception as e:
        st.error(f"Ошибка чтения файла: {e}")
        return

    st.subheader("Обзор данных")
    st.dataframe(df.head(200))

    columns = df.columns.tolist()
    if not columns:
        st.error("В файле нет столбцов.")
        return

    col = st.selectbox("Выберите столбец для преобразования (латиница → русский)", columns)
    download_format = st.radio("Формат выгрузки", ["CSV", "Excel"])

    if st.button("Обработать"):
        st.info("Начинаем обработку...")
        # Обработка
        df_result = df.copy()
        new_col_name = f"{col}_ru"
        df_result[new_col_name] = df_result[col].apply(process_value_for_display)

        st.success("Готово")
        st.subheader("Результат (первые строки)")
        st.dataframe(df_result.head(200))

        # Подготовка к скачиванию
        if download_format == "Excel":
            output = io.BytesIO()
            try:
                df_result.to_excel(output, index=False)
                output.seek(0)
                st.download_button("Скачать .xlsx", data=output, file_name="transliterated_result.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                st.error(f"Не удалось сохранить в Excel: {e}")
        else:
            # CSV (в памяти)
            try:
                csv_bytes = df_result.to_csv(index=False, encoding="utf-8").encode("utf-8")
                st.download_button("Скачать .csv", data=csv_bytes, file_name="transliterated_result.csv", mime="text/csv")
            except Exception as e:
                st.error(f"Не удалось сохранить в CSV: {e}")

if __name__ == "__main__":
    main()
