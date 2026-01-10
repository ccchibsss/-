import streamlit as st
import pandas as pd
from transliterate import translit
import io
import re

def main():
    st.title("Расширенный инструмент транслитерации с обработкой больших файлов")
    st.markdown("""
    Этот инструмент позволяет обрабатывать очень большие файлы Excel или CSV, транслитерировать выбранные столбцы,
    логировать процесс и скачивать результаты. Также автоматически переводит кириллицу в русский перед транслитерацией
    и добавляет перевод в скобках.
    """)

    # Логирование
    log_placeholder = st.empty()

    uploaded_file = st.file_uploader("Загрузите файл (.xlsx или .csv)", type=["xlsx", "csv"])

    if uploaded_file:
        file_type = None
        if uploaded_file.name.endswith('.xlsx'):
            file_type = 'excel'
        elif uploaded_file.name.endswith('.csv'):
            file_type = 'csv'

        encoding = 'utf-8'
        if file_type == 'csv':
            encoding = st.selectbox("Выберите кодировку CSV файла", ["utf-8", "latin1", "cp1251"])

        # Размер блока для чтения
        chunk_size = st.number_input("Укажите размер блока для обработки (строк)", min_value=1000, max_value=100000, value=10000, step=1000)

        try:
            # Чтение файла по частям
            if file_type == 'excel':
                df = pd.read_excel(uploaded_file)
            else:
                # Чтение CSV по частям
                df_chunks = pd.read_csv(uploaded_file, chunksize=chunk_size, encoding=encoding)
                df = pd.concat(df_chunks, ignore_index=True)
            log_placeholder.info("Файл успешно загружен.")
        except Exception as e:
            st.error(f"Ошибка при чтении файла: {e}")
            return

        st.subheader("Исходные данные")
        st.dataframe(df)

        columns = df.columns.tolist()
        selected_column_for_translit = st.selectbox("Выберите столбец для транслитерации", columns)

        if selected_column_for_translit:
            # Меню выбора формата для скачивания
            download_format = st.radio("Выберите формат для скачивания", ["CSV", "Excel"])

            # Функции для автоматического перевода кириллицы в русский
            def is_cyrillic(text):
                return bool(re.search('[\u0400-\u04FF]', str(text)))

            def process_value(value):
                value_str = str(value)
                if is_cyrillic(value_str):
                    try:
                        rus_text = translit(value_str, 'ru', reversed=True)
                    except Exception:
                        rus_text = value_str
                    return f"{value_str} ({rus_text})"
                else:
                    return value_str

            if st.button("Начать обработку и транслитерацию"):
                df_result = df.copy()

                # Применяем функцию обработки
                df_result[f"{selected_column_for_translit}_processed"] = df_result[selected_column_for_translit].apply(process_value)

                st.subheader("Результат")
                st.dataframe(df_result)

                # Создание файла для скачивания
                output = io.BytesIO()
                if download_format == "Excel":
                    df_result.to_excel(output, index=False)
                    filename = "transliterated_result.xlsx"
                    mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                else:
                    df_result.to_csv(output, index=False, encoding='utf-8')
                    filename = "transliterated_result.csv"
                    mime_type = "text/csv"
                output.seek(0)

                st.download_button(
                    label="Скачать файл",
                    data=output,
                    file_name=filename,
                    mime=mime_type
                )
        else:
            st.info("Пожалуйста, выберите столбец для транслитерации.")

if __name__ == "__main__":
    main()
