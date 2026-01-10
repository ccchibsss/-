import streamlit as st
import pandas as pd
from transliterate import translit
import io
import time

def main():
    st.title("Расширенный инструмент транслитерации с обработкой больших файлов")
    st.markdown("""
    Этот инструмент позволяет обрабатывать очень большие файлы Excel или CSV, транслитерировать выбранные столбцы,
    логировать процесс и скачивать результаты.
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
        selected_columns = st.multiselect("Выберите столбцы для транслитерации", columns)

        if selected_columns:
            if st.button("Начать обработку и транслитерацию"):
                df_result = pd.DataFrame()
                total_rows = len(df)
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Обработка по частям для больших файлов
                total_chunks = (total_rows // chunk_size) + 1
                processed_rows = 0

                # Обработка по частям
                for chunk in (pd.read_excel(uploaded_file, chunksize=chunk_size) if file_type == 'excel' else pd.read_csv(uploaded_file, chunksize=chunk_size, encoding=encoding)):
                    # Обработка выбранных столбцов
                    for col in selected_columns:
                        new_col = f"{col}_transliterated"
                        chunk[new_col] = chunk[col].apply(transliterate_text)
                    df_result = pd.concat([df_result, chunk], ignore_index=True)
                    processed_rows += len(chunk)
                    progress = processed_rows / total_rows
                    progress_bar.progress(progress)
                    status_text.text(f"Обработано строк: {processed_rows}/{total_rows}")
                st.success("Обработка завершена!")

                st.subheader("Результат")
                st.dataframe(df_result)

                # Создание файла для скачивания
                output = io.BytesIO()
                if file_type == 'excel':
                    df_result.to_excel(output, index=False)
                else:
                    df_result.to_csv(output, index=False, encoding=encoding)
                output.seek(0)

                st.download_button(
                    label="Скачать файл",
                    data=output,
                    file_name="transliterated_full.xlsx" if file_type == 'excel' else "transliterated_full.csv",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if file_type == 'excel' else "text/csv"
                )

        else:
            st.info("Пожалуйста, выберите хотя бы один столбец для транслитерации.")

def transliterate_text(text):
    if pd.isna(text):
        return text
    try:
        return translit(str(text), 'ru', reversed=True)
    except Exception:
        return text

if __name__ == "__main__":
    main()
