import streamlit as st
import pandas as pd
from transliterate import translit
import io

def main():
    st.title("Транслитерация Excel с добавлением оригинала")
    st.markdown("""
    Этот инструмент позволяет загружать Excel файл, выбрать один или несколько столбцов для транслитерации с кириллицы на латиницу,
    и скачать обновленный файл с оригинальными и транслитерированными данными.
    """)

    uploaded_file = st.file_uploader("Загрузите Excel файл (.xlsx)", type=["xlsx"])

    if uploaded_file:
        try:
            # Чтение файла
            df = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"Ошибка при чтении файла: {e}")
            return

        st.subheader("Исходные данные")
        st.dataframe(df)

        # Выбор столбцов для транслитерации
        columns = df.columns.tolist()
        selected_columns = st.multiselect(
            "Выберите столбцы для транслитерации",
            columns
        )

        if selected_columns:
            if st.button("Транслитерировать выбранные столбцы"):
                df_result = df.copy()
                for col in selected_columns:
                    # Создаём новый столбец с суффиксом '_transliterated'
                    new_col_name = f"{col}_transliterated"
                    df_result[new_col_name] = df_result[col].apply(transliterate_text)
                
                st.subheader("Результат")
                st.dataframe(df_result)

                # Создаем файл для скачивания
                output = io.BytesIO()
                df_result.to_excel(output, index=False)
                output.seek(0)

                st.download_button(
                    label="Скачать обновленный файл",
                    data=output,
                    file_name="transliterated.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.info("Пожалуйста, выберите хотя бы один столбец для транслитерации.")

def transliterate_text(text):
    """
    Транслитерирует кириллический текст в латиницу.
    Если текст NaN или не строка, возвращает исходное значение.
    """
    if pd.isna(text):
        return text
    try:
        return translit(str(text), 'ru', reversed=True)
    except Exception:
        return text

if __name__ == "__main__":
    main()
