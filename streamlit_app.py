import streamlit as st
import pandas as pd
from transliterate import translit

def main():
    st.title("Транслитерация с добавлением оригинала в Excel")

    uploaded_file = st.file_uploader("Загрузите Excel-файл (.xlsx)", type=["xlsx"])

    if uploaded_file:
        # Чтение файла
        df = pd.read_excel(uploaded_file)
        st.write("Доступные столбцы:", df.columns.tolist())

        # Выбор столбца для обработки
        column_name = st.selectbox("Выберите столбец для транслитерации", df.columns)

        # Функция для транслитерации и добавления оригинала
        def transliterate_with_original(text):
            try:
                # Транслитерация кириллицы в латиницу
                transliterated = translit(str(text), 'ru', reversed=True)
                # Возвращаем строку с транслит и оригиналом
                return f"{transliterated} ({text})"
            except:
                # В случае ошибки возвращаем оригинал
                return str(text)

        # Обработка выбранного столбца
        new_column_name = f"{column_name}_translit"
        df[new_column_name] = df[column_name].apply(transliterate_with_original)

        # Показываем результат
        st.write("Результат:")
        st.dataframe(df[[column_name, new_column_name]])

        # Предлагаем скачать обработанный файл
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Скачать обработанный файл",
            data=csv,
            file_name="transliterated.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if __name__ == "__main__":
    main()
