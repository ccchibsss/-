# Патч: безопасно добавить конвертацию единиц в методы экспорта и интерфейс,
# без немедленного падения если класс HighVolumeAutoPartsCatalog ещё не
# определён.
# Запустите этот скрипт в том же окружении, где определён ваш класс, или
# вызовите apply_patch(YourClass) вручную после определения класса.

import re
import io
import os
from pathlib import Path
from typing import Optional, List
import importlib
import sys

# Импорты опциональны — проверяются при применении патча
try:
    import pandas as pd
except Exception:
    pd = None
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = pq = None

# Защита на случай, если EXCEL_ROW_LIMIT не определён в окружении
try:
    EXCEL_ROW_LIMIT  # type: ignore
except NameError:
    EXCEL_ROW_LIMIT = 1_000_000

# --- Вспомогательные функции конвертации ---
def _dim_factor_from_cm(target: str) -> float:
    # База: cm
    return {"мм": 10.0, "см": 1.0, "м": 1.0 / 100.0, "оригинал": 1.0}.get(target, 1.0)

def _weight_factor_from_kg(target: str) -> float:
    # База: kg
    return {"г": 1000.0, "кг": 1.0, "т": 1.0 / 1000.0, "оригинал": 1.0}.get(target, 1.0)

def _convert_numeric_column(df: "pd.DataFrame", col: str, factor: float, decimals: int = 4):
    if pd is None:
        return
    if col not in df.columns:
        return
    converted = pd.to_numeric(df[col].replace("", pd.NA), errors='coerce') * factor
    df[col] = converted.round(decimals).astype(object).where(~converted.isna(), "")

def _convert_dimensions_str_cell(s: str, dim_factor: float, leave_empty_if_nonparse: bool = True):
    if not isinstance(s, str) or not s.strip():
        return ""
    parts = re.findall(r"[\d]+(?:[.,]\d+)?", s)
    if not parts:
        return "" if leave_empty_if_nonparse else s
    out_parts = []
    for p in parts:
        p = p.replace(",", ".")
        try:
            v = float(p)
            v2 = v * dim_factor
            if abs(v2 - round(v2)) < 1e-9:
                out_parts.append(str(int(round(v2))))
            else:
                out_parts.append(str(round(v2, 4)).rstrip('0').rstrip('.'))
        except Exception:
            out_parts.append(p)
    return "x".join(out_parts)

def _export_common_postprocess_pdf(pdf: "pd.DataFrame", dimension_unit: str, weight_unit: str):
    if pd is None:
        return pdf
    dim_factor = _dim_factor_from_cm(dimension_unit)
    weight_factor = _weight_factor_from_kg(weight_unit)
    for col in ["Длинна", "Ширина", "Высота"]:
        if col in pdf.columns:
            _convert_numeric_column(pdf, col, dim_factor, decimals=4)
    compound = "Длинна/Ширина/Высота"
    if compound in pdf.columns:
        pdf[compound] = pdf[compound].fillna("").astype(str).apply(
            lambda s: _convert_dimensions_str_cell(s, dim_factor, leave_empty_if_nonparse=True)
        )
    if "Вес" in pdf.columns:
        _convert_numeric_column(pdf, "Вес", weight_factor, decimals=4)
    return pdf

# --- Новые методы (как обычные функции) ---
def _new_export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None,
                                 include_prices: bool = True, apply_markup: bool = True,
                                 dimension_unit: str = "см", weight_unit: str = "кг") -> bool:
    try:
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        logger.info(f"Executing export query (with unit conversion): {query}")
        df = self.conn.execute(query).pl()
        pdf = df.to_pandas()
        for col in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
            if col in pdf.columns:
                pdf[col] = pdf[col].astype(str).replace({'nan': ''})
        pdf = _export_common_postprocess_pdf(pdf, dimension_unit, weight_unit)
        output_dir = Path("auto_parts_data")
        output_dir.mkdir(parents=True, exist_ok=True)
        buf = io.StringIO()
        pdf.to_csv(buf, sep=';', index=False)
        with open(output_path, "wb") as f:
            f.write(b'\xef\xbb\xbf')
            f.write(buf.getvalue().encode('utf-8'))
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        try:
            import streamlit as st
            st.success(f"Данные экспортированы: {output_path} ({size_mb:.1f} МБ)")
        except Exception:
            pass
        return True
    except Exception as e:
        logger.exception("Ошибка экспорта CSV (with unit conversion)")
        try:
            import streamlit as st
            st.error(f"Ошибка при экспорте в CSV: {str(e)}")
        except Exception:
            pass
        return False

def _new_export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None,
                                   include_prices: bool = True, apply_markup: bool = True,
                                   dimension_unit: str = "см", weight_unit: str = "кг") -> bool:
    try:
        if pd is None:
            raise RuntimeError("pandas не установлен в окружении")
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = pd.read_sql(query, self.conn)
        for col in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
            if col in df.columns:
                df[col] = df[col].astype(str).replace({r'^nan$': ''}, regex=True)
        df = _export_common_postprocess_pdf(df, dimension_unit, weight_unit)
        if len(df) <= EXCEL_ROW_LIMIT:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
        else:
            sheets = (len(df) // EXCEL_ROW_LIMIT) + 1
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for i in range(sheets):
                    df.iloc[i*EXCEL_ROW_LIMIT:(i+1)*EXCEL_ROW_LIMIT].to_excel(
                        writer, index=False, sheet_name=f"Данные_{i+1}")
        return True
    except Exception as e:
        logger.exception("Ошибка экспорта Excel (with unit conversion)")
        try:
            import streamlit as st
            st.error(f"Ошибка при экспорте в Excel: {str(e)}")
        except Exception:
            pass
        return False

def _new_export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None,
                           include_prices: bool = True, apply_markup: bool = True,
                           dimension_unit: str = "см", weight_unit: str = "кг") -> bool:
    try:
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = self.conn.execute(query).pl().to_pandas()
        df = _export_common_postprocess_pdf(df, dimension_unit, weight_unit)
        if pa is None or pq is None:
            # fallback to pandas parquet if pyarrow missing
            if pd is None:
                raise RuntimeError("pyarrow и pandas отсутствуют; не могу сохранить parquet")
            df.to_parquet(output_path, index=False)
        else:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_path)
        return True
    except Exception as e:
        logger.exception("Ошибка экспорта Parquet (with unit conversion)")
        try:
            import streamlit as st
            st.error(f"Ошибка при экспорте в Parquet: {str(e)}")
        except Exception:
            pass
        return False

def _new_show_export_interface(self):
    try:
        import streamlit as st
    except Exception:
        raise RuntimeError("streamlit не доступен в окружении для интерфейса")
    st.header("📤 Экспорт данных")
    total = self.conn.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
    st.info(f"Всего: {total}")
    if total == 0:
        st.warning("Нет данных для экспорта")
        return

    format_choice = st.radio("Формат", ["CSV", "Excel", "Parquet"])
    selected_columns = st.multiselect("Колонки", [
        "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
        "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
        "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение", "Цена", "Валюта"
    ])

    include_prices = st.checkbox("Включить цены", value=True)
    apply_markup = st.checkbox("Применить наценку", value=True, disabled=not include_prices)

    st.subheader("Единицы измерения при экспорте")
    col1, col2 = st.columns(2)
    with col1:
        dimension_unit = st.selectbox("Единицы размеров:", ["мм", "см", "м", "оригинал"], index=1,
                                      help="Исходно длины хранятся в сантиметрах; выберите желаемые единицы в экспорте.")
    with col2:
        weight_unit = st.selectbox("Единицы веса:", ["г", "кг", "т", "оригинал"], index=1,
                                   help="Исходно вес хранится в килограммах; выберите желаемые единицы в экспорте.")

    if st.button("🚀 Экспортировать"):
        output_path = self.data_dir / f"export.{format_choice.lower()}"
        with st.spinner("Генерация файла..."):
            if format_choice == "CSV":
                self.export_to_csv_optimized(str(output_path), selected_columns if selected_columns else None,
                                            include_prices, apply_markup, dimension_unit, weight_unit)
            elif format_choice == "Excel":
                self.export_to_excel_optimized(str(output_path), selected_columns if selected_columns else None,
                                              include_prices, apply_markup, dimension_unit, weight_unit)
            elif format_choice == "Parquet":
                self.export_to_parquet(str(output_path), selected_columns if selected_columns else None,
                                       include_prices, apply_markup, dimension_unit, weight_unit)
            else:
                st.warning("Неподдерживаемый формат")
                return
        try:
            with open(output_path, "rb") as f:
                st.download_button("⬇️ Скачать файл", f, file_name=output_path.name)
        except Exception:
            st.info(f"Файл сохранён: {output_path}")

# --- Функция применить патч ---
def apply_patch(HV_class: Optional[type] = None) -> bool:
    """
    Применяет патч к классу HighVolumeAutoPartsCatalog.

    Если HV_class не передан, функция попробует найти класс в __main__ и globals().
    Вернёт True при успешном применении, False — если класс не найден.
    """
    # попытки найти класс автоматически
    if HV_class is None:
        # сначала в глобалах текущего модуля
        HV_class = globals().get("HighVolumeAutoPartsCatalog")
    if HV_class is None:
        # затем в __main__
        try:
            main_mod = importlib.import_module("__main__")
            HV_class = getattr(main_mod, "HighVolumeAutoPartsCatalog", None)
        except Exception:
            HV_class = None
    if HV_class is None:
        # также попробовать в загруженных модулях (по имени)
        for modname, mod in list(sys.modules.items()):
            if not mod:
                continue
            hv = getattr(mod, "HighVolumeAutoPartsCatalog", None)
            if hv is not None:
                HV_class = hv
                break

    if HV_class is None:
        print("HighVolumeAutoPartsCatalog не найден. Вызовите apply_patch(HighVolumeAutoPartsCatalog) после определения класса.")
        # всё ещё регистрируем функции в возвращаемом словаре для ручного применения
        return False

    # Применяем методы (monkeypatch)
    setattr(HV_class, "export_to_csv_optimized", _new_export_to_csv_optimized)
    setattr(HV_class, "export_to_excel_optimized", _new_export_to_excel_optimized)
    setattr(HV_class, "export_to_parquet", _new_export_to_parquet)
    setattr(HV_class, "show_export_interface", _new_show_export_interface)
    print("Патч успешно применён к HighVolumeAutoPartsCatalog: добавлена поддержка конвертации единиц при экспорте.")
    return True

# Если класс уже доступен в момент запуска — применяем автоматически
_apply_result = apply_patch()
# Если не применено, пользователь может вручную вызвать:
# from этот_модуль import apply_patch
# apply_patch(HighVolumeAutoPartsCatalog)
