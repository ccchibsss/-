#!/usr/bin/env python3
# Улучшенное, оптимизированное и более красочное приложение Streamlit для замены и предварительного просмотра брендов/моделей авто
import io
import os
import json
from functools import lru_cache
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st

# Необязательный морфологический анализатор (если есть — используется для склонения)
try:
    import pymorphy2
    morph = pymorphy2.MorphAnalyzer()
except Exception:
    morph = None

CSV_ENCODING = "utf-8-sig"
ADDITIONS_FILE = "additional_brands.json"

# Исходный словарь брендов и моделей (может быть расширен пользователем)
car_brands_models: Dict[str, str] = {
    "Acura": "Акура",
    "Integra": "Интегра",
    "MDX": "МДХ",
    "RDX": "РДХ",
    "RSX": "РСХ",
    "TLX": "ТЛКС",
    # ... (остальной словарь сокращён для краткости, добавьте весь ваш список)
}

# Загрузка сохранённых дополнений, если есть
if os.path.exists(ADDITIONS_FILE):
    try:
        with open(ADDITIONS_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, dict):
                car_brands_models.update({str(k): str(v) for k, v in loaded.items()})
    except Exception:
        pass

@lru_cache(maxsize=20000)
def decline_word_cached(word: str) -> str:
    """Возвращает склонённое слово в именительном падеже (если есть pymorphy2)."""
    if not word or morph is None:
        return word
    try:
        p = morph.parse(word)[0]
        inf = p.inflect({"nomn"})
        return inf.word if inf else p.word
    except Exception:
        return word

def build_final_struct(base_map: Dict[str, str], additions: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Строит структуру trie и метаданные для быстрого поиска подстрок (без учёта регистра).
    Возвращает словарь с trie, отображением и максимальной длиной поиска.
    """
    final = {**(base_map or {})}
    if additions:
        final.update(additions)

    final = {k: v for k, v in final.items() if isinstance(k, str) and k.strip()}

    if not final:
        return {"trie": None, "map": {}, "max_len": 0}

    mapping: Dict[str, Tuple[str, str]] = {}
    max_len = 0

    for k, v in final.items():
        lk = k.lower()
        display = v if v is not None else k
        display_decl = decline_word_cached(str(display))
        mapping[lk] = (k, display_decl)
        if len(lk) > max_len:
            max_len = len(lk)

    trie = {}
    END = "_end_"
    for lk, pair in mapping.items():
        node = trie
        for ch in lk:
            node = node.setdefault(ch, {})
        node[END] = (lk, pair)

    return {"trie": trie, "map": mapping, "max_len": max_len}

def _is_word_char(c: str) -> bool:
    return c.isalnum() or c == "_"

def get_matches(text: str, struct: Dict[str, Any]) -> List[Tuple[int, int, str, str]]:
    if not text or not struct or struct.get("trie") is None:
        return []

    trie = struct["trie"]
    text_l = text.lower()
    n = len(text_l)
    max_len = struct.get("max_len", n)
    END = "_end_"
    matches: List[Tuple[int, int, str, str]] = []

    for i in range(n):
        node = trie
        for j in range(i, min(n, i + max_len)):
            ch = text_l[j]
            if ch not in node:
                break
            node = node[ch]
            if END in node:
                lk, (orig_key, disp) = node[END]
                start = i
                end = j + 1
                if start > 0 and _is_word_char(text[start - 1]):
                    continue
                if end < n and _is_word_char(text[end]):
                    continue
                matches.append((start, end, orig_key, disp))
    matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    filtered = []
    last_end = -1
    for s, e, ok, du in matches:
        if s >= last_end:
            filtered.append((s, e, ok, du))
            last_end = e
    return filtered

def highlight_html(text: str, matches: List[Tuple[int, int, str, str]]) -> str:
    if not matches:
        return escape_html(text)
    out = []
    last = 0
    for s, e, orig, disp in matches:
        out.append(escape_html(text[last:s]))
        snippet = escape_html(text[s:e])
        out.append(f'<mark style="{HIGHLIGHT_STYLE}">{snippet}</mark>')
        last = e
    out.append(escape_html(text[last:]))
    return "".join(out)

def escape_html(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))

def process_text(text: str, struct: Dict[str, Any]) -> Tuple[str, str]:
    if not text:
        return text, ""
    matches = get_matches(text, struct)
    html = highlight_html(text, matches)
    return text, html

def load_dictionary(source: Optional[str] = None, fileobj: Optional[io.BytesIO] = None) -> Dict[str, str]:
    try:
        if fileobj is not None:
            if hasattr(fileobj, "read"):
                data = fileobj.read()
                b = io.BytesIO(data)
                name = getattr(fileobj, "name", "") or source or ""
            else:
                b = fileobj
                name = source or ""
            if name.lower().endswith(".json"):
                text = b.getvalue().decode("utf-8")
                obj = json.loads(text)
                return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
            if name.lower().endswith(".csv"):
                df = pd.read_csv(b)
            else:
                df = pd.read_excel(b, engine='openpyxl')
            if len(df.columns) >= 2:
                return {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
        if source:
            if source.startswith("http"):
                r = requests.get(source, timeout=10)
                r.raise_for_status()
                if source.lower().endswith(".json"):
                    obj = r.json()
                    return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
                if source.lower().endswith(".csv"):
                    df = pd.read_csv(io.StringIO(r.text))
                else:
                    ext = os.path.splitext(source)[1].lower()
                    if ext in ['.xlsx', '.xlsm', '.xltx', '.xltm']:
                        df = pd.read_excel(io.BytesIO(r.content), engine='openpyxl')
                    elif ext == '.xls':
                        df = pd.read_excel(io.BytesIO(r.content), engine='xlrd')
                    else:
                        df = pd.read_excel(io.BytesIO(r.content), engine='openpyxl')
                if len(df.columns) >= 2:
                    return {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
            else:
                ext = os.path.splitext(source)[1].lower()
                if ext == '.json':
                    with open(source, "r", encoding="utf-8") as f:
                        obj = json.load(f)
                        return {str(k): str(v) for k, v in (obj.items() if isinstance(obj, dict) else [])}
                elif ext == '.csv':
                    df = pd.read_csv(source)
                elif ext in ['.xlsx', '.xlsm', '.xltx', '.xltm']:
                    df = pd.read_excel(source, engine='openpyxl')
                elif ext == '.xls':
                    df = pd.read_excel(source, engine='xlrd')
                if len(df.columns) >= 2:
                    return {str(k): str(v) for k, v in zip(df.iloc[:,0], df.iloc[:,1])}
    except Exception:
        return {}
    return {}

def process_file_for_processing(file_bytes: bytes, filename: str, col_name: str, struct: Dict[str, Any]) -> pd.DataFrame:
    stream = io.BytesIO(file_bytes)
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(stream)
    else:
        df = pd.read_excel(stream, engine='openpyxl')

    if col_name not in df.columns:
        raise ValueError(f"Столбец '{col_name}' не найден в файле.")

    series = df[col_name].fillna("").astype(str)
    plain_list = []
    html_list = []

    for txt in series:
        plain, html = process_text(txt, struct)
        plain_list.append(plain)
        html_list.append(html)

    df[col_name] = plain_list
    df[f"{col_name}_preview_html"] = html_list

    return df

def run():
    st.set_page_config(page_title="🚗 Обработка брендов/моделей", layout="wide")
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2196F3,#21CBF3);padding:16px;border-radius:8px">
        <h2 style="color:white;margin:0">🚗 Обработка данных и расширение словаря</h2>
        <p style="color:rgba(255,255,255,0.9);margin:4px 0 0">Быстрая подстановка и визуальный просмотр совпадений</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.write("")

    base_dict = car_brands_models.copy()
    struct = build_final_struct(base_dict)

    with st.expander("🛠️ Настройки словаря", expanded=True):
        left, right = st.columns([2, 1])
        with left:
            uploaded_dict = st.file_uploader("📁 Загрузить файл словаря (json/csv/xlsx)", type=["json", "csv", "xls", "xlsx"])
            dict_url = st.text_input("🌐 Или указать URL (json/csv/xlsx)")
            if st.button("🔄 Загрузить/Обновить словарь"):
                with st.spinner("Загрузка..."):
                    loaded = {}
                    try:
                        if uploaded_dict:
                            loaded = load_dictionary(source=uploaded_dict.name, fileobj=uploaded_dict)
                        elif dict_url:
                            loaded = load_dictionary(source=dict_url)
                        if loaded:
                            base_dict.update(loaded)
                            with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
                                json.dump({str(k): str(v) for k, v in base_dict.items()}, f, ensure_ascii=False, indent=2)
                            struct = build_final_struct(base_dict)
                            st.success(f"Словарь обновлён: +{len(loaded)} пар")
                        else:
                            st.info("Ничего не загружено (проверьте формат)")
                    except Exception as e:
                        st.error(f"Ошибка: {e}")

        with right:
            st.markdown("#### Текущий словарь (часть)")
            if base_dict:
                df_dict = pd.DataFrame(list(base_dict.items()), columns=["Ключ", "Значение"]).head(200)
                st.dataframe(df_dict)

            st.markdown("#### Добавить вручную")
            new_k = st.text_input("🔑 Новый ключ", key="nk")
            new_v = st.text_input("📝 Новое значение", key="nv")
            if st.button("➕ Добавить пару вручную"):
                if new_k and new_v:
                    base_dict[new_k] = new_v
                    with open(ADDITIONS_FILE, "w", encoding="utf-8") as f:
                        json.dump({str(k): str(v) for k, v in base_dict.items()}, f, ensure_ascii=False, indent=2)
                    struct = build_final_struct(base_dict)
                    st.success(f"Добавлено: '{new_k}':'{new_v}'")
                else:
                    st.warning("Заполните оба поля")

    st.markdown("---")
    with st.expander("📝 Обработка файла", expanded=True):
        uploaded = st.file_uploader("📂 Выберите CSV/XLSX файл для обработки", type=["csv", "xls", "xlsx"])
        col_name = st.text_input("🔤 Имя столбца для обработки (или оставьте пустым для выбора)")

        if uploaded and not col_name:
            try:
                if uploaded.name.lower().endswith(".csv"):
                    df0 = pd.read_csv(uploaded, nrows=0)
                else:
                    df0 = pd.read_excel(uploaded, nrows=0, engine='openpyxl')
                cols = df0.columns.tolist()
                if cols:
                    col_name = st.selectbox("📑 Выберите столбец", cols)
            except Exception:
                pass

        if uploaded and col_name:
            try:
                with st.spinner("Обрабатываем..."):
                    bytes_data = uploaded.read()
                    struct = build_final_struct(base_dict)
                    df_res = process_file_for_processing(bytes_data, uploaded.name, col_name, struct)
                st.success("✅ Обработка завершена")
                st.markdown("### Превью (с подсветкой совпадений)")
                show_df = df_res.head(50).copy()
                html_table = show_df.to_html(escape=False, index=False)
                st.markdown(html_table, unsafe_allow_html=True)
                st.markdown("### Скачать результат")
                buf = io.BytesIO()
                if uploaded.name.lower().endswith(".csv"):
                    df_res.to_csv(buf, index=False, encoding=CSV_ENCODING)
                    buf.seek(0)
                    st.download_button("⬇️ Скачать CSV", buf, file_name="result.csv", mime="text/csv")
                else:
                    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                        df_res.to_excel(writer, index=False)
                    buf.seek(0)
                    st.download_button("⬇️ Скачать XLSX", buf, file_name="result.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                st.error(f"Ошибка обработки: {e}")

    st.markdown("---")
    st.caption("Подсветка не влияет на экспортируемые данные — она предназначена только для визуальной проверки.")

if __name__ == "__main__":
    run()
