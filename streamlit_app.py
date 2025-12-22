# integrated_autoparts_app.py
# Интеграция представленных фрагментов в один рабочий модуль.
# Содержит класс HighVolumeAutoPartsCatalog и Streamlit + CLI интерфейсы.
# Требует: polars, duckdb, pandas, streamlit (опционально для UI), openpyxl (для
# Excel экспортов).
# Запуск: streamlit run integrated_autoparts_app.py  или python
# integrated_autoparts_app.py

import platform
import sys
import os
import time
import logging
import io
import zipfile
import warnings
import json
import re
import argparse
import inspect
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_ROW_LIMIT = 1_000_000

# Внешние зависимости (pinned optional)
try:
    import polars as pl
except Exception:
    pl = None
try:
    import duckdb
except Exception:
    duckdb = None
try:
    import pandas as pd
except Exception:
    pd = None
try:
    import streamlit as st
except Exception:
    st = None

# -------------------------
# Класс приложения
# -------------------------
class HighVolumeAutoPartsCatalog:
    """
    Интегрированный класс из предоставленных фрагментов.
    Содержит: загрузку конфигов, чтение/нормализацию Excel, запись в DuckDB,
    экспорт (CSV/Excel/Parquet), управляющие операции и Streamlit интерфейсные части.
    """
    def __init__(self, data_dir: str = "./auto_parts_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        self.db_path = self.data_dir / "catalog.duckdb"
        if duckdb is None:
            logger.warning("duckdb не установлен: БД недоступно.")
            self.conn = None
        else:
            self.conn = duckdb.connect(database=str(self.db_path))
            self.setup_database()

        # Если Streamlit есть — настроим страницу (незначительно)
        if st is not None:
            try:
                st.set_page_config(page_title="AutoParts Catalog 10M+", layout="wide", page_icon="🚗")
            except Exception:
                pass

    # ---------- Конфиги ----------
    def load_cloud_config(self) -> Dict[str, Any]:
        cfg = self.data_dir / "cloud_config.json"
        default = {"enabled": False, "provider": "s3", "bucket": "", "region": "", "sync_interval": 3600, "last_sync": 0}
        if cfg.exists():
            try:
                return json.loads(cfg.read_text(encoding='utf-8'))
            except Exception:
                return default
        else:
            cfg.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding='utf-8')
            return default

    def save_cloud_config(self) -> None:
        cfg = self.data_dir / "cloud_config.json"
        self.cloud_config['last_sync'] = int(time.time())
        cfg.write_text(json.dumps(self.cloud_config, ensure_ascii=False, indent=2), encoding='utf-8')

    def load_price_rules(self) -> Dict[str, Any]:
        p = self.data_dir / "price_rules.json"
        default = {"global_markup": 0.2, "brand_markups": {}, "min_price": 0.0, "max_price": 99999.0}
        if p.exists():
            try:
                return json.loads(p.read_text(encoding='utf-8'))
            except Exception:
                return default
        else:
            p.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding='utf-8')
            return default

    def save_price_rules(self) -> None:
        p = self.data_dir / "price_rules.json"
        p.write_text(json.dumps(self.price_rules, ensure_ascii=False, indent=2), encoding='utf-8')

    def load_exclusion_rules(self) -> List[str]:
        f = self.data_dir / "exclusion_rules.txt"
        if f.exists():
            try:
                return [line.strip() for line in f.read_text(encoding='utf-8').splitlines() if line.strip()]
            except Exception:
                return []
        else:
            defaults = ["Кузов", "Стекла", "Масла"]
            f.write_text("\n".join(defaults), encoding='utf-8')
            return defaults

    def save_exclusion_rules(self) -> None:
        f = self.data_dir / "exclusion_rules.txt"
        f.write_text("\n".join(self.exclusion_rules), encoding='utf-8')

    def load_category_mapping(self) -> Dict[str, str]:
        f = self.data_dir / "category_mapping.txt"
        default = {"Радиатор": "Охлаждение", "Шаровая опора": "Подвеска", "Фильтр масляный": "Фильтры", "Тормозные колодки": "Тормоза"}
        if f.exists():
            try:
                mapping = {}
                for line in f.read_text(encoding='utf-8').splitlines():
                    if "|" in line:
                        k, v = line.split("|", 1)
                        mapping[k.strip()] = v.strip()
                return mapping
            except Exception:
                return default
        else:
            f.write_text("\n".join([f"{k}|{v}" for k, v in default.items()]), encoding='utf-8')
            return default

    def save_category_mapping(self) -> None:
        f = self.data_dir / "category_mapping.txt"
        f.write_text("\n".join([f"{k}|{v}" for k, v in self.category_mapping.items()]), encoding='utf-8')

    # ---------- DuckDB setup ----------
    def setup_database(self) -> None:
        if self.conn is None:
            return
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS oe (
                    oe_number_norm VARCHAR PRIMARY KEY,
                    oe_number VARCHAR,
                    name VARCHAR,
                    applicability VARCHAR,
                    category VARCHAR
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS parts (
                    artikul_norm VARCHAR,
                    brand_norm VARCHAR,
                    artikul VARCHAR,
                    brand VARCHAR,
                    multiplicity INTEGER,
                    barcode VARCHAR,
                    length DOUBLE,
                    width DOUBLE,
                    height DOUBLE,
                    weight DOUBLE,
                    image_url VARCHAR,
                    dimensions_str VARCHAR,
                    description VARCHAR,
                    PRIMARY KEY (artikul_norm, brand_norm)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS cross_references (
                    oe_number_norm VARCHAR,
                    artikul_norm VARCHAR,
                    brand_norm VARCHAR,
                    PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS prices (
                    artikul_norm VARCHAR,
                    brand_norm VARCHAR,
                    price DOUBLE,
                    currency VARCHAR DEFAULT 'RUB',
                    PRIMARY KEY (artikul_norm, brand_norm)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR
                );
            """)
            # try indexes (duckdb supports CREATE INDEX)
            for sql in [
                "CREATE INDEX IF NOT EXISTS idx_oe_number_norm ON oe(oe_number_norm);",
                "CREATE INDEX IF NOT EXISTS idx_parts_keys ON parts(artikul_norm, brand_norm);",
                "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm);",
                "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm);",
                "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm);"
            ]:
                try:
                    self.conn.execute(sql)
                except Exception:
                    pass
        except Exception as e:
            logger.exception("setup_database failed: %s", e)

    # ---------- Normalization / parsing helpers ----------
    @staticmethod
    def normalize_key(series: "pl.Series") -> "pl.Series":
        if pl is None:
            raise RuntimeError("polars required for normalize_key")
        return (series.fill_null("").cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())

    @staticmethod
    def clean_values(series: "pl.Series") -> "pl.Series":
        if pl is None:
            raise RuntimeError("polars required for clean_values")
        return (series.fill_null("").cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())

    def determine_category_vectorized(self, name_series: "pl.Series") -> "pl.Series":
        if pl is None:
            raise RuntimeError("polars required for determine_category_vectorized")
        name_lower = name_series.str.to_lowercase()
        expr = pl.when(pl.lit(False)).then(pl.lit(None))
        for k, v in self.category_mapping.items():
            expr = expr.when(name_lower.str.contains(k.lower())).then(pl.lit(v))
        categories_map = {
            'Фильтр': 'фильтр|filter',
            'Тормоза': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission',
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering',
            'Выпуск': 'глушитель|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling',
            'Топливо': 'топливный|бензонасос|форсунк|fuel'
        }
        for cat, pattern in categories_map.items():
            expr = expr.when(name_lower.str.contains(pattern, literal=False)).then(pl.lit(cat))
        return expr.otherwise(pl.lit('Разное')).alias('category')

    # Dimension parsing (lightweight)
    DIM_SEP_REGEX = re.compile(r'[x×*/\s,;]+', flags=re.IGNORECASE)
    WEIGHT_REGEX = re.compile(r'(\d+[.,]?\d*)\s*(kg|кг|g|гр|гр\.|g\.|grams|lb|lbs|фунт|oz|унц)', flags=re.IGNORECASE)
    UNIT_TRAILING_REGEX = re.compile(r'(mm|мм|cm|см|m|м|in|inch|дюйм|дюйма)\b', flags=re.IGNORECASE)

    def _to_float(self, s: Any) -> Optional[float]:
        if s is None:
            return None
        try:
            return float(str(s).strip().replace(',', '.'))
        except Exception:
            return None

    def parse_dimension_string(self, s: Any):
        if s is None:
            return (None, None, None, None)
        text = str(s).lower()
        weight_kg = None
        w = self.WEIGHT_REGEX.search(text)
        if w:
            val = self._to_float(w.group(1))
            unit = w.group(2).lower()
            if val is not None:
                if unit in ('kg', 'кг'):
                    weight_kg = val
                elif unit in ('g', 'гр', 'гр.', 'g.'):
                    weight_kg = val / 1000.0
                elif unit in ('lb', 'lbs', 'фунт'):
                    weight_kg = val * 0.45359237
                elif unit in ('oz', 'унц'):
                    weight_kg = val * 0.0283495231
        if w:
            text_dims = (text[:w.start()] + text[w.end():]).strip()
        else:
            text_dims = text
        if not text_dims:
            return (None, None, None, weight_kg)
        tu = self.UNIT_TRAILING_REGEX.search(text_dims)
        unit = tu.group(1).lower() if tu else None
        parts = re.split(r'[x×*/,;]+', text_dims)
        nums = []
        for p in parts:
            p2 = re.sub(r'(mm|мм|cm|см|m|м|in|inch|дюйм|дюйма)', '', p, flags=re.IGNORECASE)
            found = re.findall(r'\d+[.,]?\d*', p2)
            for f in found:
                v = self._to_float(f)
                if v is not None:
                    nums.append(v)
        if not nums:
            alln = re.findall(r'\d+[.,]?\d*', text_dims)
            for f in alln:
                v = self._to_float(f)
                if v is not None:
                    nums.append(v)
        mul = 1.0
        if unit:
            if unit in ('mm', 'мм'):
                mul = 0.1
            elif unit in ('cm', 'см'):
                mul = 1.0
            elif unit in ('m', 'м'):
                mul = 100.0
            elif unit in ('in', 'inch', 'дюйм', 'дюйма'):
                mul = 2.54
        else:
            if nums and max(nums) > 300:
                mul = 0.1
        length = width = height = None
        if len(nums) >= 3:
            length, width, height = nums[0] * mul, nums[1] * mul, nums[2] * mul
        elif len(nums) == 2:
            length, width = nums[0] * mul, nums[1] * mul
        elif len(nums) == 1:
            length = nums[0] * mul
        return (float(length) if length is not None else None,
                float(width) if width is not None else None,
                float(height) if height is not None else None,
                float(weight_kg) if weight_kg is not None else None)

    def parse_dimensions_series(self, series: Any):
        if pl is None:
            raise RuntimeError("polars required for parse_dimensions_series")
        if hasattr(series, "to_list"):
            items = series.to_list()
        elif isinstance(series, (list, tuple)):
            items = list(series)
        else:
            items = [series]
        parsed = [self.parse_dimension_string(x) for x in items]
        return pl.DataFrame({
            "parsed_length": [r[0] for r in parsed],
            "parsed_width": [r[1] for r in parsed],
            "parsed_height": [r[2] for r in parsed],
            "parsed_weight": [r[3] for r in parsed]
        })

    # ---------- File detection / read / prepare ----------
    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        variants = {
            'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'OE'],
            'artikul': ['артикул', 'article', 'sku'],
            'brand': ['бренд', 'brand', 'производитель', 'manufacturer'],
            'name': ['наименование', 'название', 'name', 'описание', 'description'],
            'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability'],
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13'],
            'multiplicity': ['кратность шт', 'кратность', 'multiplicity'],
            'length': ['длина (см)', 'длина', 'length', 'длинна'],
            'width': ['ширина (см)', 'ширина', 'width'],
            'height': ['высота (см)', 'высота', 'height'],
            'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight'],
            'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка'],
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size'],
            'price': ['цена', 'price', 'рекомендованная цена', 'retail price'],
            'currency': ['валюта', 'currency']
        }
        actual_lower = {c.lower(): c for c in actual_columns}
        mapping = {}
        for key in expected_columns:
            for variant in variants.get(key, [key]):
                vlower = variant.lower()
                for act_l, act_orig in actual_lower.items():
                    if vlower in act_l and act_orig not in mapping:
                        mapping[act_orig] = key
        return mapping

    def read_and_prepare_file(self, path: str, ftype: str) -> "pl.DataFrame":
        logger.info("Reading file %s as %s", path, ftype)
        try:
            if not os.path.exists(path):
                logger.error("File not found: %s", path)
                return pl.DataFrame() if pl else None
            # use polars Excel reader if available; fall back to pandas conversion
            if pl:
                try:
                    df = pl.read_excel(path)
                except Exception:
                    # fallback: pandas -> polars
                    if pd:
                        pdf = pd.read_excel(path)
                        df = pl.from_pandas(pdf)
                    else:
                        logger.error("No available excel reader")
                        return pl.DataFrame()
            else:
                raise RuntimeError("polars required to process files")
        except Exception as e:
            logger.exception("read_and_prepare_file failed: %s", e)
            return pl.DataFrame()

        if df.is_empty():
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['artikul', 'brand', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        expected = schemas.get(ftype, [])
        colmap = self.detect_columns(df.columns, expected)
        if not colmap:
            logger.info("No recognizable columns for %s in %s", ftype, path)
            return pl.DataFrame()
        df = df.rename(colmap)

        for c in ['artikul', 'brand', 'oe_number']:
            if c in df.columns:
                df = df.with_columns(self.clean_values(pl.col(c)).alias(c))
        for c in ['oe_number', 'artikul', 'brand']:
            if c in df.columns:
                df = df.with_columns(self.normalize_key(pl.col(c)).alias(f"{c}_norm"))

        # try parse dimensions_str if present
        if 'dimensions_str' in df.columns:
            try:
                parsed = self.parse_dimensions_series(df['dimensions_str'])
                if 'length' not in df.columns:
                    df = df.with_columns(parsed['parsed_length'].alias('length'))
                else:
                    df = df.with_columns(pl.when(pl.col('length').is_null()).then(parsed['parsed_length']).otherwise(pl.col('length')).alias('length'))
                if 'width' not in df.columns:
                    df = df.with_columns(parsed['parsed_width'].alias('width'))
                else:
                    df = df.with_columns(pl.when(pl.col('width').is_null()).then(parsed['parsed_width']).otherwise(pl.col('width')).alias('width'))
                if 'height' not in df.columns:
                    df = df.with_columns(parsed['parsed_height'].alias('height'))
                else:
                    df = df.with_columns(pl.when(pl.col('height').is_null()).then(parsed['parsed_height']).otherwise(pl.col('height')).alias('height'))
                if 'weight' not in df.columns:
                    df = df.with_columns(parsed['parsed_weight'].alias('weight'))
                else:
                    df = df.with_columns(pl.when(pl.col('weight').is_null()).then(parsed['parsed_weight']).otherwise(pl.col('weight')).alias('weight'))
            except Exception:
                logger.debug("parse_dimensions_series failed for %s", path)

        return df

    # ---------- Upsert ----------
    def upsert_data(self, table: str, df: "pl.DataFrame", pk: List[str]) -> None:
        if df is None or df.is_empty():
            return
        if self.conn is None:
            logger.info("DuckDB not available; skipping upsert into %s", table)
            return
        temp = f"temp_{table}_{int(time.time())}"
        try:
            self.conn.register(temp, df.to_arrow())
            pk_csv = ", ".join(f'"{c}"' for c in pk)
            try:
                self.conn.execute(f"DELETE FROM {table} WHERE ({pk_csv}) IN (SELECT {pk_csv} FROM {temp});")
            except Exception:
                conds = " OR ".join([f"{table}.{c} = t.{c}" for c in pk])
                self.conn.execute(f"DELETE FROM {table} WHERE EXISTS (SELECT 1 FROM {temp} t WHERE {conds});")
            self.conn.execute(f"INSERT INTO {table} SELECT * FROM {temp};")
            logger.info("Upserted %d rows into %s", len(df), table)
        except Exception as e:
            logger.exception("upsert_data failed for %s: %s", table, e)
        finally:
            try:
                self.conn.unregister(temp)
            except Exception:
                pass

    def upsert_prices(self, df: "pl.DataFrame") -> None:
        if df is None or df.is_empty():
            return
        if 'artikul' in df.columns and 'brand' in df.columns:
            df = df.with_columns([self.normalize_key(pl.col('artikul')).alias('artikul_norm'), self.normalize_key(pl.col('brand')).alias('brand_norm')])
        if 'currency' not in df.columns:
            df = df.with_columns(pl.lit('RUB').alias('currency'))
        rules = self.load_price_rules()
        df = df.filter((pl.col('price') >= rules['min_price']) & (pl.col('price') <= rules['max_price']))
        self.upsert_data('prices', df, ['artikul_norm', 'brand_norm'])

    # ---------- Processing pipeline ----------
    def process_and_load_data(self, dataframes: Dict[str, "pl.DataFrame"]) -> None:
        if pl is None:
            raise RuntimeError("polars required for processing")
        logger.info("Starting process_and_load_data")
        # OE
        if 'oe' in dataframes:
            df_oe = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df_oe.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'])
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))
            self.upsert_data('oe', oe_df, ['oe_number_norm'])
            cross_df = df_oe.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df, ['oe_number_norm', 'artikul_norm', 'brand_norm'])
        # cross
        if 'cross' in dataframes:
            df_cross = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            self.upsert_data('cross_references', df_cross, ['oe_number_norm', 'artikul_norm', 'brand_norm'])
        # prices
        if 'prices' in dataframes:
            self.upsert_prices(dataframes['prices'])
        # assemble parts
        key_files = {k: v for k, v in dataframes.items() if k in ['oe', 'barcode', 'images', 'dimensions']}
        if key_files:
            all_parts = pl.concat([v.select(['artikul', 'artikul_norm', 'brand', 'brand_norm']) for v in key_files.values() if 'artikul_norm' in v.columns]).filter(pl.col('artikul_norm') != "").unique(subset=['artikul_norm', 'brand_norm'])
            for f in ['oe', 'barcode', 'images', 'dimensions']:
                if f not in key_files:
                    continue
                df_part = key_files[f]
                if df_part.is_empty() or 'artikul_norm' not in df_part.columns:
                    continue
                join_cols = [c for c in df_part.columns if c not in ['artikul', 'artikul_norm', 'brand', 'brand_norm']]
                if not join_cols:
                    continue
                existing_cols = set(all_parts.columns)
                join_cols = [c for c in join_cols if c not in existing_cols]
                if not join_cols:
                    continue
                df_sub = df_part.select(['artikul_norm', 'brand_norm'] + join_cols).unique(subset=['artikul_norm', 'brand_norm'])
                all_parts = all_parts.join(df_sub, on=['artikul_norm', 'brand_norm'], how='left')
        else:
            all_parts = pl.DataFrame()

        if not all_parts.is_empty():
            if 'multiplicity' not in all_parts.columns:
                all_parts = all_parts.with_columns(pl.lit(1).cast(pl.Int32).alias('multiplicity'))
            else:
                all_parts = all_parts.with_columns(pl.col('multiplicity').fill_null(1).cast(pl.Int32))
            for c in ['length', 'width', 'height']:
                if c not in all_parts.columns:
                    all_parts = all_parts.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
                else:
                    all_parts = all_parts.with_columns(pl.col(c).cast(pl.Float64))
            if 'weight' not in all_parts.columns:
                all_parts = all_parts.with_columns(pl.lit(None).cast(pl.Float64).alias('weight'))
            else:
                all_parts = all_parts.with_columns(pl.col('weight').cast(pl.Float64))
            if 'dimensions_str' not in all_parts.columns:
                all_parts = all_parts.with_columns(pl.lit(None).cast(pl.Utf8).alias('dimensions_str'))
            # build dimensions_str if missing
            all_parts = all_parts.with_columns([
                pl.col('length').cast(pl.Utf8).fill_null('').alias('_length'),
                pl.col('width').cast(pl.Utf8).fill_null('').alias('_width'),
                pl.col('height').cast(pl.Utf8).fill_null('').alias('_height')
            ])
            all_parts = all_parts.with_columns(
                pl.when((pl.col('dimensions_str').is_not_null()) & (pl.col('dimensions_str') != '')).then(pl.col('dimensions_str')).otherwise(
                    pl.concat_str([pl.col('_length'), pl.lit('x'), pl.col('_width'), pl.lit('x'), pl.col('_height')], separator='')
                ).alias('dimensions_str')
            ).drop(['_length', '_width', '_height'])
            if 'artikul' not in all_parts.columns:
                all_parts = all_parts.with_columns(pl.lit('').alias('artikul'))
            if 'brand' not in all_parts.columns:
                all_parts = all_parts.with_columns(pl.lit('').alias('brand'))
            all_parts = all_parts.with_columns(
                description=pl.concat_str(['Артикул: ', pl.col('artikul'), ', Бренд: ', pl.col('brand'), ', Кратность: ', pl.col('multiplicity').cast(pl.Utf8), ' шт.'])
            )
            final_cols = ['artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode', 'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description']
            df_final = all_parts.select([pl.col(c) if c in all_parts.columns else pl.lit('').alias(c) for c in final_cols])
            self.upsert_data('parts', df_final, ['artikul_norm', 'brand_norm'])
            logger.info("Parts upserted: %d", len(df_final))
        else:
            logger.info("No parts assembled from provided files.")

    # ---------- Export SQL builder ----------
    def _get_brand_markups_sql(self) -> str:
        rows = []
        for b, m in self.load_price_rules().get('brand_markups', {}).items():
            safe_b = b.replace("'", "''")
            rows.append(f"SELECT '{safe_b}' AS brand, {m} AS markup")
        return " UNION ALL ".join(rows) if rows else "SELECT NULL AS brand, 0 AS markup LIMIT 0"

    def build_export_query(self, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> str:
        desc_text = ("Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. "
                     "Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей. "
                     "В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электроматериалы, автомасла, инструмент, "
                     "а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. "
                     "Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. "
                     "Выбирайте только лучшее — надежность и качество от ведущих производителей.")
        brand_markups_sql = self._get_brand_markups_sql()

        select_parts = []
        if include_prices:
            if apply_markup:
                global_markup = self.load_price_rules().get('global_markup', 0)
                select_parts.append(f"CASE WHEN pr.price IS NOT NULL THEN pr.price * (1 + COALESCE(brm.markup, {global_markup})) ELSE pr.price END AS \"Цена\"")
            else:
                select_parts.append('pr.price AS "Цена"')
            select_parts.append("COALESCE(pr.currency, 'RUB') AS \"Валюта\"")

        columns_map = [
            ("Артикул бренда", 'r.artikul AS "Артикул бренда"'),
            ("Бренд", 'r.brand AS "Бренд"'),
            ("Наименование", 'COALESCE(r.representative_name, r.analog_representative_name) AS "Наименование"'),
            ("Применимость", 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"'),
            ("Описание", 'CONCAT(COALESCE(r.description, \'\'), dt.text) AS "Описание"'),
            ("Категория товара", 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория товара"'),
            ("Кратность", 'r.multiplicity AS "Кратность"'),
            ("Длинна", 'COALESCE(r.length, r.analog_length) AS "Длинна"'),
            ("Ширина", 'COALESCE(r.width, r.analog_width) AS "Ширина"'),
            ("Высота", 'COALESCE(r.height, r.analog_height) AS "Высота"'),
            ("Вес", 'COALESCE(r.weight, r.analog_weight) AS "Вес"'),
            ("Длинна/Ширина/Высота", """
                COALESCE(
                    CASE
                        WHEN r.dimensions_str IS NULL OR r.dimensions_str = '' OR UPPER(TRIM(r.dimensions_str)) = 'XX'
                        THEN NULL
                        ELSE r.dimensions_str
                    END,
                    r.analog_dimensions_str
                ) AS "Длинна/Ширина/Высота"
            """),
            ("OE номер", 'r.oe_list AS "OE номер"'),
            ("аналоги", 'r.analog_list AS "аналоги"'),
            ("Ссылка на изображение", 'r.image_url AS "Ссылка на изображение"')
        ]
        for name, expr in columns_map:
            if not selected_columns or name in selected_columns:
                select_parts.append(expr.strip())

        select_clause = ",\n        ".join(select_parts)
        ctes = f"""
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${desc_text}$$ AS text
        ),
        BrandMarkups AS (
            {brand_markups_sql}
        ),
        PartDetails AS (
            SELECT 
                cr.artikul_norm, cr.brand_norm,
                STRING_AGG(DISTINCT regexp_replace(regexp_replace(o.oe_number, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', ''), ', ') AS oe_list,
                ANY_VALUE(o.name) AS representative_name,
                ANY_VALUE(o.applicability) AS representative_applicability,
                ANY_VALUE(o.category) AS representative_category
            FROM cross_references cr
            LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY cr.artikul_norm, cr.brand_norm
        ),
        AllAnalogs AS (
            SELECT cr1.artikul_norm, cr1.brand_norm,
                STRING_AGG(DISTINCT regexp_replace(regexp_replace(p2.artikul, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', ''), ', ') AS analog_list
            FROM cross_references cr1
            JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
            JOIN parts p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
            WHERE (cr1.artikul_norm != p2.artikul_norm OR cr1.brand_norm != p2.brand_norm)
            GROUP BY cr1.artikul_norm, cr1.brand_norm
        ),
        InitialOENumbers AS (
            SELECT DISTINCT p.artikul_norm, p.brand_norm, cr.oe_number_norm
            FROM parts p
            LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
            WHERE cr.oe_number_norm IS NOT NULL
        ),
        Level1Analogs AS (
            SELECT DISTINCT i.artikul_norm AS source_artikul_norm, i.brand_norm AS source_brand_norm,
                cr2.artikul_norm AS related_artikul_norm, cr2.brand_norm AS related_brand_norm
            FROM InitialOENumbers i
            JOIN cross_references cr2 ON i.oe_number_norm = cr2.oe_number_norm
            WHERE NOT (i.artikul_norm = cr2.artikul_norm AND i.brand_norm = cr2.brand_norm)
        ),
        Level1OENumbers AS (
            SELECT DISTINCT l1.source_artikul_norm, l1.source_brand_norm, cr3.oe_number_norm
            FROM Level1Analogs l1
            JOIN cross_references cr3 ON l1.related_artikul_norm = cr3.artikul_norm AND l1.related_brand_norm = cr3.brand_norm
            WHERE NOT EXISTS (
                SELECT 1 FROM InitialOENumbers i WHERE i.artikul_norm = l1.source_artikul_norm AND i.brand_norm = l1.source_brand_norm AND i.oe_number_norm = cr3.oe_number_norm
            )
        ),
        Level2Analogs AS (
            SELECT DISTINCT loe.source_artikul_norm, loe.source_brand_norm, cr4.artikul_norm AS related_artikul_norm, cr4.brand_norm AS related_brand_norm
            FROM Level1OENumbers loe
            JOIN cross_references cr4 ON loe.oe_number_norm = cr4.oe_number_norm
            WHERE NOT (loe.source_artikul_norm = cr4.artikul_norm AND loe.source_brand_norm = cr4.brand_norm)
        ),
        AllRelatedParts AS (
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm FROM Level1Analogs
            UNION
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm FROM Level2Analogs
        ),
        AggregatedAnalogData AS (
            SELECT arp.source_artikul_norm, arp.source_brand_norm,
                MAX(CASE WHEN p2.length IS NOT NULL THEN p2.length ELSE NULL END) AS length,
                MAX(CASE WHEN p2.width IS NOT NULL THEN p2.width ELSE NULL END) AS width,
                MAX(CASE WHEN p2.height IS NOT NULL THEN p2.height ELSE NULL END) AS height,
                MAX(CASE WHEN p2.weight IS NOT NULL THEN p2.weight ELSE NULL END) AS weight,
                ANY_VALUE(CASE WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX' THEN p2.dimensions_str ELSE NULL END) AS dimensions_str,
                ANY_VALUE(CASE WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != '' THEN pd2.representative_name ELSE NULL END) AS representative_name,
                ANY_VALUE(CASE WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != '' THEN pd2.representative_applicability ELSE NULL END) AS representative_applicability,
                ANY_VALUE(CASE WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != '' THEN pd2.representative_category ELSE NULL END) AS representative_category
            FROM AllRelatedParts arp
            JOIN parts p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),
        RankedData AS (
            SELECT p.*, ROW_NUMBER() OVER (PARTITION BY p.artikul_norm, p.brand_norm ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST) AS rn
            FROM parts p
            LEFT JOIN PartDetails pd ON p.artikul_norm= pd.artikul_norm AND p.brand_norm= pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm= aa.artikul_norm AND p.brand_norm= aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm= p_analog.artikul_norm AND p.brand_norm= p_analog.brand_norm
        )
        """
        join_price = "LEFT JOIN prices pr ON r.artikul_norm= pr.artikul_norm AND r.brand_norm= pr.brand_norm LEFT JOIN BrandMarkups brm ON r.brand= brm.brand" if include_prices else ""
        query = f"{ctes} SELECT {select_clause} FROM RankedData r CROSS JOIN DescriptionTemplate dt {join_price} WHERE r.rn=1 ORDER BY r.brand, r.artikul"
        return query

    # ---------- Exports ----------
    def export_to_csv(self, path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        if self.conn is None:
            logger.error("duckdb not available: cannot export.")
            return False
        try:
            total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
            if total == 0:
                logger.info("No data for export.")
                return False
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            if pd:
                pdf = df.to_pandas()
            else:
                raise RuntimeError("pandas required to write CSV")
            for c in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
                if c in pdf.columns:
                    pdf[c] = pdf[c].astype(str).replace({'nan': ''})
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            buf = io.StringIO()
            pdf.to_csv(buf, sep=';', index=False)
            with open(path, "wb") as f:
                f.write(b'\xef\xbb\xbf')
                f.write(buf.getvalue().encode('utf-8'))
            size_mb = os.path.getsize(path) / (1024 * 1024)
            logger.info("Exported CSV: %s (%.1f MB)", path, size_mb)
            return True
        except Exception as e:
            logger.exception("export_to_csv failed: %s", e)
            return False

    def export_to_excel(self, path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        if self.conn is None:
            logger.error("duckdb not available: cannot export.")
            return False
        if pd is None:
            logger.error("pandas required for Excel export.")
            return False
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = pd.read_sql(query, self.conn)
            for c in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
                if c in df.columns:
                    df[c] = df[c].astype(str).replace({r'^nan$': ''}, regex=True)
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            if len(df) <= EXCEL_ROW_LIMIT:
                with pd.ExcelWriter(path, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
            else:
                sheets = (len(df) // EXCEL_ROW_LIMIT) + 1
                with pd.ExcelWriter(path, engine='openpyxl') as writer:
                    for i in range(sheets):
                        df.iloc[i*EXCEL_ROW_LIMIT:(i+1)*EXCEL_ROW_LIMIT].to_excel(writer, index=False, sheet_name=f"Данные_{i+1}")
            logger.info("Exported Excel: %s", path)
            return True
        except Exception as e:
            logger.exception("export_to_excel failed: %s", e)
            return False

    def export_to_parquet(self, path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        if self.conn is None:
            logger.error("duckdb not available: cannot export.")
            return False
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            df.write_parquet(path)
            logger.info("Exported Parquet: %s", path)
            return True
        except Exception as e:
            logger.exception("export_to_parquet failed: %s", e)
            return False

    # ---------- Management ----------
    def delete_by_brand(self, brand_norm: str) -> int:
        if self.conn is None:
            logger.warning("duckdb not available: cannot delete.")
            return 0
        try:
            cnt = self.conn.execute("SELECT COUNT(*) FROM parts WHERE brand_norm= ?", [brand_norm]).fetchone()[0]
            self.conn.execute("DELETE FROM parts WHERE brand_norm= ?", [brand_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            logger.info("Deleted %d parts for brand_norm=%s", cnt, brand_norm)
            return cnt
        except Exception as e:
            logger.exception("delete_by_brand failed: %s", e)
            return 0

    def delete_by_artikul(self, artikul_norm: str) -> int:
        if self.conn is None:
            logger.warning("duckdb not available: cannot delete.")
            return 0
        try:
            cnt = self.conn.execute("SELECT COUNT(*) FROM parts WHERE artikul_norm= ?", [artikul_norm]).fetchone()[0]
            self.conn.execute("DELETE FROM parts WHERE artikul_norm= ?", [artikul_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            logger.info("Deleted %d parts for artikul_norm=%s", cnt, artikul_norm)
            return cnt
        except Exception as e:
            logger.exception("delete_by_artikul failed: %s", e)
            return 0

    # ---------- Cloud sync (best-effort) ----------
    def perform_cloud_sync(self) -> None:
        if not self.cloud_config.get('enabled'):
            logger.info("Cloud sync disabled in config.")
            return
        if not self.cloud_config.get('bucket'):
            logger.warning("Cloud bucket/container not specified.")
            return
        provider = self.cloud_config.get('provider', 's3')
        bucket = self.cloud_config.get('bucket')
        region = self.cloud_config.get('region', '')
        files_to_upload = []
        if self.db_path.exists():
            files_to_upload.append((str(self.db_path), self.db_path.name))
        cfg = self.data_dir / "cloud_config.json"
        if cfg.exists():
            files_to_upload.append((str(cfg), cfg.name))
        app_src_path = self.data_dir / "app_source.py"
        try:
            src = None
            try:
                src = Path(__file__).read_text(encoding='utf-8')
            except Exception:
                try:
                    src = inspect.getsource(sys.modules[__name__])
                except Exception:
                    src = None
            if src:
                app_src_path.write_text(src, encoding='utf-8')
                files_to_upload.append((str(app_src_path), app_src_path.name))
        except Exception as e:
            logger.debug("Could not write source: %s", e)

        success = True
        if provider == 's3':
            try:
                import boto3
                s3 = boto3.client('s3', region_name=region if region else None)
                for local, remote_name in files_to_upload:
                    try:
                        s3.upload_file(local, bucket, remote_name)
                        logger.info("Uploaded %s -> s3://%s/%s", local, bucket, remote_name)
                    except Exception as e:
                        success = False
                        logger.exception("S3 upload failed: %s", e)
                if success:
                    logger.info("S3 sync completed.")
            except Exception as e:
                logger.exception("S3 sync error: %s", e)
                success = False
        elif provider == 'gcs':
            try:
                from google.cloud import storage
                client = storage.Client()
                bucket_obj = client.bucket(bucket)
                for local, remote_name in files_to_upload:
                    blob = bucket_obj.blob(remote_name)
                    blob.upload_from_filename(local)
                    logger.info("Uploaded %s -> gs://%s/%s", local, bucket, remote_name)
                logger.info("GCS sync completed.")
            except Exception as e:
                logger.exception("GCS sync error: %s", e)
                success = False
        elif provider == 'azure':
            try:
                from azure.storage.blob import BlobServiceClient
                conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
                if not conn_str:
                    raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set")
                service = BlobServiceClient.from_connection_string(conn_str)
                container_client = service.get_container_client(bucket)
                try:
                    container_client.create_container()
                except Exception:
                    pass
                for local, remote_name in files_to_upload:
                    blob_client = container_client.get_blob_client(remote_name)
                    with open(local, 'rb') as f:
                        blob_client.upload_blob(f, overwrite=True)
                    logger.info("Uploaded %s -> azure://%s/%s", local, bucket, remote_name)
                logger.info("Azure sync completed.")
            except Exception as e:
                logger.exception("Azure sync error: %s", e)
                success = False
        else:
            logger.error("Unknown cloud provider: %s", provider)
            success = False

        if success:
            self.cloud_config['last_sync'] = int(time.time())
            self.save_cloud_config()

    # ---------- Utilities ----------
    def stats(self) -> Dict[str, int]:
        if self.conn is None:
            return {"parts": 0, "oe": 0, "prices": 0}
        try:
            total_parts = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
            total_oe = self.conn.execute("SELECT COUNT(*) FROM oe").fetchone()[0]
            total_prices = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            return {"parts": total_parts, "oe": total_oe, "prices": total_prices}
        except Exception:
            return {"parts": 0, "oe": 0, "prices": 0}

    def merge_all_data_parallel(self, file_paths: Dict[str, str], max_workers: int = 4) -> Dict[str, "pl.DataFrame"]:
        results: Dict[str, "pl.DataFrame"] = {}
        if not file_paths:
            return results
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for k, v in file_paths.items():
                if v and os.path.exists(v):
                    futures[executor.submit(self.read_and_prepare_file, v, k)] = k
            for fut in as_completed(futures):
                k = futures[fut]
                try:
                    df = fut.result()
                    if df is not None and not df.is_empty():
                        results[k] = df
                except Exception as e:
                    logger.debug("Failed reading %s: %s", k, e)
        return results

# ---------- Simple CLI entrypoint ----------
def cli_main():
    parser = argparse.ArgumentParser(description="AutoParts Catalog CLI")
    sub = parser.add_subparsers(dest="cmd")
    p_import = sub.add_parser("import", help="Import files into DB")
    p_import.add_argument("--oe")
    p_import.add_argument("--cross")
    p_import.add_argument("--barcode")
    p_import.add_argument("--dimensions")
    p_import.add_argument("--images")
    p_import.add_argument("--prices")
    p_export = sub.add_parser("export", help="Export data")
    p_export.add_argument("format", choices=["csv", "excel", "parquet"])
    p_export.add_argument("--out", required=True)
    p_export.add_argument("--no-prices", action="store_true")
    p_export.add_argument("--no-markup", action="store_true")
    p_stats = sub.add_parser("stats", help="Show stats")
    p_sync = sub.add_parser("sync", help="Cloud sync")
    p_delete = sub.add_parser("delete", help="Delete")
    p_delete.add_argument("--brand")
    p_delete.add_argument("--artikul")

    if len(sys.argv) <= 1:
        parser.print_help()
        return
    args = parser.parse_args()
    catalog = HighVolumeAutoPartsCatalog()
    if args.cmd == "import":
        files = {}
        for k in ["oe","cross","barcode","dimensions","images","prices"]:
            val = getattr(args, k, None)
            if val and os.path.exists(val):
                files[k] = val
        if not files:
            logger.error("No valid files provided for import.")
            return
        df_dict = catalog.merge_all_data_parallel(files)
        catalog.process_and_load_data(df_dict)
    elif args.cmd == "export":
        fmt = args.format
        out = args.out
        include_prices = not args.no_prices
        apply_markup = not args.no_markup
        if fmt == "csv":
            ok = catalog.export_to_csv(out, include_prices=include_prices, apply_markup=apply_markup)
        elif fmt == "excel":
            ok = catalog.export_to_excel(out, include_prices=include_prices, apply_markup=apply_markup)
        else:
            ok = catalog.export_to_parquet(out, include_prices=include_prices, apply_markup=apply_markup)
        if not ok:
            logger.error("Export failed.")
    elif args.cmd == "stats":
        s = catalog.stats()
        logger.info("Stats: parts=%d oe=%d prices=%d", s["parts"], s["oe"], s["prices"])
    elif args.cmd == "sync":
        catalog.perform_cloud_sync()
    elif args.cmd == "delete":
        if args.brand:
            if pl:
                norm_b = HighVolumeAutoPartsCatalog.normalize_key(pl.Series([args.brand]))[0]
            else:
                norm_b = re.sub(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "", args.brand).strip().lower()
            cnt = catalog.delete_by_brand(norm_b)
            logger.info("Deleted %d records for brand '%s'", cnt, args.brand)
        if args.artikul:
            if pl:
                norm_a = HighVolumeAutoPartsCatalog.normalize_key(pl.Series([args.artikul]))[0]
            else:
                norm_a = re.sub(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "", args.artikul).strip().lower()
            cnt = catalog.delete_by_artikul(norm_a)
            logger.info("Deleted %d records for artikul '%s'", cnt, args.artikul)

# ---------- Streamlit main (if available) ----------
def run_streamlit_ui():
    if st is None:
        logger.error("Streamlit not available.")
        return
    catalog = HighVolumeAutoPartsCatalog()
    st.title("🚗 AutoParts Catalog 10M+")
    st.sidebar.title("🧭 Меню")
    option = st.sidebar.radio("Выберите раздел", ["Загрузка данных","Экспорт","Статистика","Управление"])
    if option == "Загрузка данных":
        st.header("📥 Загрузка данных")
        col1, col2 = st.columns(2)
        with col1:
            oe_file = st.file_uploader("OE", type=['xlsx'])
            cross_file = st.file_uploader("Cross", type=['xlsx'])
            barcode_file = st.file_uploader("Barcode", type=['xlsx'])
        with col2:
            dimensions_file = st.file_uploader("Dimensions", type=['xlsx'])
            images_file = st.file_uploader("Images", type=['xlsx'])
            prices_file = st.file_uploader("Prices", type=['xlsx'])
        uploaded = {'oe': oe_file, 'cross': cross_file, 'barcode': barcode_file, 'dimensions': dimensions_file, 'images': images_file, 'prices': prices_file}
        if st.button("Обработать и загрузить"):
            saved = {}
            for k, f in uploaded.items():
                if f:
                    p = catalog.data_dir / f"{k}_{int(time.time())}.xlsx"
                    with open(p, "wb") as fh:
                        fh.write(f.getbuffer())
                    saved[k] = str(p)
            if saved:
                with st.spinner("Чтение..."):
                    df_dict = catalog.merge_all_data_parallel(saved)
                with st.spinner("Загрузка..."):
                    catalog.process_and_load_data(df_dict)
                st.success("Загрузка завершена")
            else:
                st.warning("Нет загруженных файлов")
    elif option == "Экспорт":
        catalog.show_export_interface()
    elif option == "Статистика":
        catalog.show_statistics()
    elif option == "Управление":
        catalog.show_data_management()

# ---------- Entrypoint ----------
if __name__ == "__main__":
    # Если запущен через streamlit - интерфейс Streamlit вызовет run_streamlit_ui;
    # при обычном запуске - CLI.
    if st is not None and ("STREAMLIT_SERVER" in os.environ or any("streamlit" in a.lower() for a in sys.argv)):
        try:
            run_streamlit_ui()
        except Exception as e:
            logger.exception("Streamlit UI failed: %s", e)
            cli_main()
    else:
        cli_main()
