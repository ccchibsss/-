# -*- coding: utf-8 -*-
import platform
import sys
import polars as pl
import duckdb
import streamlit as st
import os
import time
import io
import json
import logging
import warnings
import re
import inspect
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, Optional, List

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_ROW_LIMIT = 1_000_000

class HighVolumeAutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)

        # Конфигурации
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()

        st.set_page_config(page_title="AutoParts Catalog 10M+", layout="wide", page_icon="🚗")

    # --- Конфигурации ---
    def load_cloud_config(self):
        path = self.data_dir / "cloud_config.json"
        default = {"enabled": False, "provider": "s3", "bucket": "", "region": "", "sync_interval": 3600, "last_sync": 0}
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except:
                return default
        else:
            path.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding='utf-8')
            return default

    def save_cloud_config(self):
        self.cloud_config["last_sync"] = int(time.time())
        (self.data_dir / "cloud_config.json").write_text(json.dumps(self.cloud_config, ensure_ascii=False, indent=2), encoding='utf-8')
    
    def load_price_rules(self):
        path = self.data_dir / "price_rules.json"
        default = {"global_markup": 0.2, "brand_markups": {}, "min_price": 0.0, "max_price": 99999.0}
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except:
                return default
        else:
            path.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding='utf-8')
            return default

    def save_price_rules(self):
        (self.data_dir / "price_rules.json").write_text(json.dumps(self.price_rules, ensure_ascii=False, indent=2), encoding='utf-8')
    
    def load_exclusion_rules(self):
        path = self.data_dir / "exclusion_rules.txt"
        if path.exists():
            try:
                return [line.strip() for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except:
                return []
        else:
            path.write_text("Кузов\nСтекла\nМасла", encoding='utf-8')
            return ["Кузов", "Стекла", "Масла"]
    def save_exclusion_rules(self):
        (self.data_dir / "exclusion_rules.txt").write_text("\n".join(self.exclusion_rules), encoding='utf-8')

    def load_category_mapping(self):
        path = self.data_dir / "category_mapping.txt"
        default = {"Радиатор": "Охлаждение", "Шаровая опора": "Подвеска", "Фильтр масляный": "Фильтры", "Тормозные колодки": "Тормоза"}
        if path.exists():
            try:
                mapping = {}
                for line in path.read_text(encoding='utf-8').splitlines():
                    if "|" in line:
                        k,v = line.split("|",1)
                        mapping[k.strip()] = v.strip()
                return mapping
            except:
                return default
        else:
            path.write_text("\n".join([f"{k}|{v}" for k,v in default.items()]), encoding='utf-8')
            return default
    def save_category_mapping(self):
        (self.data_dir / "category_mapping.txt").write_text("\n".join([f"{k}|{v}" for k,v in self.category_mapping.items()]), encoding='utf-8')

    # --- БД ---
    def setup_database(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS oe (oe_number_norm VARCHAR PRIMARY KEY, oe_number VARCHAR, name VARCHAR, applicability VARCHAR, category VARCHAR)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS parts (artikul_norm VARCHAR, brand_norm VARCHAR, artikul VARCHAR, brand VARCHAR, multiplicity INTEGER, barcode VARCHAR, length DOUBLE, width DOUBLE, height DOUBLE, weight DOUBLE, image_url VARCHAR, dimensions_str VARCHAR, description VARCHAR, PRIMARY KEY (artikul_norm, brand_norm))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS cross_references (oe_number_norm VARCHAR, artikul_norm VARCHAR, brand_norm VARCHAR, PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS prices (artikul_norm VARCHAR, brand_norm VARCHAR, price DOUBLE, currency VARCHAR DEFAULT 'RUB', PRIMARY KEY (artikul_norm, brand_norm))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS metadata (key VARCHAR PRIMARY KEY, value VARCHAR)")
        self.create_indexes()

    def create_indexes(self):
        for sql in [
            "CREATE INDEX IF NOT EXISTS idx_oe_number_norm ON oe(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_keys ON parts(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)"
        ]:
            try:
                self.conn.execute(sql)
            except:
                pass

    # --- Нормализация ---
    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        return (series.fill_null("").cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        name_lower = name_series.str.to_lowercase()
        expr = pl.when(pl.lit(False)).then(pl.lit(None))
        for k,v in self.category_mapping.items():
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

    # --- Разбор весогабаритов ---
    DIM_SEP_REGEX = re.compile(r'[x×*/\s,;]+', flags=re.IGNORECASE)
    WEIGHT_REGEX = re.compile(r'(\d+[.,]?\d*)\s*(kg|кг|g|гр|гр\.|g\.|grams|lb|lbs|фунт|oz|унц)', flags=re.IGNORECASE)

    def _to_float(self, s: str) -> Optional[float]:
        if s is None:
            return None
        s = str(s).strip().replace(',', '.')
        try:
            return float(s)
        except:
            return None

    def parse_dimension_string(self, s: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        Parse a dimensions string and return (length_cm, width_cm, height_cm, weight_kg)
        Supports units: mm, cm, m, in/inch, kg, g, lb, oz.
        """
        if not s:
            return (None, None, None, None)
        text = str(s).lower().strip()
        # Try to extract weight first
        weight_kg = None
        wmatch = self.WEIGHT_REGEX.search(text)
        if wmatch:
            val = self._to_float(wmatch.group(1))
            unit = wmatch.group(2).lower()
            if val is not None:
                if unit in ('kg','кг'):
                    weight_kg = val
                elif unit in ('g','гр','гр.','g.','grams'):
                    weight_kg = val / 1000.0
                elif unit in ('lb','lbs','фунт'):
                    weight_kg = val * 0.45359237
                elif unit in ('oz','унц'):
                    weight_kg = val * 0.0283495231

        # Clean common separators and units for dims
        # Remove weight piece from text to avoid confusion
        if wmatch:
            text_dims = text[:wmatch.start()] + text[wmatch.end():]
        else:
            text_dims = text

        # Replace multiple separators to 'x'
        parts = re.split(r'[x×*/,;]+', text_dims)
        # Also try to find number sequences with units like "120x80x30 mm" -> unit at end
        unit = None
        # find trailing unit like mm/cm/m/in/inch/см/м
        trailing_unit_match = re.search(r'(mm|мм|cm|см|m|м|in|inch|дюйм|дюйма)$', text_dims.strip())
        if trailing_unit_match:
            unit = trailing_unit_match.group(1)
        # collect numeric tokens
        nums = []
        for p in parts:
            p = p.strip()
            if not p:
                continue
            # remove unit tokens inside token
            tok = re.sub(r'(mm|мм|cm|см|m|м|in|inch|дюйм|дюйма)', '', p).strip()
            # maybe token like "120/80/30"
            subp = re.split(r'[/\s]+', tok)
            for sp in subp:
                nv = self._to_float(sp)
                if nv is not None:
                    nums.append(nv)
        # If no numbers found, fallback to any number in string
        if not nums:
            nums = [self._to_float(x) for x in re.findall(r'\d+[.,]?\d*', text_dims)]
            nums = [n for n in nums if n is not None]
        # Determine unit multiplier to convert to cm
        mul = 1.0  # assume cm by default if unit not found
        if unit:
            unit = unit.lower()
            if unit in ('mm','мм'):
                mul = 0.1
            elif unit in ('cm','см'):
                mul = 1.0
            elif unit in ('m','м'):
                mul = 100.0
            elif unit in ('in','inch','дюйм','дюйма'):
                mul = 2.54
        else:
            # heuristics: if numbers are large ( > 100 ) maybe in mm
            if nums and max(nums) > 300:
                mul = 0.1  # mm -> cm
            else:
                mul = 1.0  # assume cm

        length = width = height = None
        if len(nums) >= 3:
            length = nums[0] * mul
            width = nums[1] * mul
            height = nums[2] * mul
        elif len(nums) == 2:
            length = nums[0] * mul
            width = nums[1] * mul
        elif len(nums) == 1:
            length = nums[0] * mul

        # final normalization: None if NaN
        def norm(x):
            return None if x is None else float(x)
        return (norm(length), norm(width), norm(height), None if weight_kg is None else float(weight_kg))

    def parse_dimensions_series(self, series: pl.Series) -> pl.DataFrame:
        """
        Parse a polars Series of dimension strings -> returns DataFrame with columns length,width,height,weight
        columns are floats (cm for dims, kg for weight) or None
        """
        results = [self.parse_dimension_string(s) for s in series.to_list()]
        lengths = [r[0] for r in results]
        widths = [r[1] for r in results]
        heights = [r[2] for r in results]
        weights = [r[3] for r in results]
        return pl.DataFrame({
            'parsed_length': lengths,
            'parsed_width': widths,
            'parsed_height': heights,
            'parsed_weight': weights
        })

    # --- Обработка файлов ---
    def detect_columns(self, actual_cols, expected_cols):
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
        mapping = {}
        actual_lower = {c.lower(): c for c in actual_cols}
        for key in expected_cols:
            for variant in variants.get(key, [key]):
                for act_lower, act_orig in actual_lower.items():
                    if variant.lower() in act_lower and act_orig not in mapping:
                        mapping[act_orig] = key
        return mapping

    def read_and_prepare_file(self, path, ftype):
        try:
            df = pl.read_excel(str(path), engine='calamine')
            if df.is_empty():
                return pl.DataFrame()
        except:
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
            return pl.DataFrame()
        df = df.rename(colmap)
        for c in ['artikul', 'brand', 'oe_number']:
            if c in df.columns:
                df = df.with_columns(self.clean_values(pl.col(c)).alias(c))
        for c in ['oe_number','artikul','brand']:
            if c in df.columns:
                df = df.with_columns(self.normalize_key(pl.col(c)).alias(f"{c}_norm"))

        # If dimensions_str present, parse into numeric fields when numeric fields are missing
        if 'dimensions_str' in df.columns:
            try:
                parsed = self.parse_dimensions_series(df['dimensions_str'])
                # prefer existing numeric columns if exist, else fill from parsed
                if 'length' not in df.columns:
                    df = df.with_columns(parsed['parsed_length'].alias('length'))
                else:
                    df = df.with_columns(pl.when(pl.col('length').is_null() | (pl.col('length') == '')).then(parsed['parsed_length']).otherwise(pl.col('length')).alias('length'))
                if 'width' not in df.columns:
                    df = df.with_columns(parsed['parsed_width'].alias('width'))
                else:
                    df = df.with_columns(pl.when(pl.col('width').is_null() | (pl.col('width') == '')).then(parsed['parsed_width']).otherwise(pl.col('width')).alias('width'))
                if 'height' not in df.columns:
                    df = df.with_columns(parsed['parsed_height'].alias('height'))
                else:
                    df = df.with_columns(pl.when(pl.col('height').is_null() | (pl.col('height') == '')).then(parsed['parsed_height']).otherwise(pl.col('height')).alias('height'))
                if 'weight' not in df.columns:
                    df = df.with_columns(parsed['parsed_weight'].alias('weight'))
                else:
                    df = df.with_columns(pl.when(pl.col('weight').is_null() | (pl.col('weight') == '')).then(parsed['parsed_weight']).otherwise(pl.col('weight')).alias('weight'))
                # cast numeric columns to floats
                for c in ['length','width','height','weight']:
                    if c in df.columns:
                        df = df.with_columns(pl.col(c).cast(pl.Float64))
            except Exception as e:
                logger.debug("Parse dimensions failed: %s", e)

        return df

    def clean_values(self, series):
        return series.fill_null("").cast(pl.Utf8).str.replace_all("'", "").str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "").str.replace_all(r"\s+", " ").str.strip_chars()

    # --- Загрузка/обновление ---
    def upsert_data(self, table, df, pk):
        if df.is_empty():
            return
        df = df.unique(keep='first')
        temp_name = f"temp_{table}_{int(time.time())}"
        try:
            self.conn.register(temp_name, df.to_arrow())
        except:
            return
        try:
            pk_csv = ", ".join(f'"{c}"' for c in pk)
            self.conn.execute(f"""
                DELETE FROM {table}
                WHERE ({pk_csv}) IN (SELECT {pk_csv} FROM {temp_name});
            """)
            self.conn.execute(f"""
                INSERT INTO {table}
                SELECT * FROM {temp_name};
            """)
        except:
            pass
        finally:
            try:
                self.conn.unregister(temp_name)
            except:
                pass

    def upsert_prices(self, df):
        if df.is_empty():
            return
        if 'artikul' in df.columns and 'brand' in df.columns:
            df = df.with_columns([self.normalize_key(pl.col('artikul')).alias('artikul_norm'), self.normalize_key(pl.col('brand')).alias('brand_norm')])
        if 'currency' not in df.columns:
            df = df.with_columns(pl.lit('RUB').alias('currency'))
        df = df.filter((pl.col('price') >= self.load_price_rules()['min_price']) & (pl.col('price') <= self.load_price_rules()['max_price']))
        self.upsert_data('prices', df, ['artikul_norm','brand_norm'])

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        # Обновление базы
        st.info("🔄 Начинается обновление базы данных...")
        steps = ['oe','cross','parts']
        progress = st.progress(0)
        count_steps = len(steps)
        step_idx = 0

        # Обработка OE
        if 'oe' in dataframes:
            step_idx+=1
            progress.progress(step_idx / count_steps)
            df_oe = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df_oe.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(subset=['oe_number_norm'])
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))
            self.upsert_data('oe', oe_df, ['oe_number_norm'])

            cross_df = df_oe.filter(pl.col('artikul_norm') != "").select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df, ['oe_number_norm','artikul_norm','brand_norm'])

        # Обработка cross
        if 'cross' in dataframes:
            step_idx+=1
            progress.progress(step_idx / count_steps)
            df_cross = dataframes['cross'].filter((pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            self.upsert_data('cross_references', df_cross, ['oe_number_norm','artikul_norm','brand_norm'])

        # Обработка prices
        if 'prices' in dataframes:
            df_price = dataframes['prices']
            if not df_price.is_empty():
                self.upsert_prices(df_price)
                st.success(f"Обновлено цен: {len(df_price)}")

        # Обработка parts
        step_idx+=1
        progress.progress(step_idx / count_steps)
        # Собираем части из разных файлов
        parts_df = None
        key_files = {k:v for k,v in dataframes.items() if k in ['oe','barcode','images','dimensions']}
        if key_files:
            all_parts = pl.concat([v.select(['artikul','artikul_norm','brand','brand_norm']) for v in key_files.values() if 'artikul_norm' in v.columns]).filter(pl.col('artikul_norm') != "").unique(subset=['artikul_norm','brand_norm'])
            for f in ['oe','barcode','images','dimensions']:
                if f not in key_files:
                    continue
                df_part = key_files[f]
                if df_part.is_empty() or 'artikul_norm' not in df_part.columns:
                    continue
                join_cols = [c for c in df_part.columns if c not in ['artikul','artikul_norm','brand','brand_norm']]
                if not join_cols:
                    continue
                existing_cols = set(all_parts.columns)
                join_cols = [c for c in join_cols if c not in existing_cols]
                if not join_cols:
                    continue
                df_sub = df_part.select(['artikul_norm','brand_norm']+join_cols).unique(subset=['artikul_norm','brand_norm'])
                all_parts = all_parts.join(df_sub, on=['artikul_norm','brand_norm'], how='left')
        if 'all_parts' in locals() and all_parts and not all_parts.is_empty():
            # Формируем описание
            if 'multiplicity' not in all_parts.columns:
                all_parts = all_parts.with_columns(pl.lit(1).cast(pl.Int32).alias('multiplicity'))
            else:
                all_parts = all_parts.with_columns(pl.col('multiplicity').fill_null(1).cast(pl.Int32))
            for c in ['length','width','height']:
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

            # If dimensions_str present and numeric dims missing, try parse
            if 'dimensions_str' in all_parts.columns:
                try:
                    parsed = self.parse_dimensions_series(all_parts['dimensions_str'])
                    # fill missing numeric dims
                    all_parts = all_parts.with_columns(
                        pl.when(pl.col('length').is_null()).then(parsed['parsed_length']).otherwise(pl.col('length')).alias('length'),
                        pl.when(pl.col('width').is_null()).then(parsed['parsed_width']).otherwise(pl.col('width')).alias('width'),
                        pl.when(pl.col('height').is_null()).then(parsed['parsed_height']).otherwise(pl.col('height')).alias('height'),
                        pl.when(pl.col('weight').is_null()).then(parsed['parsed_weight']).otherwise(pl.col('weight')).alias('weight')
                    )
                except Exception as e:
                    logger.debug("bulk parse dims failed: %s", e)

            # Создаем dimensions_str если отсутствует
            all_parts = all_parts.with_columns([
                pl.col('length').cast(pl.Utf8).fill_null('').alias('_length'),
                pl.col('width').cast(pl.Utf8).fill_null('').alias('_width'),
                pl.col('height').cast(pl.Utf8).fill_null('').alias('_height')
            ])
            all_parts = all_parts.with_columns(
                pl.when(
                    (pl.col('dimensions_str').is_not_null()) & (pl.col('dimensions_str') != '')
                ).then(pl.col('dimensions_str')).otherwise(
                    pl.concat_str([pl.col('_length'), pl.lit('x'), pl.col('_width'), pl.lit('x'), pl.col('_height')], separator='')
                ).alias('dimensions_str')
            ).drop(['_length','_width','_height'])
            # Артикул и бренд
            if 'artikul' not in all_parts.columns:
                all_parts = all_parts.with_columns(pl.lit('').alias('artikul'))
            if 'brand' not in all_parts.columns:
                all_parts = all_parts.with_columns(pl.lit('').alias('brand'))
            # Описание
            all_parts = all_parts.with_columns(
                description=pl.concat_str([
                    'Артикул: ', pl.col('artikul'), ', ',
                    'Бренд: ', pl.col('brand'), ', ',
                    'Кратность: ', pl.col('multiplicity').cast(pl.Utf8), ' шт.'
                ])
            )
            # Финальная таблица
            final_cols = ['artikul_norm','brand_norm','artikul','brand','multiplicity','barcode','length','width','height','weight','image_url','dimensions_str','description']
            df_final = all_parts.select([pl.col(c) if c in all_parts.columns else pl.lit('').alias(c) for c in final_cols])
            self.upsert_data('parts', df_final, ['artikul_norm','brand_norm'])
        progress.progress(1.0, text="Обновление завершено")
        time.sleep(1)

    # --- Экспорт ---
    def _get_brand_markups_sql(self):
        rows = []
        for b,m in self.load_price_rules().get('brand_markups', {}).items():
            safe_b = b.replace("'", "''")
            rows.append(f"SELECT '{safe_b}' AS brand, {m} AS markup")
        return " UNION ALL ".join(rows) if rows else "SELECT NULL AS brand, 0 AS markup LIMIT 0"

    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
        desc_text = ("Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. "
                     "Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей. "
                     "В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электроматериалы, автомасла, инструмент, "
                     "а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. "
                     "Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. "
                     "Выбирайте только лучшее — надежность и качество от ведущих производителей.")
        brand_markups_sql = self._get_brand_markups_sql()

        # Колонки
        select_parts = []

        # Цена
        if include_prices:
            if apply_markup:
                global_markup = self.load_price_rules().get('global_markup', 0)
                select_parts.append(f"CASE WHEN pr.price IS NOT NULL THEN pr.price * (1 + COALESCE(brm.markup, {global_markup})) ELSE pr.price END AS \"Цена\"")
            else:
                select_parts.append('pr.price AS "Цена"')
            select_parts.append("COALESCE(pr.currency, 'RUB') AS \"Валюта\"")
        # Остальные
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

    def export_to_csv(self, path, selected_columns=None, include_prices=True, apply_markup=True):
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total==0:
            st.warning("Нет данных для экспорта")
            return False
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = self.conn.execute(query).pl()
        import pandas as pd
        pdf = df.to_pandas()
        for c in ["Длинна","Ширина","Высота","Вес","Длинна/Ширина/Высота"]:
            if c in pdf.columns:
                pdf[c] = pdf[c].astype(str).replace({'nan': ''})
        Path("auto_parts_data").mkdir(parents=True, exist_ok=True)
        buf = io.StringIO()
        pdf.to_csv(buf, sep=';', index=False)
        with open(path, "wb") as f:
            f.write(b'\xef\xbb\xbf')
            f.write(buf.getvalue().encode('utf-8'))
        size_mb = os.path.getsize(path)/(1024*1024)
        st.success(f"Данные экспортированы: {path} ({size_mb:.1f} МБ)")
        return True

    def export_to_excel(self, path, selected_columns=None, include_prices=True, apply_markup=True):
        import pandas as pd
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = pd.read_sql(query, self.conn)
        for c in ["Длинна","Ширина","Высота","Вес","Длинна/Ширина/Высота"]:
            if c in df.columns:
                df[c] = df[c].astype(str).replace({r'^nan$': ''}, regex=True)
        if len(df)<=EXCEL_ROW_LIMIT:
            with pd.ExcelWriter(path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
        else:
            sheets = (len(df)//EXCEL_ROW_LIMIT)+1
            with pd.ExcelWriter(path, engine='openpyxl') as writer:
                for i in range(sheets):
                    df.iloc[i*EXCEL_ROW_LIMIT:(i+1)*EXCEL_ROW_LIMIT].to_excel(writer, index=False, sheet_name=f"Данные_{i+1}")
        return True

    def export_to_parquet(self, path, selected_columns=None, include_prices=True, apply_markup=True):
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            df.write_parquet(path)
            return True
        except:
            return False

    # --- Управление ---
    def delete_by_brand(self, brand_norm):
        try:
            cnt = self.conn.execute("SELECT COUNT(*) FROM parts WHERE brand_norm= ?", [brand_norm]).fetchone()[0]
            self.conn.execute("DELETE FROM parts WHERE brand_norm= ?", [brand_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            return cnt
        except:
            return 0

    def delete_by_artikul(self, artikul_norm):
        try:
            cnt = self.conn.execute("SELECT COUNT(*) FROM parts WHERE artikul_norm= ?", [artikul_norm]).fetchone()[0]
            self.conn.execute("DELETE FROM parts WHERE artikul_norm= ?", [artikul_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            return cnt
        except:
            return 0

    # --- интерфейсы ---
    def show_export(self):
        st.header("📤 Экспорт данных")
        total = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        st.info(f"Всего: {total}")
        if total==0:
            st.warning("Нет данных")
            return
        format_ = st.radio("Формат", ["CSV","Excel","Parquet"])
        selected_columns = st.multiselect("Колонки", ["Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание", "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение", "Цена", "Валюта"])
        include_prices = st.checkbox("Включить цены", value=True)
        apply_markup = st.checkbox("Применить наценку", value=True, disabled=not include_prices)
        if st.button("🚀 Экспортировать"):
            path = self.data_dir / f"export.{format_.lower()}"
            with st.spinner("Генерация файла..."):
                if format_=="CSV":
                    self.export_to_csv(str(path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_=="Excel":
                    self.export_to_excel(str(path), selected_columns if selected_columns else None, include_prices, apply_markup)
                else:
                    self.export_to_parquet(str(path), selected_columns if selected_columns else None, include_prices, apply_markup)
            with open(path, "rb") as f:
                st.download_button("⬇️ Скачать файл", f, file_name=path.name)

    def show_price_settings(self):
        st.header("💰 Управление ценами и наценками")
        st.subheader("Общая наценка")
        g_markup = st.number_input("Общая наценка (%)", min_value=0.0, max_value=500.0, value=self.load_price_rules()['global_markup']*100, step=0.1)
        self.price_rules['global_markup'] = g_markup/100
        st.subheader("Наценки по брендам")
        try:
            brands = self.conn.execute("SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL").fetchall()
            brands = [b[0] for b in brands]
        except:
            brands=[]
        if brands:
            brand = st.selectbox("Бренд", brands)
            current_markup = self.load_price_rules().get('brand_markups', {}).get(brand, self.load_price_rules().get('global_markup',0))
            markup = st.number_input("Наценка (%)", min_value=0.0, max_value=500.0, value=current_markup*100, step=0.1)
            if st.button("💾 Сохранить"):
                if 'brand_markups' not in self.price_rules:
                    self.price_rules['brand_markups'] = {}
                self.price_rules['brand_markups'][brand] = markup/100
                self.save_price_rules()
                st.success(f"Наценка для {brand} сохранена")
        st.subheader("Ограничения цен")
        min_p = st.number_input("Минимальная цена", min_value=0.0, value=self.load_price_rules().get('min_price',0), step=0.01)
        max_p = st.number_input("Максимальная цена", min_value=0.0, value=self.load_price_rules().get('max_price',99999), step=0.01)
        self.price_rules['min_price']=min_p
        self.price_rules['max_price']=max_p
        if st.button("💾 Сохранить все"):
            self.save_price_rules()
            st.success("Настройки сохранены")

    def show_exclusion(self):
        st.header("🚫 Управление исключениями")
        st.info("Товары, содержащие эти слова, исключены из экспорта")
        current = "\n".join(self.exclusion_rules)
        new = st.text_area("Исключения (по одному слову)", value=current, height=200)
        if st.button("💾 Сохранить исключения"):
            self.exclusion_rules = [line.strip() for line in new.splitlines() if line.strip()]
            self.save_exclusion_rules()

    def show_categories(self):
        st.header("🗂️ Управление категориями товаров")
        st.info("Настройте соответствие названий и категорий")
        if hasattr(self, 'category_mapping'):
            df = pl.DataFrame({"Название": list(self.category_mapping.keys()), "Категория": list(self.category_mapping.values())}).to_pandas()
            st.dataframe(df)
        else:
            st.write("Нет правил")
        st.subheader("Добавить / Обновить")
        k = st.text_input("Ключевое слово")
        v = st.text_input("Категория")
        if st.button("➕ Добавить/Обновить"):
            if k and v:
                self.category_mapping[k]=v
                self.save_category_mapping()
                st.success("Правило сохранено")
        st.subheader("Удалить")
        if hasattr(self, 'category_mapping') and self.category_mapping:
            to_del = st.selectbox("Правила", list(self.category_mapping.keys()))
            if st.button("🗑️ Удалить"):
                del self.category_mapping[to_del]
                self.save_category_mapping()
                st.success("Правило удалено")

    def show_cloud_sync(self):
        st.header("☁️ Облачная синхронизация")
        self.cloud_config['enabled'] = st.checkbox("Включить", value=self.cloud_config.get('enabled', False))
        providers = ["s3","gcs","azure"]
        current_idx = providers.index(self.cloud_config.get('provider','s3')) if self.cloud_config.get('provider') in providers else 0
        self.cloud_config['provider'] = st.selectbox("Провайдер", providers, index=current_idx)
        self.cloud_config['bucket'] = st.text_input("Bucket/Container", value=self.cloud_config.get('bucket',''))
        self.cloud_config['region'] = st.text_input("Регион", value=self.cloud_config.get('region',''))
        self.cloud_config['sync_interval'] = st.number_input("Интервал (сек)", min_value=300, max_value=86400, value=int(self.cloud_config.get('sync_interval',3600)))
        if st.button("💾 Сохранить настройки"):
            self.save_cloud_config()
            st.success("Настройки сохранены")
        if st.button("🔄 Обновить сейчас"):
            self.perform_cloud_sync()
        last_sync = self.cloud_config.get('last_sync',0)
        if last_sync:
            st.info(f"Последняя синхронизация: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_sync))}")

    def perform_cloud_sync(self):
        """
        Upload DB, config and current app source to configured cloud provider.
        Supports s3 (boto3), gcs (google-cloud-storage), azure (azure.storage.blob).
        Credentials are expected to be configured in environment or default SDK locations.
        """
        if not self.cloud_config.get('enabled'):
            st.warning("Облачная синхронизация отключена")
            return
        if not self.cloud_config.get('bucket'):
            st.warning("Не указан bucket")
            return
        with st.spinner("Синхронизация..."):
            bucket = self.cloud_config.get('bucket')
            provider = self.cloud_config.get('provider', 's3')
            region = self.cloud_config.get('region', '')
            files_to_upload = []
            # files: DB and config
            if self.db_path.exists():
                files_to_upload.append((str(self.db_path), f"{self.db_path.name}"))
            cfg = self.data_dir / "cloud_config.json"
            if cfg.exists():
                files_to_upload.append((str(cfg), cfg.name))
            # try save current source to file
            app_src_path = self.data_dir / "app_source.py"
            try:
                src = None
                # prefer reading actual file
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
                        key = remote_name
                        try:
                            s3.upload_file(local, bucket, key)
                        except Exception as e:
                            success = False
                            logger.exception("S3 upload failed: %s", e)
                            st.error(f"S3 upload failed: {e}")
                    if success:
                        st.success("Файлы успешно загружены в S3")
                except Exception as e:
                    st.error(f"Ошибка S3: {e}")
                    success = False
            elif provider == 'gcs':
                try:
                    from google.cloud import storage
                    client = storage.Client()
                    bucket_obj = client.bucket(bucket)
                    for local, remote_name in files_to_upload:
                        blob = bucket_obj.blob(remote_name)
                        blob.upload_from_filename(local)
                    st.success("Файлы успешно загружены в GCS")
                except Exception as e:
                    st.error(f"Ошибка GCS: {e}")
                    success = False
            elif provider == 'azure':
                try:
                    from azure.storage.blob import BlobServiceClient
                    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
                    if not conn_str:
                        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set in env")
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
                    st.success("Файлы успешно загружены в Azure Blob")
                except Exception as e:
                    st.error(f"Ошибка Azure: {e}")
                    success = False
            else:
                st.error("Неизвестный провайдер")
                success = False

            if success:
                self.cloud_config['last_sync'] = int(time.time())
                self.save_cloud_config()
            time.sleep(1.2)

    # --- Вспомогательные ---
    def merge_all_data_parallel(self, file_paths: Dict[str,str], max_workers=4):
        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for k,v in file_paths.items():
                if v and os.path.exists(v):
                    futures[executor.submit(self.read_and_prepare_file, v, k)] = k
            for fut in as_completed(futures):
                k = futures[fut]
                try:
                    df = fut.result()
                    if not df.is_empty():
                        results[k]=df
                except:
                    pass
        return results

    # --- Statistics simple implementation ---
    def show_statistics(self):
        st.header("📊 Статистика")
        try:
            total_parts = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
            total_oe = self.conn.execute("SELECT COUNT(*) FROM oe").fetchone()[0]
            total_prices = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            st.info(f"Записей в parts (уникальные артикули+бренд): {total_parts}")
            st.info(f"OE номеров: {total_oe}")
            st.info(f"Цен: {total_prices}")
        except Exception as e:
            st.error(f"Не удалось получить статистику: {e}")

    def delete_local_cloud_source(self):
        # helper to cleanup written source file if needed
        try:
            p = self.data_dir / "app_source.py"
            if p.exists():
                p.unlink()
        except:
            pass

    def _del_brand(self):
        try:
            brands = self.conn.execute("SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL").fetchall()
            brands = [b[0] for b in brands]
        except:
            brands=[]
        if brands:
            brand = st.selectbox("Выберите бренд", brands)
            if st.button("Удалить бренд"):
                norm_b = self.normalize_key(pl.Series([brand]))[0]
                cnt = self.delete_by_brand(norm_b)
                st.success(f"Удалено {cnt}")
                st.experimental_rerun()

    def _del_artikul(self):
        artikul = st.text_input("Артикул")
        if artikul:
            norm_a = self.normalize_key(pl.Series([artikul]))[0]
            cnt = self.delete_by_artikul(norm_a)
            st.success(f"Удалено {cnt}")
            st.experimental_rerun()

# --- Основный запуск ---
def main():
    st.title("🚗 AutoParts Catalog 10M+")
    st.markdown("### Платформа для больших каталогов автозапчастей")
    catalog = HighVolumeAutoPartsCatalog()
    st.sidebar.title("🧭 Меню")
    option = st.sidebar.radio("Выберите раздел", ["Загрузка данных", "Экспорт", "Статистика", "Управление"])
    if option=="Загрузка данных":
        st.header("📥 Загрузка данных")
        cols = st.columns(2)
        with cols[0]:
            oe_file = st.file_uploader("Основные данные (OE)", type=['xlsx'])
            cross_file = st.file_uploader("Кроссы (OE→Артикул)", type=['xlsx'])
            barcode_file = st.file_uploader("Штрих-коды", type=['xlsx'])
        with cols[1]:
            weight_dims_file = st.file_uploader("Вес и габариты", type=['xlsx'])
            images_file = st.file_uploader("Изображения", type=['xlsx'])
            prices_file = st.file_uploader("Цены", type=['xlsx'])
        uploaded = {'oe': oe_file, 'cross': cross_file, 'barcode': barcode_file, 'dimensions': weight_dims_file, 'images': images_file, 'prices': prices_file}
        if st.button("Обработать и загрузить"):
            paths = {}
            for k,v in uploaded.items():
                if v:
                    p = catalog.data_dir / f"{k}_{int(time.time())}.xlsx"
                    with open(p,'wb') as f:
                        f.write(v.read())
                    paths[k]=str(p)
            if paths:
                with st.spinner("Обработка файлов..."):
                    df_dict = catalog.merge_all_data_parallel(paths)
                with st.spinner("Загрузка в базу..."):
                    catalog.process_and_load_data(df_dict)
            else:
                st.warning("Загрузите хотя бы один файл")
    elif option=="Экспорт":
        catalog.show_export()
    elif option=="Статистика":
        catalog.show_statistics()
    elif option=="Управление":
        catalog.show_management()

if __name__=="__main__":
    main()
