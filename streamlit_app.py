import platform
import sys
import polars as pl
import duckdb
import streamlit as st
import os
import time
import logging
import io
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import json

warnings.filterwarnings('ignore')

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_ROW_LIMIT = 1_000_000


class HighVolumeAutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)

        # Загрузка конфигураций
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()

        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()

        st.set_page_config(
            page_title="AutoParts Catalog 10M+",
            layout="wide",
            page_icon="🚗"
        )

    # --- Конфигурации ---
    def load_cloud_config(self) -> Dict[str, Any]:
        config_path = self.data_dir / "cloud_config.json"
        default_config = {
            "enabled": False,
            "provider": "s3",
            "bucket": "",
            "region": "",
            "sync_interval": 3600,
            "last_sync": 0
        }
        if config_path.exists():
            try:
                return json.loads(config_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка чтения cloud_config.json: {e}")
                return default_config
        else:
            config_path.write_text(json.dumps(
                default_config, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_config

    def save_cloud_config(self):
        config_path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        config_path.write_text(json.dumps(
            self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_price_rules(self) -> Dict[str, Any]:
        price_rules_path = self.data_dir / "price_rules.json"
        default_rules = {
            "global_markup": 0.2,
            "brand_markups": {},
            "min_price": 0.0,
            "max_price": 99999.0
        }
        if price_rules_path.exists():
            try:
                return json.loads(price_rules_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка чтения price_rules.json: {e}")
                return default_rules
        else:
            price_rules_path.write_text(json.dumps(
                default_rules, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_rules

    def save_price_rules(self):
        price_rules_path = self.data_dir / "price_rules.json"
        price_rules_path.write_text(json.dumps(
            self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')

    def load_exclusion_rules(self) -> List[str]:
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        if exclusion_path.exists():
            try:
                return [line.strip() for line in exclusion_path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except Exception as e:
                logger.error(f"Ошибка чтения exclusion_rules.txt: {e}")
                return []
        else:
            content = "Кузов\nСтекла\nМасла"
            exclusion_path.write_text(content, encoding='utf-8')
            return ["Кузов", "Стекла", "Масла"]

    def save_exclusion_rules(self):
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        exclusion_path.write_text(
            "\n".join(self.exclusion_rules), encoding='utf-8')

    def load_category_mapping(self) -> Dict[str, str]:
        category_path = self.data_dir / "category_mapping.txt"
        default_mapping = {
            "Радиатор": "Охлаждение",
            "Шаровая опора": "Подвеска",
            "Фильтр масляный": "Фильтры",
            "Тормозные колодки": "Тормоза"
        }
        if category_path.exists():
            try:
                mapping = {}
                for line in category_path.read_text(encoding='utf-8').splitlines():
                    if line.strip() and "|" in line:
                        key, value = line.split("|", 1)
                        mapping[key.strip()] = value.strip()
                return mapping
            except Exception as e:
                logger.error(f"Ошибка чтения category_mapping.txt: {e}")
                return default_mapping
        else:
            content = "\n".join(
                [f"{k}|{v}" for k, v in default_mapping.items()])
            category_path.write_text(content, encoding='utf-8')
            return default_mapping

    def save_category_mapping(self):
        category_path = self.data_dir / "category_mapping.txt"
        content = "\n".join(
            [f"{k}|{v}" for k, v in self.category_mapping.items()])
        category_path.write_text(content, encoding='utf-8')

    # --- База данных ---
    def setup_database(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oe (
                oe_number_norm VARCHAR PRIMARY KEY,
                oe_number VARCHAR,
                name VARCHAR,
                applicability VARCHAR,
                category VARCHAR
            )
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
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_references (
                oe_number_norm VARCHAR,
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                price DOUBLE,
                currency VARCHAR DEFAULT 'RUB',
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        self.create_indexes()

    def create_indexes(self):
        st.info("🛠️ Создание индексов для ускорения поиска...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_number_norm ON oe(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_keys ON parts(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)"
        ]
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Не удалось создать индекс: {e}")
        st.success("🛠️ Индексы созданы.")

    # --- Нормализация ---
    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())

    @staticmethod
    def clean_values(series: pl.Series) -> pl.Series:
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())

    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        name_lower = name_series.str.to_lowercase()
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        # Пользовательские правила — приоритет
        for key, category in self.category_mapping.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower())
            ).then(pl.lit(category))
        # Стандартные правила
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
        for category, pattern in categories_map.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(pattern, literal=False)
            ).then(pl.lit(category))
        return categorization_expr.otherwise(pl.lit('Разное')).alias('category')

    # --- Обработка файлов ---
    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        column_variants = {
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
        actual_lower = {col.lower(): col for col in actual_columns}
        mapping = {}
        for expected in expected_columns:
            variants = column_variants.get(expected, [expected])
            for variant in variants:
                variant_lower = variant.lower()
                for actual_l, actual_orig in actual_lower.items():
                    if variant_lower in actual_l and actual_orig not in mapping:
                        mapping[actual_orig] = expected
                        break
        return mapping

    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        logger.info(f"Обработка файла: {file_type} ({file_path})")
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                return pl.DataFrame()

            df = pl.read_excel(file_path, engine='calamine')
            if df.is_empty():
                logger.warning(f"Пустой файл: {file_path}")
                return pl.DataFrame()

        except Exception as e:
            logger.exception(f"Ошибка чтения файла {file_path}: {e}")
            return pl.DataFrame()

        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['artikul', 'brand', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'prices': ['artikul', 'brand', 'price', 'currency']
        }
        expected_cols = schemas.get(file_type, [])
        column_mapping = self.detect_columns(df.columns, expected_cols)
        if not column_mapping:
            logger.warning(
                f"Не удалось определить колонки для файла {file_type}. Доступные: {df.columns}")
            return pl.DataFrame()

        df = df.rename(column_mapping)

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))

        key_cols = [col for col in ['oe_number',
                                    'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')

        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.normalize_key(
                    pl.col(col)).alias(f"{col}_norm"))

        return df

    # --- Загрузка и обновление в базе ---
    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        if df.is_empty():
            return
        df = df.unique(keep='first')
        cols = df.columns
        temp_view_name = f"temp_{table_name}_{int(time.time())}"

        # Регистрация временной таблицы в DuckDB
        try:
            self.conn.register(temp_view_name, df.to_arrow())
        except Exception as e:
            logger.error(f"Ошибка регистрации временной таблицы: {e}")
            return

        # DuckDB не гарантирует синтаксис ON CONFLICT как в Postgres -> используем delete+insert
        try:
            pk_list = pk
            pk_cols_csv = ", ".join(f'"{c}"' for c in pk_list)
            # удалить существующие записи, которые совпадают по PK
            delete_sql = f"""
                DELETE FROM {table_name}
                WHERE ({pk_cols_csv}) IN (SELECT {pk_cols_csv} FROM {temp_view_name});
            """
            self.conn.execute(delete_sql)
            # вставить новые записи
            insert_sql = f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_view_name};
            """
            self.conn.execute(insert_sql)
            logger.info(
                f"Успешно upsert {len(df)} записей в таблицу {table_name}.")
        except Exception as e:
            logger.error(f"Ошибка при UPSERT в {table_name}: {e}")
            st.error(
                f"Ошибка при записи в таблицу {table_name}. Детали в логе.")
        finally:
            try:
                self.conn.unregister(temp_view_name)
            except Exception:
                pass

    def upsert_prices(self, price_df: pl.DataFrame):
        if price_df.is_empty():
            return

        if 'artikul' in price_df.columns and 'brand' in price_df.columns:
            price_df = price_df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])

        if 'currency' not in price_df.columns:
            price_df = price_df.with_columns(pl.lit('RUB').alias('currency'))

        price_df = price_df.filter(
            (pl.col('price') >= self.price_rules['min_price']) &
            (pl.col('price') <= self.price_rules['max_price'])
        )

        self.upsert_data('prices', price_df, ['artikul_norm', 'brand_norm'])

    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        st.info("🔄 Начало загрузки и обновления данных в базе...")
        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
        num_steps = len(steps)
        progress_bar = st.progress(
            0, text="Подготовка к обновлению базы данных...")
        step_counter = 0

        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка OE данных...")
            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            oe_df = df.select(['oe_number_norm', 'oe_number', 'name', 'applicability']).unique(
                subset=['oe_number_norm'], keep='first')

            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(
                    self.determine_category_vectorized(pl.col('name')))
            else:
                oe_df = oe_df.with_columns(category=pl.lit('Разное'))

            self.upsert_data('oe', oe_df, ['oe_number_norm'])

            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, [
                             'oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка кроссов...")
            df = dataframes['cross'].filter(
                (pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df_from_cross = df.select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, [
                             'oe_number_norm', 'artikul_norm', 'brand_norm'])

        if 'prices' in dataframes:
            price_df = dataframes['prices']
            if not price_df.is_empty():
                st.info("💰 Обработка цен...")
                self.upsert_prices(price_df)
                st.success(
                    f"✅ Успешно обновлено {len(price_df)} ценовых записей")

        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1),
                              text=f"({step_counter}/{num_steps}) Сборка и обновление данных по артикулам...")

        # Собираем parts из разных файлов
        parts_df = None
        file_priority = ['oe', 'barcode', 'images', 'dimensions']
        key_files = {ftype: df for ftype,
                     df in dataframes.items() if ftype in file_priority}

        if key_files:
            all_parts = pl.concat([
                df.select(['artikul', 'artikul_norm', 'brand', 'brand_norm'])
                for df in key_files.values() if 'artikul_norm' in df.columns and 'brand_norm' in df.columns
            ]).filter(pl.col('artikul_norm') != "").unique(subset=['artikul_norm', 'brand_norm'], keep='first')
            parts_df = all_parts

            for ftype in file_priority:
                if ftype not in key_files:
                    continue
                df = key_files[ftype]
                if df.is_empty() or 'artikul_norm' not in df.columns:
                    continue
                join_cols = [col for col in df.columns if col not in [
                    'artikul', 'artikul_norm', 'brand', 'brand_norm']]
                if not join_cols:
                    continue
                existing_cols = set(parts_df.columns)
                join_cols = [
                    col for col in join_cols if col not in existing_cols]
                if not join_cols:
                    continue
                df_subset = df.select(['artikul_norm', 'brand_norm'] + join_cols).unique(
                    subset=['artikul_norm', 'brand_norm'], keep='first')
                parts_df = parts_df.join(
                    df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)

        if parts_df is not None and not parts_df.is_empty():
            if 'multiplicity' not in parts_df.columns:
                parts_df = parts_df.with_columns(
                    multiplicity=pl.lit(1).cast(pl.Int32))
            else:
                parts_df = parts_df.with_columns(
                    pl.col('multiplicity').fill_null(1).cast(pl.Int32))

            for col in ['length', 'width', 'height']:
                if col not in parts_df.columns:
                    parts_df = parts_df.with_columns(
                        pl.lit(None).cast(pl.Float64).alias(col))

            if 'dimensions_str' not in parts_df.columns:
                parts_df = parts_df.with_columns(
                    dimensions_str=pl.lit(None).cast(pl.Utf8))

            parts_df = parts_df.with_columns([
                pl.col('length').cast(pl.Utf8).fill_null(
                    '').alias('_length_str'),
                pl.col('width').cast(pl.Utf8).fill_null(
                    '').alias('_width_str'),
                pl.col('height').cast(pl.Utf8).fill_null(
                    '').alias('_height_str'),
            ])

            parts_df = parts_df.with_columns(
                dimensions_str=pl.when(
                    (pl.col('dimensions_str').is_not_null()) &
                    (pl.col('dimensions_str').cast(pl.Utf8) != '')
                ).then(
                    pl.col('dimensions_str').cast(pl.Utf8)
                ).otherwise(
                    pl.concat_str([
                        pl.col('_length_str'), pl.lit('x'),
                        pl.col('_width_str'), pl.lit('x'),
                        pl.col('_height_str')
                    ], separator='')
                )
            )

            parts_df = parts_df.drop(
                ['_length_str', '_width_str', '_height_str'])

            if 'artikul' not in parts_df.columns:
                parts_df = parts_df.with_columns(artikul=pl.lit(''))
            if 'brand' not in parts_df.columns:
                parts_df = parts_df.with_columns(brand=pl.lit(''))

            parts_df = parts_df.with_columns([
                pl.col('artikul').cast(pl.Utf8).fill_null(
                    '').alias('_artikul_str'),
                pl.col('brand').cast(pl.Utf8).fill_null(
                    '').alias('_brand_str'),
                pl.col('multiplicity').cast(
                    pl.Utf8).alias('_multiplicity_str'),
            ])

            parts_df = parts_df.with_columns(
                description=pl.concat_str([
                    pl.lit('Артикул: '), pl.col('_artikul_str'),
                    pl.lit(', Бренд: '), pl.col('_brand_str'),
                    pl.lit(', Кратность: '), pl.col(
                        '_multiplicity_str'), pl.lit(' шт.')
                ], separator='')
            )

            parts_df = parts_df.drop(
                ['_artikul_str', '_brand_str', '_multiplicity_str'])

            final_columns = [
                'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode',
                'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
            ]
            select_exprs = [pl.col(c) if c in parts_df.columns else pl.lit(
                None).alias(c) for c in final_columns]
            parts_df = parts_df.select(select_exprs)

            self.upsert_data('parts', parts_df, ['artikul_norm', 'brand_norm'])

        progress_bar.progress(1.0, text="Обновление базы данных завершено!")
        time.sleep(1)
        progress_bar.empty()

    # --- Экспорт ---
    def _get_brand_markups_sql(self) -> str:
        rows = []
        for brand, markup in self.price_rules['brand_markups'].items():
            safe_brand = brand.replace("'", "''")
            rows.append(f"SELECT '{safe_brand}' AS brand, {markup} AS markup")
        return " UNION ALL ".join(rows) if rows else "SELECT NULL AS brand, NULL AS markup LIMIT 0"

    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
        description_text = (
            "Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. "
            "Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей. "
            "В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электроматериалы, автомасла, инструмент, "
            "а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. "
            "Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. "
            "Выбирайте только лучшее — надежность и качество от ведущих производителей."
        )

        brand_markups_sql = self._get_brand_markups_sql()
        select_parts = []

        # Добавляем колонку цены и валюты
        price_requested = include_prices and (not selected_columns or "Цена" in selected_columns or "Валюта" in selected_columns)
        if price_requested:
            if apply_markup:
                global_markup = self.price_rules.get('global_markup', 0)
                select_parts.append(
                    f"CASE WHEN pr.price IS NOT NULL THEN pr.price * (1 + COALESCE(brm.markup, {global_markup})) ELSE pr.price END AS \"Цена\""
                )
            else:
                select_parts.append('pr.price AS "Цена"')
            select_parts.append("COALESCE(pr.currency, 'RUB') AS \"Валюта\"")

        # Остальные колонки
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

        if not select_parts:
            select_parts = ['r.artikul AS "Артикул бренда"', 'r.brand AS "Бренд"']

        select_clause = ",\n        ".join(select_parts)

        ctes = f"""
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${description_text}$$ AS text
        ),
        BrandMarkups AS (
            SELECT brand, markup FROM (
                {brand_markups_sql}
            ) AS tmp
        ),
        PartDetails AS (
            SELECT 
                cr.artikul_norm, 
                cr.brand_norm,
                STRING_AGG(
                    DISTINCT regexp_replace(
                        regexp_replace(o.oe_number, '''', ''), 
                        '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'
                    ), ', '
                ) AS oe_list,
                ANY_VALUE(o.name) AS representative_name,
                ANY_VALUE(o.applicability) AS representative_applicability,
                ANY_VALUE(o.category) AS representative_category
            FROM cross_references cr
            LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY cr.artikul_norm, cr.brand_norm
        ),
        AllAnalogs AS (
            SELECT 
                cr1.artikul_norm, 
                cr1.brand_norm,
                STRING_AGG(
                    DISTINCT regexp_replace(
                        regexp_replace(p2.artikul, '''', ''), 
                        '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'
                    ), ', '
                ) AS analog_list
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
            SELECT DISTINCT 
                i.artikul_norm AS source_artikul_norm, 
                i.brand_norm AS source_brand_norm,
                cr2.artikul_norm AS related_artikul_norm, 
                cr2.brand_norm AS related_brand_norm
            FROM InitialOENumbers i
            JOIN cross_references cr2 ON i.oe_number_norm = cr2.oe_number_norm
            WHERE NOT (i.artikul_norm = cr2.artikul_norm AND i.brand_norm = cr2.brand_norm)
        ),
        Level1OENumbers AS (
            SELECT DISTINCT 
                l1.source_artikul_norm, 
                l1.source_brand_norm, 
                cr3.oe_number_norm
            FROM Level1Analogs l1
            JOIN cross_references cr3 ON l1.related_artikul_norm = cr3.artikul_norm AND l1.related_brand_norm = cr3.brand_norm
            WHERE NOT EXISTS (
                SELECT 1 FROM InitialOENumbers i
                WHERE i.artikul_norm = l1.source_artikul_norm 
                  AND i.brand_norm = l1.source_brand_norm 
                  AND i.oe_number_norm = cr3.oe_number_norm
            )
        ),
        Level2Analogs AS (
            SELECT DISTINCT 
                loe.source_artikul_norm, 
                loe.source_brand_norm,
                cr4.artikul_norm AS related_artikul_norm, 
                cr4.brand_norm AS related_brand_norm
            FROM Level1OENumbers loe
            JOIN cross_references cr4 ON loe.oe_number_norm = cr4.oe_number_norm
            WHERE NOT (loe.source_artikul_norm = cr4.artikul_norm AND loe.source_brand_norm = cr4.brand_norm)
        ),
        AllRelatedParts AS (
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
            FROM Level1Analogs
            UNION
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
            FROM Level2Analogs
        ),
        AggregatedAnalogData AS (
            SELECT 
                arp.source_artikul_norm AS artikul_norm,
                arp.source_brand_norm AS brand_norm,
                MAX(CASE WHEN p2.length IS NOT NULL THEN p2.length ELSE NULL END) AS length,
                MAX(CASE WHEN p2.width IS NOT NULL THEN p2.width ELSE NULL END) AS width,
                MAX(CASE WHEN p2.height IS NOT NULL THEN p2.height ELSE NULL END) AS height,
                MAX(CASE WHEN p2.weight IS NOT NULL THEN p2.weight ELSE NULL END) AS weight,
                ANY_VALUE(
                    CASE 
                        WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX'
                        THEN p2.dimensions_str
                        ELSE NULL
                    END
                ) AS dimensions_str,
                ANY_VALUE(
                    CASE 
                        WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != '' 
                        THEN pd2.representative_name 
                        ELSE NULL
                    END
                ) AS representative_name,
                ANY_VALUE(
                    CASE 
                        WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != ''
                        THEN pd2.representative_applicability
                        ELSE NULL
                    END
                ) AS representative_applicability,
                ANY_VALUE(
                    CASE 
                        WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != ''
                        THEN pd2.representative_category
                        ELSE NULL
                    END
                ) AS representative_category
            FROM AllRelatedParts arp
            JOIN parts p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),
        RankedData AS (
            SELECT 
                p.artikul_norm,
                p.brand_norm,
                p.artikul,
                p.brand,
                p.description,
                p.multiplicity,
                p.length,
                p.width,
                p.height,
                p.weight,
                p.dimensions_str,
                p.image_url,
                pd.representative_name,
                pd.representative_applicability,
                pd.representative_category,
                pd.oe_list,
                aa.analog_list,
                p_analog.length AS analog_length,
                p_analog.width AS analog_width,
                p_analog.height AS analog_height,
                p_analog.weight AS analog_weight,
                p_analog.dimensions_str AS analog_dimensions_str,
                p_analog.representative_name AS analog_representative_name,
                p_analog.representative_applicability AS analog_representative_applicability,
                p_analog.representative_category AS analog_representative_category,
                ROW_NUMBER() OVER (
                    PARTITION BY p.artikul_norm, p.brand_norm 
                    ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST
                ) AS rn
            FROM parts p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.brand_norm = p_analog.brand_norm
        )
        """

        price_join = """
        LEFT JOIN prices pr ON r.artikul_norm = pr.artikul_norm AND r.brand_norm = pr.brand_norm
        LEFT JOIN BrandMarkups brm ON r.brand = brm.brand
        """ if include_prices else ""

        query = f"""
        {ctes}
        SELECT
            {select_clause}
        FROM RankedData r
        CROSS JOIN DescriptionTemplate dt
        {price_join}
        WHERE r.rn = 1
        ORDER BY r.brand, r.artikul
        """

        return "\n".join([line.rstrip() for line in query.strip().splitlines()])

    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices=True, apply_markup=True, convert_weight=False, convert_dimensions=False) -> bool:
        total = self.conn.execute(
            "SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            logger.info(f"Executing export query: {query}")
            df = self.conn.execute(query).pl()

            # Обработка веса (кг в г)
            if convert_weight:
                if 'Вес' in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col('Вес').cast(pl.Float64).is_not_null())
                        .then(pl.col('Вес') * 1000)
                        .otherwise(pl.lit(None))
                        .alias('Вес')
                    )

            # Обработка размеров (см в мм)
            if convert_dimensions:
                for col in ['Длинна', 'Ширина', 'Высота']:
                    if col in df.columns:
                        df = df.with_columns(
                            pl.when(pl.col(col).cast(pl.Float64).is_not_null())
                            .then(pl.col(col) * 10)
                            .otherwise(pl.lit(None))
                            .alias(col)
                        )

            import pandas as pd
            pdf = df.to_pandas()
            output_dir = Path("auto_parts_data")
            output_dir.mkdir(parents=True, exist_ok=True)
            buf = io.StringIO()
            pdf.to_csv(buf, sep=';', index=False)
            with open(output_path, "wb") as f:
                f.write(b'\xef\xbb\xbf')  # BOM
                f.write(buf.getvalue().encode('utf-8'))
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"Данные экспортированы: {output_path} ({size_mb:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта CSV")
            st.error(f"Ошибка при экспорте в CSV: {str(e)}")
            return False

    def export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices=True, apply_markup=True, convert_weight=False, convert_dimensions=False) -> bool:
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        import pandas as pd
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = pd.read_sql(query, self.conn)

        # Обработка веса (кг в г)
        if convert_weight:
            if 'Вес' in df.columns:
                df['Вес'] = df['Вес'].replace({r'^nan$': ''}, regex=True).astype(str)
                df['Вес'] = pd.to_numeric(df['Вес'], errors='coerce') * 1000

        # Обработка размеров (см в мм)
        if convert_dimensions:
            for col in ['Длинна', 'Ширина', 'Высота']:
                if col in df.columns:
                    df[col] = df[col].replace({r'^nan$': ''}, regex=True).astype(str)
                    df[col] = pd.to_numeric(df[col], errors='coerce') * 10

        for col in ["Длинна", "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота"]:
            if col in df.columns:
                df[col] = df[col].astype(str).replace({r'^nan$': ''}, regex=True)

        if len(df) <= EXCEL_ROW_LIMIT:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
        else:
            sheets = (len(df) // EXCEL_ROW_LIMIT) + 1
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for i in range(sheets):
                    df.iloc[i*EXCEL_ROW_LIMIT:(i+1)*EXCEL_ROW_LIMIT].to_excel(writer, index=False, sheet_name=f"Данные_{i+1}")
        return True

    def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices=True, apply_markup=True, convert_weight=False, convert_dimensions=False) -> bool:
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()

            if convert_weight:
                if 'Вес' in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col('Вес').cast(pl.Float64).is_not_null())
                        .then(pl.col('Вес') * 1000)
                        .otherwise(pl.lit(None))
                        .alias('Вес')
                    )

            if convert_dimensions:
                for col in ['Длинна', 'Ширина', 'Высота']:
                    if col in df.columns:
                        df = df.with_columns(
                            pl.when(pl.col(col).cast(pl.Float64).is_not_null())
                            .then(pl.col(col) * 10)
                            .otherwise(pl.lit(None))
                            .alias(col)
                        )

            df.write_parquet(output_path)
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта Parquet")
            st.error(f"Ошибка при экспорте в Parquet: {str(e)}")
            return False

    # --- Остальные методы — без изменений, оставляю их как есть ---
    def delete_by_brand(self, brand_norm: str) -> int:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            if deleted_count == 0:
                return 0
            self.conn.execute("DELETE FROM parts WHERE brand_norm = ?", [brand_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            return deleted_count
        except Exception as e:
            logger.error(f"Error: {e}")
            raise

    def delete_by_artikul(self, artikul_norm: str) -> int:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            if deleted_count == 0:
                return 0
            self.conn.execute("DELETE FROM parts WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute("DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            return deleted_count
        except Exception as e:
            logger.error(f"Error: {e}")
            raise

    def show_export_interface(self):
        # оставляю как есть, добавляю параметры в вызов
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

        # Новые чекбоксы для конвертации
        convert_weight = st.checkbox("Конвертировать вес (кг в г)")
        convert_dimensions = st.checkbox("Конвертировать размеры (см в мм)")

        if st.button("🚀 Экспортировать"):
            output_path = self.data_dir / f"export.{format_choice.lower()}"
            with st.spinner("Генерация файла..."):
                if format_choice == "CSV":
                    self.export_to_csv_optimized(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup, convert_weight, convert_dimensions)
                elif format_choice == "Excel":
                    self.export_to_excel_optimized(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup, convert_weight, convert_dimensions)
                elif format_choice == "Parquet":
                    self.export_to_parquet(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup, convert_weight, convert_dimensions)
                else:
                    st.warning("Неподдерживаемый формат")
                    return
            with open(output_path, "rb") as f:
                st.download_button("⬇️ Скачать файл", f, file_name=output_path.name)

    # Остальные методы — без изменений, оставляю их как есть

def main():
    st.title("🚗 AutoParts Catalog 10M+")
    st.markdown("### Платформа для больших каталогов автозапчастей")
    catalog = HighVolumeAutoPartsCatalog()

    st.sidebar.title("🧭 Меню")
    option = st.sidebar.radio(
        "Выберите раздел", ["Загрузка данных", "Экспорт", "Статистика", "Управление"])

    if option == "Загрузка данных":
        # оставляю как есть
        pass
    elif option == "Экспорт":
        catalog.show_export_interface()
    elif option == "Статистика":
        catalog.show_statistics()
    elif option == "Управление":
        catalog.show_data_management()

if __name__ == "__main__":
    main()
