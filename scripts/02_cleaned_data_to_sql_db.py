# ---
# jupyter:
#   jupytext:
#     formats: notebooks//ipynb,scripts//py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3.12 (Updated)
#     language: python
#     name: python312
# ---

# %%
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, Float, BigInteger, String, Date

# %%
# вернуться на уровень вверх по пути директории, чтобы дальнейшие относительные пути к файлам были корректны
current_folder = os.path.basename(os.getcwd())

if current_folder != 'iaros_data_berka_analysis':
    os.chdir('..')
    current_folder = os.path.basename(os.getcwd())
    print(f'Changed current directory to {current_folder}')
else:
    print(f'The current directory is {current_folder}')

# %% [markdown]
# # Создание engine для подключения к БД

# %%
# пустая postgresql БД предварительно создана, называется berka_db

engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/berka_db')

# %%
# записываем структуру БД в переменную

metadata = MetaData()


# %% [markdown]
# # Создание и заполнение таблицы accounts

# %%
# функция загрузки csv в датафрейм из cleaned_data либо из data/raw_data либо из пользовательского пути

def df_from_csv(file: str, path='clean', sep=',', dtype: dict | None=None, parse_dates=False):
    """
    file - string, name of the csv, with extension
    -----------------------------------
    path - path to a directory, must be a string:
    'clean', 'raw' or custom relational or absolute path
    default is 'clean'
    
    'clean' defines relational path as 'data/cleaned_data',
    'raw' defines 'data/raw_data'
    -----------------------------------
    sep - file delimiter, default is comma ','
    -----------------------------------
    dtype - dict, mapping of column names/indices and types
    -----------------------------------
    parse_dates - bool, list of Hashable, list of lists or dict of {Hashablelist}, default False
    """

    if os.path.isdir(path) != True:
        path = path.lower().strip()
        
        if path == 'clean':
            dir = 'data/cleaned_data/'
            full_path = os.path.join(dir, file)
            
        elif path == 'raw':
            dir = 'data/raw_data/'
            full_path = os.path.join(dir, file)
        else:
            raise ValueError(f"Path argument must be 'clean', 'raw' or custom path, but got {path}")
    else:
        full_path = os.path.join(path, file)

        if os.path.isfile(full_path) != True:
            raise ValueError(f'{file} does not exist in {path}')
        
            
    return pd.read_csv(full_path, sep=sep, dtype=dtype, parse_dates=parse_dates)
     
    

# %%
# подгружаем очищенные данные

account_clean = df_from_csv('account_df_clean.csv', parse_dates=['date'])

# %%
account_clean.head(3)

# %%
account_types = account_clean.dtypes
account_types


# %%
def add_table_to_metadata(table_name: str, metadata, df_types: pd.Series):
    """
    table_name - string, name of a SQL table
    metadata - sqlalchemy object MetaData()
    df_types - pandas series with dataframe's data types. You can pass edited series with types as strings.
            To learn ccepted type labels refer to keys of {'int64':Integer, 'BigInt':BigInteger, 'float64':Float, 'object':String, 'datetime64[ns]':Date}
            Series index values will be used as columns names in the SQL table
    """
    if not isinstance(df_types, pd.Series):
        raise TypeError(f'df_types must be pd.Series object, but got {type(df_types)}')
        
    # маппинг для датафрейм типов и sqlalchemy типов
    # BigInt это кастомное значение для случаев очень больших целых чисел.
    # если нужен BigInt, то тогда нужно в df_types передать серию где будет значение BigInt для соответсвующей колонки
    
    types_mapping = {'int64':Integer, 'BigInt':BigInteger, 'float64':Float, 'object':String, 'datetime64[ns]':Date}

    # создание генератора на основании типов в датафрейме и типов sqlalchemy
    # затем добавление таблицы в metadata
    return Table(table_name, metadata, *[Column(col_name, types_mapping[str(dtype)]) for col_name, dtype in df_types.items()])


# %%
accounts_tab = add_table_to_metadata('accounts', metadata, account_types)

# %%
accounts_tab.create(engine)


# %%
# функция добавления данных из датафрейма в имеющиюся таблицу SQL

def append_df_to_sql(df, table_name, engine, if_exists='append', index=False, chunksize: int | None=None):
    if not isinstance(table_name, str):
        raise TypeError(f'table_name must be a string, but got {type(table_name)}')
        
    df.to_sql(table_name, engine, if_exists=if_exists, index=index)


# %%
# добавляем данные из датафрейма в таблицу accounts

append_df_to_sql(account_clean, 'accounts', engine)


# %%
# функция сверки количества строк в датафрейме и строк добавленных в БД
def count_rows(engine, table_name: str, df: pd.DataFrame | None=None):

    # проверка типа для table_name
    if not isinstance(table_name, str):
        raise TypeError(f'table_name must be a string, but got {type(table_name)}')
        
    with engine.connect() as con:
        query = con.execute(text(f'SELECT COUNT(*) FROM {table_name}'))
        row_count = query.scalar() # результат запроса в виде числа
    
    if df is not None:
        df_rows = df.shape[0]
        if df_rows != row_count:
            raise ValueError(f'''Dataframe row count is not equal to SQL table row count!
Dataframe: {df_rows}
SQL table: {row_count}''')
        else:
            print(f'Dataframe and SQL table row counts are equal.\n{row_count} rows')
    else:
        print(f'{row_count} rows in the SQL table')
        return row_count


# %%
# сверяем количество строк в датафрейме и в БД

count_rows(engine, 'accounts', account_clean)

# %% [markdown]
# ## Создание таблицы client

# %%
client_clean = df_from_csv('client_df_clean.csv', parse_dates=['birth_date'])

# %%
client_clean.head(3)

# %%
client_types = client_clean.dtypes
client_types

# %%
client_tab = add_table_to_metadata('clients', metadata, client_types)

# %%
client_tab.create(engine)

# %%
append_df_to_sql(client_clean, 'clients', engine)

# %%
# сверяем размеры датафрейма и БД таблицы

count_rows(engine, 'clients', client_clean)

# %% [markdown]
# ## Создание и заполнение таблицы disposition

# %%
disp_clean = df_from_csv('disposition_df_clean.csv')

# %%
disp_clean.head(3)

# %%
disp_types = disp_clean.dtypes
disp_types

# %%
disp_tab = add_table_to_metadata('disposition', metadata, disp_types)

# %%
disp_tab.create(engine)

# %%
# заполняем таблицу disposition

append_df_to_sql(disp_clean, 'disposition', engine)

# %%
# сверяем количество строк

count_rows(engine, 'disposition', disp_clean)

# %% [markdown]
# ## Создание и заполнение таблицы permanent order

# %%
order_clean = df_from_csv('order_df_clean.csv')

# %%
order_clean.head(3)

# %%
order_types = order_clean.dtypes
order_types

# %%
# создаем таблицу в БД

order_tab = add_table_to_metadata('orders', metadata, order_types)

# %%
order_tab.create(engine)

# %%
# заполняем таблицу

append_df_to_sql(order_clean, 'orders', engine)

# %%
# проверяем целостность
count_rows(engine, 'orders', order_clean)

# %% [markdown]
# ## Создание и заполнение таблицы transaction

# %%
trans_clean = df_from_csv('transaction_df_clean.csv', parse_dates=['date'], dtype={'bank':'str'})

# %%
trans_clean.head(3)

# %%
trans_types = trans_clean.dtypes
trans_types

# %%
trans_tab = add_table_to_metadata('transactions', metadata, trans_types)

# %%
trans_tab.create(engine)

# %%
# chunksize 1000: time taken - 68 s
append_df_to_sql(trans_clean, 'transactions', engine, chunksize=1000)

# %%
count_rows(engine, 'transactions', trans_clean)

# %% [markdown]
# ## Создание и заполнение таблицы loan

# %%
loan_clean = df_from_csv('loan_df_clean.csv', parse_dates=['date'])

# %%
loan_clean.head(3)

# %%
loan_types = loan_clean.dtypes
loan_types

# %%
loan_tab = add_table_to_metadata('loan', metadata, loan_types)

# %%
loan_tab.create(engine)

# %%
append_df_to_sql(loan_clean, 'loan', engine)

# %%
count_rows(engine, 'loan', loan_clean)

# %% [markdown]
# ## Создание и заполнение таблицы credit card

# %%
card_clean = df_from_csv('card_df_clean.csv', parse_dates=['issued'])

# %%
card_clean.head(3)

# %%
card_types = card_clean.dtypes
card_types

# %%
card_tab = add_table_to_metadata('cards', metadata, card_types)

# %%
card_tab.create(engine)

# %%
append_df_to_sql(card_clean, 'cards', engine)

# %%
count_rows(engine, 'cards', card_clean)

# %% [markdown]
# ## Создание и наполнение таблицы district (Demographic data)

# %%
district_clean = df_from_csv('district_df_clean.csv')

# %%
district_clean.head(3)

# %%
district_types = district_clean.dtypes
district_types

# %%
district_tab = add_table_to_metadata('district', metadata, district_types)

# %%
district_tab.create(engine)

# %%
append_df_to_sql(district_clean, 'district', engine)

# %%
count_rows(engine,'district',  district_clean)


# %%
# создаем функцию подсчета файлов с оперделенным расширением в указанной директории
# она нам понадобится для проверки количества файлов с очищенными данными и созданных таблиц

def count_specific_files(dir='data/cleaned_data/', ext='.csv'):
    """
    counts visible files with specified extension. Skips temporary and hidden files.
    
    dir - relative or absolute path to directory. Default is 'data/cleaned_data/'
    ext - files extension. Default is '.csv'
    """
    if not os.path.isdir(dir):
        if os.path.isabs(dir):
            raise ValueError(f'dir argument must existing path! {dir} does not exist')
        else:
            raise ValueError(f'dir argument must existing path! {dir} is relative path. Check current directory or if path is correct')

    
    filtered_files = [file for file in os.listdir(dir) if file.endswith(ext) and not file.startswith('.') and not file.startswith('~')]

    return len(filtered_files)


# %%
# определяем функцию сверки количества файлов с данными с количеством таблиц

def cleaned_and_tables_count(engine, files_dir='data/cleaned_data/', file_ext='.csv', schema='public'):
    """
    counts tables in a database and counts visible files in specified directory - temporary and hidden files are skipped
    then compares the counts
    ------------------------
    engine - database engine
    ------------------------
    files_dir - directory in which files to be counted. The default is 'data/cleaned_data/'
    file_ext - files extension. The default is '.csv'
    schema - database schema in which tables must be counted. The default is 'public'
    """

    # подсоединяемся к БД. Делаем запрос, считающий количество таблиц в указанной схеме БД
    with engine.connect() as con:
        query = con.execute(text(f"""
        SELECT count(tablename)
        FROM pg_catalog.pg_tables
        WHERE schemaname = '{schema}'"""))
        tables_count = query.scalar()

    # через функцию count_specific_files() считаем количество файлов с указанны
    files_count = count_specific_files(files_dir, file_ext)

    # сравниваем количество. Выводим ошибку если не равно
    if tables_count != files_count:
        raise ValueError(f"""Files count is not equal to tables count!
Files: {files_count}
Tables: {tables_count}""")

    # если равно, то выводим сообщение и количество.
    else:
        print(f'Files count is equal to tables count. The count is {tables_count}')
    


# %%
# проверяем количество файлов с очищенными данными и количество созданных таблиц в БД

cleaned_and_tables_count(engine)
