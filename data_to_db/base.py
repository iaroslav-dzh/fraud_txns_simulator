from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, Float, \
                       BigInteger, String, Date, Boolean, DateTime


# 1.

def add_table_to_metadata(table_name: str, metadata, df_types: pd.Series):
    """
    Добавление объекта sqlalchemy.Table в объект sqlalchemy.MetaData.
    ------------------------
    table_name: str. Название SQL таблицы.
    metadata: sqlalchemy.MetaData.
    df_types: pd.Series. Типы данных в датафрейме, предназначенном для выгрузки в таблицу.
              Можно передать измененную серию с типами в виде строк. Значения в серии 
              будут маппиться со значениями типов sqlalchemy.
              Принимаемые лейблы типов можно узнать из ключей словаря:
              {'int64':Integer, 'BigInt': BigInteger, 'float64': Float, 'object': String,
              'datetime64[ns]': Date}
            Значения индекса серии будут использованы как навзания колонок в SQL таблице.
    """
    if not isinstance(df_types, pd.Series):
        raise TypeError(f'df_types must be pd.Series object, but got {type(df_types)}')
        
    # маппинг для pandas типов и sqlalchemy типов
    # BigInt это кастомное значение для случаев очень больших целых чисел.
    # если нужен BigInt, то тогда нужно в df_types передать серию где будет значение BigInt для соответсвующей колонки
    
    types_mapping = {'int64':Integer, 'BigInt':BigInteger, 'float64':Float, 'object':String, 'datetime64[ns]':DateTime, \
                     'date':Date, 'bool':Boolean}

    # создание генератора на основании типов в датафрейме и типов sqlalchemy
    # затем добавление таблицы в metadata
    return Table(table_name, metadata, *[Column(col_name, types_mapping[str(dtype)]) \
                                         for col_name, dtype in df_types.items()])


# 2.
# функция добавления данных из датафрейма в имеющиюся таблицу SQL

def append_df_to_sql(df, table_name, engine, if_exists='append', index=False, \
                     chunksize: int | None=None):
    """
    df: pd.DataFrame. Данные для загрузки.
    table_name: str. Название таблицы в БД.
    engine: sqlalchemy.engine.base.Engine.
    if_exists: str. Аргумент pd.DataFrame.to_sql(). По умолчанию 'append'.
    index: bool. Аргумент pd.DataFrame.to_sql(). По умолчанию False.
    """
    if not isinstance(table_name, str):
        raise TypeError(f'table_name must be a string, but got {type(table_name)}')
        
    df.to_sql(table_name, engine, if_exists=if_exists, index=index, \
              chunksize=chunksize)


# 3.
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