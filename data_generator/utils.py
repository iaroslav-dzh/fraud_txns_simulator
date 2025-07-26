# Модуль с общими вспомогательными функциями
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from scipy.stats import truncnorm
from pyproj import Geod
from tqdm import tqdm
import yaml

# 1. 
# функция генерации случайных точек в указанной зоне
# это не сам полигон города, а четырехугольник из крайних точек полигона города

def Random_Points_in_Bounds(polygon, number):   
    minx, miny, maxx, maxy = polygon.bounds
    x = np.random.uniform( minx, maxx, number )
    y = np.random.uniform( miny, maxy, number )
    return x, y

#.

def gen_trans_coordinates(polygon, number):
    """
    Функция генерации координат внутри города(полигона)
    """
    x,y = Random_Points_in_Bounds(polygon, number)
    df = pd.DataFrame()
    df['points'] = list(zip(x,y))
    df['points'] = df['points'].apply(Point)
    gdf_points = gpd.GeoDataFrame(df, geometry='points')

    gdf_poly = gpd.GeoDataFrame(index=["myPoly"], geometry=[polygon])
    
    Sjoin = gpd.tools.sjoin(gdf_points, gdf_poly, predicate="within", how='left')

    # Оставить точки внутри "myPoly"
    pnts_in_poly = gdf_points[Sjoin.index_right=='myPoly']

    return pnts_in_poly, gdf_poly

# .

def build_transaction(client_id, txn_time, txn_unix, amount, txn_type, channel, category_name, online, merchant_id, \
                      trans_city, trans_lat, trans_lon, trans_ip, device_id, account, is_fraud, is_suspicious, \
                      status, rule):
    """
    Собирает словарь с данными транзакции
    """

    txn_dict = {
                "client_id": client_id, "txn_time": txn_time, "unix_time":txn_unix, "amount": amount, "type": txn_type,
                "channel": channel, "category": category_name, "online": online, "merchant_id": merchant_id,
                "trans_city":trans_city, "trans_lat": trans_lat, "trans_lon": trans_lon, "trans_ip":trans_ip,
                "device_id": device_id, "account": account, "is_fraud": is_fraud, "is_suspicious":is_suspicious, "status":status,
                "rule":rule
                }

    return txn_dict


# 2. 

def gen_trans_number_norm(avg_num, num_std, low_bound=1, up_bound=120):
    """
    Возвращает целое число по нормальному распределению.
    Число ограничено выставленными лимитами.
    ----------------------------------------
    avg_num - среднее число транзакций
    num_std - стандартное отклонение числа транзакций
    low_bound - минимальное возможное число транзакций
    up_bound - максимальное возможное число транзакций
    """
    
    # Вернет float в виде np.ndarray
    random_float = truncnorm.rvs(a=(low_bound - avg_num) / num_std, b=(up_bound - avg_num) / num_std, \
                                 loc=avg_num, scale=num_std, size=1)

    # Преобразуем float в int и извлекаем из массива
    return random_float.astype(int)[0]


# 3. 

def get_values_from_truncnorm(low_bound, high_bound, mean, std, size=1):
    """
    Сгенерировать массив чисел из обрезанного нормального распределения.
    Можно сгенерировать массив с одним числом
    ------------
    low_bound - float, int. Нижняя граница значений
    high_bound - float, int. Верхняя граница значений 
    mean - float, int. Среднее
    std - float, int. Стандартное отклонение
    size - Количество чисел в возвращаемом массиве
    ------------
    Возвращает np.ndarray
    """
    return truncnorm.rvs((low_bound - mean) / std, (high_bound - mean) / std, loc=mean, scale=std, size=size)

# 4.

def calc_distance(lat_01, lon_01, lat_02, lon_02, km=True):
    """
    Считает растояние между двумя координатами на Земном шаре.
    Между координатами последней по времени транзакции и переданными координатами.
    -----------------------------
    lat_01 - float. Широта первой точки
    lon_01 - float. Долгота первой точки
    lat_02 - float. Широта второй точки
    lon_02 - float. Долгота второй точки
    km - bool. Единицы измерения. Либо километры либо метры. Километры округляет до 2-х знаков, метры до целого.
    """
    
    # Геодезический расчёт по эллипсоиду WGS84
    geod = Geod(ellps="WGS84")
    # [-1] берет последний элемент из кортежа. Это метры
    distance_m = geod.inv(lon_01, lat_01, lon_02, lat_02)[-1]

    if km:
        return round(distance_m / 1000, 2)

    return round(distance_m)


# . Функция sample_category. На данный момент предназначена только для фрода

def sample_category(categories, online=None, is_fraud=None, rule=None):
    """
    На данный момент предназначена только для фрода
    -----------------------------------
    categories - pd.DataFrame с категориями и их характеристиками
    online - bool. Онлайн или оффлайн категория нужна
    is_fraud - bool. Фрод или не фрод. От этого зависит вероятность категории.
    """

    if is_fraud and online and rule != "trans_freq_increase":
        online_categories = categories.loc[categories.online == True]
        cat_sample = online_categories.sample(1, weights=online_categories.fraud_share)
        return cat_sample

    elif is_fraud and online and rule == "trans_freq_increase":
        chosen_categories = categories.loc[categories.category.isin(["shopping_net", "misc_net"])]
        cat_sample = chosen_categories.sample(1, weights=chosen_categories.fraud_share)
        return cat_sample

        
    elif is_fraud and not online:
        offline_categories = categories.loc[categories.online == False]
        cat_sample = offline_categories.sample(1, weights=offline_categories.fraud_share)
        return cat_sample


# Функция семплирования антифрод-правила
def sample_rule(rules):
    """
    rules - pd.DataFrame с названиями правил и их весами
    """
    return rules.rule.sample(1, weights=rules.weight).iat[0]


# Функция создания пустого датафрейма под транзакции
def create_txns_df(configs):
    """
    configs: dict. Словарь с парами 'название колонки':'тип данных'
    """
    to_df = {}
    for key in configs.keys():
        data_type = configs[key]
        to_df[key] = pd.Series(dtype=data_type)

    return pd.DataFrame(to_df)


# Создать прогрессбар
def create_progress_bar(obj, text=None):
    """
    Создать tqdm прогресс бар.
    Импортировать tqdm из tqdm
    -------------
    obj: Итерируемый объект.
    text: str. Текст для прогрессбара.
    """
    total = len(obj)
    return tqdm(total=total, desc=text)


# Случайное округление суммы
def amt_rounding(amount, rate=0.6):
    """
    Целочисленное округление.
    До единиц, десятков, сотен, тысяч.
    Либо возвращает исходную сумму.
    -------------
    amount: float | int.
    rate: float. Доля случаев когда сумма не округялется.
    """
    if np.random.uniform(0, 1) < rate:
        return amount
    
    dividers = np.array([1, 10, 100, 1000])
    reduced_divs = dividers[dividers <= amount]
    divider = np.random.choice(reduced_divs)
    return amount // divider * divider


def load_configs(path, encoding="utf8"):
    """
    Загрузить конфиги из yaml файла.
    --------
    path: str. Путь к файлу.
    encoding: str. Кодировка для считывания файла через open().
    """
    with open(path, encoding=encoding) as f:
        return yaml.safe_load(f)




        








