# **Генерация транзакций в рамках compromised client фрода**  
- **ноутбуки лучше просматривать** на nbviewer.org, т.к. там нет проблем с отображением, которые могут быть на Github:
    -  **[Ссылка](https://nbviewer.org/github/iaroslav-dzh/fraud_txns_simulator/blob/main/notebooks_clean/09_%D0%93%D0%B5%D0%BD%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D1%8F_compromised_clients_%D1%84%D1%80%D0%BE%D0%B4%D0%B0.ipynb)** на этот ноутбук
    -  **[Ссылка](https://nbviewer.org/github/iaroslav-dzh/fraud_txns_simulator/tree/main/notebooks_clean/)** на папку со всеми ноутбуками  

**Что подразумевается под compromised client фродом**
- транзакции покупок клиентов, которые были скомпрометированы - условно говоря скомпрометированы их карты или аккаунты ДБО т.е. кто-то посторонний совершает онлайн или оффлайн покупки от лица клиентов
- транзакции генерируются, чтобы попадать по логике под какое-либо антифрод правило в рамках compromised фрода, напр.: резкая смена гео, покупка с другого ip и  устройства и т.п.

**Информация о ноутбуке**
- в этом ноутбуке демонстрация **основных** функций и классов относящихся к генерации compromised client фрода
- где-то будет просто объяснение основной логики функции/класса, где-то также демонстрация работы
- в compromised фроде будут только покупки, переводов пока не будет
- **в целом** то, что генерируется в рамках compromised фрода можно увидеть почти в конце ноутбука под заголовком **Функция генерации нескольких фрод транзакций `gen_multi_fraud_txns`**


```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import pyarrow
import yaml
from data_generator.general_time import *
from data_generator.utils import load_configs, build_transaction, create_txns_df
```


```python
np.set_printoptions(suppress=True)
pd.set_option('display.max_columns', None)
```


```python
os.chdir("..")
os.getcwd()
```




    'C:\\Users\\iaros\\My_documents\\Education\\projects\\fraud_detection_01'




```python
# Базовые конфиги
base_cfg = load_configs("./config/base.yaml")
# Настройки легальных транзакций
legit_cfg = load_configs("./config/legit.yaml")
# Общие настройки фрода
fraud_cfg = load_configs("./config/fraud.yaml")
# Настройки compromised client фрода
compr_cfg = load_configs("./config/compr.yaml")
# Настройки времени
time_cfg = load_configs("./config/time.yaml") 

# Пути к файлам
data_paths = base_cfg["data_paths"]
```

<br><br>

# **Создание конфиг класса с конфигами и данными для генерации**

<br>

------------------------------------------------------
## 1. Конструктор конфиг класса `ComprConfigBuilder`
- модуль `data_generator.fraud.compr.build.config`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/build/config.py)** на файл в Github 
- создает объект `CompPurchFraudCfg` - содержащий конфиги и данные для генерации транзакций
- Смысл такой же как у конфиг билдера для легальных транзакций:
    - Принимает на вход словари с данными из конфиг файлов для создания объекта
    - Метод `build_cfg()` создает объект конфиг класса compromised client fraud транзакций с данными и конфигами для генерации транзакций, например:
        - датафреймами с данными: timestamp-ы для семплирования времени, данные выбранных клиентов, мерчанты, фрод ip адреса, фрод девайсы; путями к директориям для записи файлов

**Путь к директории для файлов текущего запуска генерации** возьмем из прошлого ноутбука с легальными транзакциями т.к. у генератора в целом каскадный процесс генерации и генерация идет поэтапно: легальные, compromised фрод, дропы распределители, дропы покупатели. И каждый этап в разной степени зависит от предыдущего. В текущем случае для compromised фрода обязательно наличие созданных ранее легальных транзакций и семпла клиентов взятого под создание тех легальных транз-ций, т.к. от легальных транзакций берется время для расчета времени фрод транзакции, а от клиентов берется семпл под compromised фрод.


```python
run_dir = './data/generated/history/generation_run_2025-07-25_121029'
```


```python
# Создадим объект конфиг класса при помощи билдера. Будем использовать конфиг класс дальше
from data_generator.fraud.compr.build.config import ComprConfigBuilder

cfg_builder = ComprConfigBuilder(base_cfg=base_cfg, legit_cfg=legit_cfg, time_cfg=time_cfg, \
                                 fraud_cfg=fraud_cfg, compr_cfg=compr_cfg, run_dir=run_dir)
configs = cfg_builder.build_cfg()
```


```python
# Проверка и демонстрация конфиг класса. Клиенты семплированные под фрод
configs.clients.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>city_id</th>
      <th>birth_date</th>
      <th>sex</th>
      <th>region</th>
      <th>city</th>
      <th>timezone</th>
      <th>lat</th>
      <th>lon</th>
      <th>population</th>
      <th>home_ip</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1114</td>
      <td>59</td>
      <td>2005-06-12</td>
      <td>male</td>
      <td>Краснодарский</td>
      <td>Краснодар</td>
      <td>UTC+3</td>
      <td>45.040235</td>
      <td>38.976080</td>
      <td>744933</td>
      <td>2.60.4.36</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1125</td>
      <td>9</td>
      <td>1959-03-24</td>
      <td>female</td>
      <td>Кемеровская</td>
      <td>Новокузнецк</td>
      <td>UTC+7</td>
      <td>53.794276</td>
      <td>87.214405</td>
      <td>547885</td>
      <td>2.60.4.47</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Фрод ip адреса, которые будут использоваться для онлайн транзакций
configs.fraud_ips.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>city</th>
      <th>lat</th>
      <th>lon</th>
      <th>fraud_ip</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Москва</td>
      <td>55.753879</td>
      <td>37.620373</td>
      <td>5.3.252.223</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Санкт-Петербург</td>
      <td>59.939125</td>
      <td>30.315822</td>
      <td>5.3.252.224</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Фрод девайсы, которые будут использоваться для онлайн транзакций
configs.fraud_devices.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>device_id</th>
      <th>platform</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>9670</td>
      <td>Windows</td>
    </tr>
    <tr>
      <th>1</th>
      <td>9671</td>
      <td>Windows</td>
    </tr>
  </tbody>
</table>
</div>



<br><br>

----------------------------------
# **Генерация времени**
-----------------------------

<br>

## 1. Функция генерации времени `generate_time_fast_geo_jump`
- для фрод кейсов в рамках правил резкой смены гео клиентом: `fast_geo_change` и `fast_geo_change_online`
- модуль `data_generator.fraud.compr.time`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/time.py)** на файл в Github 

**Условия**
1. Подразумевается что у клиента не было фрода

**Основная логика функции**  
1. Принимает на вход:
    - время последней легальной транзакций
    - крачайшую дистанцию между координатами последней легальной транз-ции и координатами текущей транзакции. Подразумевается что координаты текущей транзакции генерируются до вызова это функции и также до вызова считается дистанция между координатами
    - дистанцию между координатами последней легальной транзакции и текущей транзакции
    - пороговое значение максимальной допустимой скорости перемещения клиента между совершением транзакций км/ч. Все что строго выше по скорости - фрод. Пороговое значение будет браться из конфига `compr.yaml`, вне функции.
2. На основании дистанции между координатами и порога по скорости задается случайная скорость перемещения выше указанного порога, чтобы попасть под фрод.
3. Дистанция делится на полученную скорость для получения разницы во времени между последней легальной и текущей фрод транзакциями
4. Полученная разница прибавляется ко времени последней легальной транзакции. Это и будет временем фрод транзакции

### **Демонстрация**


```python
from data_generator.fraud.compr.time import generate_time_fast_geo_jump
```

Создадим датафрейм с дистанциями, колонками под время в минутах и под время последней легальной транзакции. Будем проходить в цикле через этот датафрейм и генерировать время каждый раз, чтобы была выборка из нескольких вариантов


```python
# 4 варианта дистанции между транзакциями в километрах
distances = pd.DataFrame({"distance":[800, 1200, 5000, 7500]})
distances["minutes"] = 0

# Время последней легальной транзакции на основании которого будет рассчитываться время для фрода
last_time = pd.to_datetime("2023-09-20 09:14:00", format="%Y-%m-%d %H:%M:%S")
last_unix = pd_timestamp_to_unix(last_time)
distances["last_unix"] = last_unix

distances
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>distance</th>
      <th>minutes</th>
      <th>last_unix</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>800</td>
      <td>0</td>
      <td>1695201240</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1200</td>
      <td>0</td>
      <td>1695201240</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5000</td>
      <td>0</td>
      <td>1695201240</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7500</td>
      <td>0</td>
      <td>1695201240</td>
    </tr>
  </tbody>
</table>
</div>



Вызов функции в цикле и запись созданного времени


```python
for row in distances.itertuples():
    _, txn_unix = generate_time_fast_geo_jump(last_txn_unix=last_unix, geo_distance=row.distance, threshold=800)
    distances.loc[row.Index, "txn_unix"] = txn_unix

# Получившаяся разнциа в минутах
distances["time_diff"] = (distances["txn_unix"] - distances["last_unix"]) // 60

# Можно увидеть заданную дистанцию и время за которое клиент "переместился"
distances[["distance", "time_diff"]]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>distance</th>
      <th>time_diff</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>800</td>
      <td>20.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1200</td>
      <td>13.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5000</td>
      <td>53.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7500</td>
      <td>127.0</td>
    </tr>
  </tbody>
</table>
</div>



<br>

--------------------------------

## 2. Функция генерации времени транзакции для симуляции участившихся транзакций `gen_time_for_frequent_trans`
- для фрод кейсов когда транзакции клиента резко участились - серия из нескольких частых транзакций подряд - правило `trans_freq_increase`
- модуль `data_generator.fraud.compr.time`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/time.py)** на файл в Github 

**Условия**
1. Подразумевается что время перввой фрод транзакции в серии участившихся транзакций создается функцией `derive_from_last_time` которая будет объеяснена дальше в ноутбуке, а эта функция создает время для всех последующих транзакций в серии, это как бы один фрод кейс все вместе.

**Основная логика функции**  
1. Берется время предыдущей транзакции
2. Создается случайная разница времени, но в рамках заданных минимума и максимума, указанных в конфигах в `compr.yaml`
3. Ко времени предыдущей транзакции прибавляется эта случайная разница

### **Демонстрация**


```python
from data_generator.fraud.compr.time import gen_time_for_frequent_trans
```

**Тест `gen_time_for_frequent_trans` в цикле**


```python
transactions = create_txns_df(base_cfg["txns_df"])
```


```python
# Датафрейм со временем одной условно легальной транзакции
last_time = pd.to_datetime("2023-09-20 09:14:00", format="%Y-%m-%d %H:%M:%S")
last_unix = pd_timestamp_to_unix(last_time)

trans_test_freq_time = transactions.copy().loc[:, ['txn_time', 'unix_time', "is_fraud"]]
trans_test_freq_time.loc[0, ["txn_time","unix_time", "is_fraud"]] = last_time, last_unix, False
trans_test_freq_time
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>is_fraud</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-09-20 09:14:00</td>
      <td>1.695201e+09</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
</div>



В конфигах выставлена частота транзакций от 1 до 5 минут включительно


```python

freq_values = []

for i in range(1,7):
    txn_time, txn_unix, freq = gen_time_for_frequent_trans(last_txn_unix=last_unix, configs=configs, test=True)
    freq_values.append(freq / 60)
        
    trans_test_freq_time.loc[i, "txn_time"] = txn_time
    trans_test_freq_time.loc[i, "unix_time"] = txn_unix
    trans_test_freq_time.loc[i, "is_fraud"] = True

    last_time = txn_time
    last_unix = txn_unix
    # к последней легальной транзакции присоединяем сгенерированные частые транзакции
    # trans_test_freq_time = pd.concat([trans_test_freq_time, trans_test_freq_time])

# средняя частота фрод транзакций - всех, кроме первой фрод транзакции
print("\n\n Mean freq: ",pd.Series(freq_values).mean(), " minutes\n\n")

# разница во времени с предыдущей транзакцией, в минутах
trans_test_freq_time["time_diff"] = trans_test_freq_time.unix_time.sub(trans_test_freq_time.unix_time.shift(1)) / 60
trans_test_freq_time
```

    
    
     Mean freq:  2.6666666666666665  minutes
    
    
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>is_fraud</th>
      <th>time_diff</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-09-20 09:14:00</td>
      <td>1.695201e+09</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2023-09-20 09:17:00</td>
      <td>1.695201e+09</td>
      <td>True</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2023-09-20 09:20:00</td>
      <td>1.695202e+09</td>
      <td>True</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2023-09-20 09:21:00</td>
      <td>1.695202e+09</td>
      <td>True</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2023-09-20 09:24:00</td>
      <td>1.695202e+09</td>
      <td>True</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2023-09-20 09:26:00</td>
      <td>1.695202e+09</td>
      <td>True</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2023-09-20 09:30:00</td>
      <td>1.695202e+09</td>
      <td>True</td>
      <td>4.0</td>
    </tr>
  </tbody>
</table>
</div>



<br>

------------------------
## 3. Функция `derive_from_last_time`  
- модуль `data_generator.fraud.time`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/time.py)** на файл в Github 
- генерация времени через прибавление ко времени последней транзакции:
    - Либо на основании гео дистанции между транзакциями либо на основании заданного лага по времени.
    - Для генерации по дистанции надо передать geo_distance. Для генерации по лагу, НЕ передавать geo_distance, ввести lag_interval
- **Одна из целей использования функции** это генерация времени фрод транзакций антифрод правило которых не является одним из правил с резкой сменой гео, чтобы они не попадали под такие правила 

**Основная логика функции**  
Use-case #1 - контроль времени, чтобы не попадать под антифрод правила резкой смены гео 
1. Берется время последней легальной транзакции и гео дистанция между этой транзакцией и текущей
2. Берется порог скорости перемещения между совершением транзакций - из конфиг файла `compr.yaml`
3. Считается разница во времени обеспечивающая допустимую скорость перемещение - значит непопадание под правило резкой смены гео
4. Разница прибавляется ко времени последней легальной транзакции

Use-case #2 - создание времени на основании переданного лага по времени
1. Берется время последней легальной транзакции
2. Берется переданный желаемый лаг по времени
3. Лаг прибавляется ко времени последней транзакции
4. В данном случае гео дистанция не передается

Use-case #3 - создание времени на основании случайного лага по времени
1. Берется время последней легальной транзакции
2. random_lag=True
3. Передаются мин. и макс. возможные значения лага
4. Генерируется случайный лаг
5. Лаг прибавляется ко времени последней транзакции
6. В данном случае гео дистанция не передается

### **Демонстрация**


```python
from data_generator.fraud.time import derive_from_last_time
```

**Расчет по гео**


```python
# Unix время последней транзакции
last_unix_deriv = pd_timestamp_to_unix(pd.to_datetime("2025-06-20 11:57:12", format="%Y-%m-%d %H:%M:%S"))
```


```python
# Передаем дистанцию и порог скорости км/ч
geo_distance = 1800

_, derived_unix = derive_from_last_time(last_unix_deriv, geo_distance=geo_distance, threshold=800)
```


```python
# Смотрим разницу во времени
pd.to_timedelta((derived_unix - last_unix_deriv), unit="s")
```




    Timedelta('0 days 05:31:17')




```python
# Скорость перемещения км/ч. Должна получится ниже или равна порогу в 800. Т.к. делим на секунды то делим также на 3600

geo_distance / ((derived_unix - last_unix_deriv) / 3600)
```




    326.0049303214771



<br>

-------------------------------------

## 4. Функция `get_time_fraud_txn`
- модуль `data_generator.fraud.compr.time`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/time.py)** на файл в Github 
- конечная функция генерации времени для compomised фрода, использует в себе другие функции времени

**Основная логика функции**
1. в зависимости от переданного антифрод правила выбирает функцию для генерации времени
2. Для этого применяет показанные ранее функции
    - `generate_time_fast_geo_jump`
    - `gen_time_for_frequent_trans`
    - `derive_from_last_time`  

Есть также вариант простого семплирования времени в соотвествии с одним из распределений времени заложенном в конфигах: `Offline_24h_Fraud`, `Online_Fraud`, `Offline_Day_Fraud`. Сейчас такой вариант будет применен если переданное антифрод правило под которое должна попасть транзакция не входит ни в одно правило упомянутое в `if-elif блоке`. Но на данный момент таких правил нет.

<br><br>

-----------------------
# **Генерация части данных транзакции, генерация суммы транзакции**
--------------------

<br>

## 1. Класс `FraudTxnPartData` - генерация части данных транзакции
- модуль `data_generator.fraud.compr.txndata`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/txndata.py)** на файл в Github 
-  генерирует: мерчант id, координаты, ip адрес, город, device id, канал, тип транзакции.

**Основная логика класса**
1. Отталкивается от антифрод правила под которое создается транзакция и онлайн статуса транзакции. Так, например, если транзакция оффлайн то для значений ip адреса, device id будут возвращены `"not applicable"`, `np.nan` соотвественно, а при онлайн транзакции ip адрес и device id. 
2. Основной метод - `get_data()`. Возвращает упомянутые ранее данные или то, что должно быть вместо них в транзакции. Включает в себя другие методы, которые вызываются в зависимости от правила:
    - `another_city()` - имитация транзакции в городе отличном от города клиента. Для онлайн это: другой ip (и при этом другого города), другое устройство, координаты другого города. Для оффлайна: мерчант из другого города, координаты этого мерчанта.
    - `new_device_and_ip()` - чисто онлайн метод. Получение device id отличного от id имеющихся у клиента и ip адреса отличного от клиента. Может вернуть при этом либо ip другого города либо ip города клиента, но отличный от ip клиента.
    - `freq_trans()` - чисто онлайн метод. Сделан для правила `trans_freq_increase` - резкое учащение транз-ций. Использует внутри `new_device_and_ip()`, но только кэширует созданные данные в атрибут `last_txn` чтобы в следующих транзакциях внутри `get_data()` обращаться к кэшированным данным. Это для того, чтобы для одного фрод кейса `trans_freq_increase` не менялись данные о девайсе, ip и т.д.
3. Для генерации данных **нуждается в переданных данных о текущем клиенте** в атрибут `client_info` в виде `namedtuple`. Подразумевается что при итерировании через клиентов при помощи `itertuples()` мы передаем `namedtuple` в этот атрибут

### **Демонстрация**


```python
from data_generator.fraud.compr.txndata import FraudTxnPartData
part_data = FraudTxnPartData(configs=configs)
```


```python
# Класс должен получить данные о клиенте в виде namedtuple
for client_info in configs.clients.loc[[1]].itertuples():
    part_data.client_info = client_info
part_data.client_info
```




    Pandas(Index=1, client_id=418, city_id=28, birth_date=Timestamp('1991-10-07 00:00:00'), sex='male', region='Челябинская', city='Магнитогорск', timezone='UTC+5', lat=53.4071891, lon=58.9791432, population=408401, home_ip='2.60.1.142')




```python
# Какие правила под compromised фрод есть
configs.rules
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>rule</th>
      <th>weight</th>
      <th>online</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>fast_geo_change</td>
      <td>0.12500</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>fast_geo_change_online</td>
      <td>0.21875</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2</th>
      <td>new_ip_and_device_high_amount</td>
      <td>0.25000</td>
      <td>True</td>
    </tr>
    <tr>
      <th>3</th>
      <td>new_device_and_high_amount</td>
      <td>0.18750</td>
      <td>True</td>
    </tr>
    <tr>
      <th>4</th>
      <td>trans_freq_increase</td>
      <td>0.21875</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Например возьмем правило быстрой смены гео. Транзакция оффлайн
rule = "fast_geo_change"

merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type \
                            = part_data.get_data(rule=rule, online=False, category_name="gas_transport", txn_num=1)
```


```python
# Город будет отличаться от города клиента. Также будут отличаться координаты, device_id
client_city = part_data.client_info.city

print(f"""Город клиента: {client_city}
Город транзакции: {trans_city}""")
```

    Город клиента: Магнитогорск
    Город транзакции: Липецк
    


```python
# Например возьмем правило транзакции с другого ip (при этом ip другого города) и другого девайса
rule = "new_ip_and_device_high_amount"

merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type \
                            = part_data.get_data(rule=rule, online=True, category_name="shopping_net", txn_num=1)
```


```python
# Город будет отличаться от города клиента. Также будут отличаться координаты, ip адрес, device_id

print(f"""Город клиента: {client_city}
Город транзакции: {trans_city}""")
```

    Город клиента: Магнитогорск
    Город транзакции: Стерлитамак
    


```python
# Нужно подгрузить девайсы клиентов и взять девайс(ы) клиента
clients_devices = pd.read_csv(data_paths["base"]["client_devices"])
client_id = part_data.client_info.client_id
devices = clients_devices.loc[clients_devices.client_id == client_id]
devices
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>platform</th>
      <th>device_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>397</th>
      <td>418</td>
      <td>iOS</td>
      <td>703</td>
    </tr>
    <tr>
      <th>5674</th>
      <td>418</td>
      <td>Windows</td>
      <td>704</td>
    </tr>
  </tbody>
</table>
</div>




```python
# девайс транзакции

print(f"""Device id транзакции: {device_id}""")
```

    Device id транзакции: 12780
    

<br>

------------------------------------
## 2. Класс `TransAmount`
- модуль `data_generator.fraud.compr.txndata`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/txndata.py)** на файл в Github
- генерация суммы транзакции для compromised client фрода.
- демонстрации работы не будет, только объяснение

**Основная логика класса**
1. создает сумму по нормальному распределению с ограничениями
2. Для всех правил кроме `trans_freq_increase` создает сумму на основе датафрейма из конфигов `configs.fraud_amounts`
3. Для `trans_freq_increase` создает сумму на основе индивидуальных конфигов для этого правила берущихся из `configs.rules_cfg["freq_txn"]`
4. В обоих случаях случайно применяет целочисленное округление через функцию `amt_rounding` из модуля `data_generator.utils` - либо округляет либо оставляет сумму как есть. В свою очередь целочисленное округление включает случайное число округления: 1, 10, 100, 1000, но не больше самой суммы.

<br>

-------------------------------------
## 3. Функция генерации одной транзакции `gen_purchase_fraud_txn`
- модуль `data_generator.fraud.compr.txns`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/txns.py)** на файл в Github
- полное создание одной транзакции 

**Основная логика**
1. На основе переданного правила, последней легальной транзакции клиента и других аргументов создает целую транзакцию
2. Использует:
    - `sample_category()` - функция случайного выбора категории покупки
    - ранее упомянутый `FraudTxnPartData` - мерчант, гео, ip и др.
    - ранее упомянутый `TransAmount` - сумма транзакции
    - `calc_distance()` - функция расчета дистанции между текущей и последней легальной транзакцией
    - ранее упомянутая `get_time_fraud_txn()` - генерация времени


```python
def gen_purchase_fraud_txn(rule, client_trans_df, configs: ComprClientFraudCfg, \
                           part_data: FraudTxnPartData, fraud_amts: TransAmount, \
                           txn_num=0, lag=False):
    """
    Генерация одной compromised client фрод транзакции для клиента.
    ------------------------------------------------
    rule - str.
    client_trans_df - датафрейм с транзакциями клиента.
    configs - ComprClientFraudCfg. Конфиги и данные для генерации фрод транзакций.
    client_device_ids - pd.Series. id девайсов клиента.
    part_data - FraudTxnPartData.
    fraud_amts - TransAmount class.
    txn_num - int. Какая по счету транзакция в данном фрод кейсе.
    lag - bool. Нужен ли лаг по времени от последней легальной транзакции.
          Используется для trans_freq_increase
    -------------------------------------------------
    Возвращает словарь с готовой транзакцией
    """
    client_info = part_data.client_info
    rules_cfg = configs.rules_cfg
    
    # Запись о последней транзакции клиента
    last_txn = client_trans_df.loc[client_trans_df.unix_time == client_trans_df.unix_time.max()]
    
    # Записываем данные клиента в переменные
    client_id = client_info.client_id

    # Берем значение online флага для выбранного правила
    online = configs.rules.loc[configs.rules.rule == rule, "online"].iat[0]
    
    # Семплирование категории. У категорий свой вес в разрезе вероятности быть фродом
    category = sample_category(configs.categories, online=online, is_fraud=is_fraud)
    
    category_name = category["category"].iat[0]
    round_clock = category["round_clock"].iat[0]
    
    # Генерация суммы транзакции. 
    # Пока что для всех правил кроме trans_freq_increase генерация через один и тот же метод
    if rule == "trans_freq_increase":
        amount = fraud_amts.freq_trans_amount()
    else:
        amount = fraud_amts.fraud_amount(category_name=category_name)
    
    partial_data = part_data.get_data(rule=rule, online=online, category_name=category_name, \
                                      txn_num=txn_num)
    # Распаковка кортежа в переменные
    merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, txn_type = partial_data
    
    # Физическое расстояние между координатами последней транзакции и координатами текущей.
    geo_distance = calc_distance(lat_01=last_txn.trans_lat.iat[0], lon_01=last_txn.trans_lon.iat[0], \
                                 lat_02=trans_lat, lon_02=trans_lon)
    
    txn_time, txn_unix = get_time_fraud_txn(trans_df=client_trans_df, configs=configs, online=online, \
                                            round_clock=round_clock, rule=rule, geo_distance=geo_distance, \
                                            lag=lag)
    # Только для freq_trans статус может отличаться от declined и is_fraud быть False для части транз-ций
    # При кол-ве до freq_min - approved и False. Условно, детект по этому правилу начинается
    # с freq_min транз-ций
    freq_min = rules_cfg["freq_txn"]["txn_num"]["min"]
    if rule == "trans_freq_increase" and (txn_num > 0 and txn_num < freq_min):
        rule_to_txn = "not applicable"
        status = "approved"
        is_fraud = False

    elif rule == "trans_freq_increase":
        rule_to_txn = "trans_freq_increase"
        status = "declined"
        is_fraud = True

    else:
        rule_to_txn = rule
        status = "declined"
        is_fraud = True
        
    # Статичные значения для данной функции
    is_suspicious = False
    account = np.nan
    
    # Возвращаем словарь со всеми данными сгенерированной транзакции
    return build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, \
                             txn_type=txn_type,  channel=channel, category_name=category_name, online=online, \
                             merchant_id=merchant_id, trans_city=trans_city, trans_lat=trans_lat, \
                             trans_lon=trans_lon, trans_ip=trans_ip, device_id=device_id, account=account, \
                             is_fraud=is_fraud, is_suspicious=is_suspicious, status=status, rule=rule_to_txn)
```

### **Демонстрация**


```python
from data_generator.fraud.compr.txndata import FraudTxnPartData, TransAmount
from data_generator.fraud.compr.txns import gen_purchase_fraud_txn
from data_generator.utils import create_txns_df, calc_distance

transactions = create_txns_df(base_cfg["txns_df"])
part_data = FraudTxnPartData(configs)
txn_amt = TransAmount(configs)

for client in configs.clients.loc[[1]].itertuples():
    client_info = client
client_id = client_info.client_id

client_txns_df = configs.transactions.query("client_id == @client_id")
part_data.client_info = client_info
client_info
```




    Pandas(Index=1, client_id=1125, city_id=9, birth_date=Timestamp('1959-03-24 00:00:00'), sex='female', region='Кемеровская', city='Новокузнецк', timezone='UTC+7', lat=53.7942757, lon=87.2144046, population=547885, home_ip='2.60.4.47')




```python
configs.rules
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>rule</th>
      <th>weight</th>
      <th>online</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>fast_geo_change</td>
      <td>0.12500</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>fast_geo_change_online</td>
      <td>0.21875</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2</th>
      <td>new_ip_and_device_high_amount</td>
      <td>0.25000</td>
      <td>True</td>
    </tr>
    <tr>
      <th>3</th>
      <td>new_device_and_high_amount</td>
      <td>0.18750</td>
      <td>True</td>
    </tr>
    <tr>
      <th>4</th>
      <td>trans_freq_increase</td>
      <td>0.21875</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>



В примере возьмем не просто последнюю транзакцию клиента, а последнюю онлайн. Конечно при генерации будет учитываться именно последняя транзакция любого типа, но тут возьмем, чтобы увидеть в т.ч. разные ip адреса и device id


```python
# Возьмем последнюю легальную ОНЛАЙН транзакцию клиента. Этого достаточно
client_one_txn =  client_txns_df.query("online == True").loc[[client_txns_df.query("online == True").unix_time.idxmax()]]

# Генерация одной фрод транзакции под антифрод правило быстрой смены гео при онлайн транзакции
fraud_one_test = gen_purchase_fraud_txn(rule="fast_geo_change_online", client_trans_df=client_one_txn, \
                                        configs=configs, part_data=part_data, fraud_amts=txn_amt,
                                        txn_num=1, lag=False)

fraud_all_txns = pd.concat([client_one_txn, pd.DataFrame([fraud_one_test])]).reset_index(drop=True)
fraud_all_txns
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1125</td>
      <td>2025-01-12 10:16:00</td>
      <td>1736676960</td>
      <td>1.0</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6930.0</td>
      <td>Новокузнецк</td>
      <td>53.794276</td>
      <td>87.214405</td>
      <td>2.60.4.47</td>
      <td>1961.0</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1125</td>
      <td>2025-01-12 11:50:52</td>
      <td>1736682652</td>
      <td>5840.0</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6959.0</td>
      <td>Уфа</td>
      <td>54.734853</td>
      <td>55.957865</td>
      <td>5.8.14.16</td>
      <td>14984.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>fast_geo_change_online</td>
    </tr>
  </tbody>
</table>
</div>



**Выше обратите внимание** на разные города, ip адреса и device id. Также на status и rule транзакции

----------------
Расчет разницы во времени между последней легальной транз. и фрод транз.


```python
# Расчет разницы во времени между последней легальной транз. и фрод транз.
pd.to_timedelta(fraud_all_txns.unix_time.sub(fraud_all_txns.unix_time.shift(1)).abs(), unit="s")
```




    0               NaT
    1   0 days 01:34:52
    Name: unix_time, dtype: timedelta64[ns]



Расчет расстояния между координатами транзакций. Будет видно, что для разницы во времени дистанция довольно большая


```python
lat_prev = fraud_all_txns.loc[0, "trans_lat"]
lon_prev = fraud_all_txns.loc[0, "trans_lon"]
lat_now = fraud_all_txns.loc[1, "trans_lat"]
lon_now = fraud_all_txns.loc[1, "trans_lon"]
```


```python
calc_distance(lat_01=lat_prev, lon_01=lon_prev, lat_02=lat_now, lon_02=lon_now)
```




    2022.44



<br>

-------------------
## 4. Функция обертка `trans_freq_wrapper` для функции `gen_purchase_fraud_txn`
- модуль `data_generator.fraud.compr.txns`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/txns.py)** на файл в Github
-  обертка нужна под правило `trans_freq_increase`
- т.к. правило `trans_freq_increase` подразумевает фрод кейс с несколькими транзакциями, то `gen_purchase_fraud_txn` должна быть вызвана несколько раз. Эта функция обеспечивает это и контроль логики для фрод кейса под это правило. Это единственное правило из compromised clients правил где создается больше одной транзакции

**Основная логика функции**
1. Задача функции вызвать `gen_purchase_fraud_txn` указанное число раз и записать это все во временный датафрейм и вернуть его как результат
2. Также задача выставить значение аргумента `lag` для `gen_purchase_fraud_txn` в зависимости от того первая это транзакция в серии или не первая. `lag` значит нужен ли лаг по времени от предыдущей транзакции, подразумевается что он нужен для первой фрод транзакции, чтобы была достаточная временная разница с последней легальной транзакцией, чтобы не попадать под правило резкой смены гео.

### **Демонстрация**


```python
from data_generator.fraud.compr.txns import trans_freq_wrapper

transactions = create_txns_df(base_cfg["txns_df"])

part_data = FraudTxnPartData(configs)
txn_amt = TransAmount(configs)

for client in configs.clients.loc[[1]].itertuples():
    client_info = client
client_id = client_info.client_id

client_txns_df = configs.transactions.query("client_id == @client_id")
part_data.client_info = client_info
client_info
```




    Pandas(Index=1, client_id=1971, city_id=3, birth_date=Timestamp('1994-03-08 00:00:00'), sex='female', region='Удмуртская', city='Ижевск', timezone='UTC+4', lat=56.8527444, lon=53.2113961, population=628117, home_ip='2.60.7.69')




```python
# Датафрейм с последней легальной транзакцией клиента
client_txns_copy = client_txns_df.loc[[client_txns_df.unix_time.idxmax()]].copy()

# txns_total - сколько транзакций будет в серии
fraud_only = trans_freq_wrapper(client_txns_temp=client_txns_copy, txns_total=6, configs=configs, \
                                part_data=part_data, fraud_amts=txn_amt)

client_trans_copy = pd.concat([client_txns_copy, fraud_only], ignore_index=True)
```

**Результат**   
- первая в списке транзакция это легальная транзакция клиента, остальное все фрод в рамках правила `trans_freq_increase`
- обратите внимание что условный фрод детект происходит не сразу, и транзакция помечается как фрод, только на 4-й раз. Этот порог задается в конфиге `compr.yaml`. Т.е. как-будто бы 3 частых транзакции еще нормально, но 4 и более - фрод.


```python
client_txns_copy
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>445</td>
      <td>2025-01-30 12:41:39</td>
      <td>1738240899</td>
      <td>3000.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6960.0</td>
      <td>Москва</td>
      <td>55.753879</td>
      <td>37.620373</td>
      <td>2.60.1.169</td>
      <td>782.0</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>1</th>
      <td>445</td>
      <td>2025-01-30 15:59:57</td>
      <td>1738252797</td>
      <td>3000.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6947.0</td>
      <td>Кострома</td>
      <td>57.802945</td>
      <td>40.990728</td>
      <td>5.8.8.149</td>
      <td>11218.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>2</th>
      <td>445</td>
      <td>2025-01-30 16:00:57</td>
      <td>1738252857</td>
      <td>4817.66</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6947.0</td>
      <td>Кострома</td>
      <td>57.802945</td>
      <td>40.990728</td>
      <td>5.8.8.149</td>
      <td>11218.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>3</th>
      <td>445</td>
      <td>2025-01-30 16:03:57</td>
      <td>1738253037</td>
      <td>2892.04</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6947.0</td>
      <td>Кострома</td>
      <td>57.802945</td>
      <td>40.990728</td>
      <td>5.8.8.149</td>
      <td>11218.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>4</th>
      <td>445</td>
      <td>2025-01-30 16:04:57</td>
      <td>1738253097</td>
      <td>5200.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6947.0</td>
      <td>Кострома</td>
      <td>57.802945</td>
      <td>40.990728</td>
      <td>5.8.8.149</td>
      <td>11218.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>trans_freq_increase</td>
    </tr>
    <tr>
      <th>5</th>
      <td>445</td>
      <td>2025-01-30 16:07:57</td>
      <td>1738253277</td>
      <td>3670.88</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6947.0</td>
      <td>Кострома</td>
      <td>57.802945</td>
      <td>40.990728</td>
      <td>5.8.8.149</td>
      <td>11218.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>trans_freq_increase</td>
    </tr>
    <tr>
      <th>6</th>
      <td>445</td>
      <td>2025-01-30 16:10:57</td>
      <td>1738253457</td>
      <td>4520.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6947.0</td>
      <td>Кострома</td>
      <td>57.802945</td>
      <td>40.990728</td>
      <td>5.8.8.149</td>
      <td>11218.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>trans_freq_increase</td>
    </tr>
  </tbody>
</table>
</div>



**Разница во времени с предыдущей транзакцией**  
Можно заметить что сперва большая разница между первой фрод транзакцией и последней легальной(т.е. которая самая первая), а дальше идут разницы меньше 5 минут, так и задумано.


```python
pd.to_timedelta(client_trans_copy.unix_time.sub(client_trans_copy.unix_time.shift(1)).abs(), unit="s")
```




    0               NaT
    1   0 days 02:44:32
    2   0 days 00:01:00
    3   0 days 00:03:00
    4   0 days 00:02:00
    5   0 days 00:02:00
    6   0 days 00:05:00
    Name: unix_time, dtype: timedelta64[ns]



**Проверим что** дистанция между координатами последней легальной и первой фрод транзакциями приемлема с точки зрения разницы времени


```python
lat_prev = client_trans_copy.loc[0, "trans_lat"]
lon_prev = client_trans_copy.loc[0, "trans_lon"]
lat_now = client_trans_copy.loc[1, "trans_lat"]
lon_now = client_trans_copy.loc[1, "trans_lon"]
```


```python
calc_distance(lat_01=lat_prev, lon_01=lon_prev, lat_02=lat_now, lon_02=lon_now)
```




    293.43



<br><br>

------------------------------------------
# **Генерация нескольких транзакций, запись транзакций в файл**
--------------------

<br>

## 1. Класс `FraudTxnsRecorder`
- модуль `data_generator.fraud.recorder`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/recorder.py)** на файл в Github
- Запись фрод транзакций в файл. **Этот класс** таже применяется для записи транзакций при дроп фроде, о котором будет в следующих ноутбуках

**Основная логика**
1. Получает в атрибут `all_txns` `pd.DataFrame` со всеми созданными транзакции по определенному типу фрода
2. Записывает этот датафрейм в `parquet` в двух директориях: индивидуальной директории текущей генерации и директории последнего запуска генератора транзакций. Т.е. например `data/generated/history/<текущий_запуск>/<тип_фрода>` и `data/generated/latest/`
3. В случае `compromised` фрода подразумевается встраивание объекта этого класса в функцию `gen_multi_fraud_txns`

<br><br>

-------
## 2. Функция генерации нескольких фрод транзакций `gen_multi_fraud_txns`
- модуль `data_generator.fraud.compr.txns`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/fraud/compr/txns.py)** на файл в Github
- полная генерация множетсва транзакций для переданных клиентов под разные антифрод правила в рамках compromised client фрода

**Основная логика**
1. Итерирование через переданных клиентов и генерация фрод транзакций для них.
2. Антифрод правило для каждого клиента берется случайно и под это правило генерируется фрод для клиента.
3. Все датафреймы с фрод транзакциями пишутся в общий список объявенный перед началом цикла. Потом всё соединяется через `pd.concat` и передается в `FraudTxnsRecorder.all_txns`, затем вызвается `FraudTxnsRecorder.write_to_file()` и транзакции записываются в файлы в двух директориях.


```python
def gen_multi_fraud_txns(configs: ComprClientFraudCfg, part_data: FraudTxnPartData, \
                         fraud_amts: TransAmount, txn_recorder: FraudTxnsRecorder):
    """
    clients_subset: pd.DataFrame. Клиенты у которых будут фрод транзакции. Сабсет клиентов для кого нагенерили
                     легальных транзакций ранее.
    configs: ComprClientFraudCfg. Конфиги для транзакций.
    part_data: FraudTxnPartData. Генератор части данных транзакций.
    fraud_amts: TransAmount. Генератор сумм транзакций.
    txn_recorder: FraudTxnsRecorder. 
    """
    all_fraud_txns = []
    # Конфиги кол-ва транз. для правила trans_freq_increase
    freq_cfg = configs.rules_cfg["freq_txn"]["txn_num"]

    for client in configs.clients.itertuples():
        rule = sample_rule(configs.rules)

        client_txns = configs.transactions.loc[configs.transactions.client_id == client.client_id]
        # Записываем данные текущего клиента в атрибут client_info класса FraudTxnPartData
        part_data.client_info = client
        
        # Это правило отдельно т.к. такой случай имеет несколько транз-ций
        if rule == "trans_freq_increase":
            client_txns_temp = client_txns.loc[[client_txns.unix_time.idxmax()]]
            # Сколько транз. будет создано под это правило
            low = freq_cfg["min"]
            high = freq_cfg["max"]
            txns_total = np.random.randint(low, high + 1) 

            # Генерируем txns_total число фрод транзакций. Датафрейм с ними записываем в переменную
            fraud_only = trans_freq_wrapper(client_txns_temp=client_txns_temp, \
                                            txns_total=txns_total, configs=configs, \
                                            part_data=part_data, fraud_amts=fraud_amts)
            
            # Добавляем созданные транзакции в общий список и сразу переводим цикл на следующую итерацию
            all_fraud_txns.append(fraud_only)
            continue

        # Остальные правила. Генерация одной транз-ции
        else:
            one_txn = gen_purchase_fraud_txn(rule=rule, client_trans_df=client_txns, \
                                             configs=configs, part_data=part_data, \
                                             fraud_amts=fraud_amts)
            
            all_fraud_txns.append(pd.DataFrame([one_txn]))

    # Запись транзакций в файл  
    txn_recorder.all_txns = pd.concat(all_fraud_txns, ignore_index=True)
    txn_recorder.write_to_file()
```

### **Демонстрация**


```python
from data_generator.fraud.compr.txns import gen_multi_fraud_txns
from data_generator.fraud.recorder import FraudTxnsRecorder
from data_generator.fraud.compr.txndata import FraudTxnPartData, TransAmount

part_data = FraudTxnPartData(configs)
txn_amt = TransAmount(configs)
txn_recorder = FraudTxnsRecorder(configs)
```


```python
# Тест
gen_multi_fraud_txns(configs=configs, part_data=part_data, fraud_amts=txn_amt, txn_recorder=txn_recorder)
```


```python
data_storage = compr_cfg["data_storage"]
compr_subdir = data_storage["folder_name"] # Название поддиректории в папке созданной под текущую генерацию
compr_file = data_storage["files"]["txns"] # Название файла с транзакциями
path_to_compr = os.path.join(run_dir, compr_subdir, compr_file)

compr_fraud_demo = pd.read_parquet(path_to_compr)
```


```python
compr_fraud_demo.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1114</td>
      <td>2025-01-20 11:13:47</td>
      <td>1737371627</td>
      <td>24741.85</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6949.0</td>
      <td>Иваново</td>
      <td>56.999468</td>
      <td>40.972823</td>
      <td>5.8.23.242</td>
      <td>12408.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>new_ip_and_device_high_amount</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1125</td>
      <td>2025-01-18 18:47:28</td>
      <td>1737226048</td>
      <td>19148.95</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6835.0</td>
      <td>Новокузнецк</td>
      <td>53.794276</td>
      <td>87.214405</td>
      <td>5.8.0.153</td>
      <td>14458.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>new_device_and_high_amount</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2401</td>
      <td>2025-01-20 10:57:21</td>
      <td>1737370641</td>
      <td>49142.25</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6964.0</td>
      <td>Новосибирск</td>
      <td>55.028102</td>
      <td>82.921058</td>
      <td>5.8.14.8</td>
      <td>10034.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>fast_geo_change_online</td>
    </tr>
    <tr>
      <th>3</th>
      <td>875</td>
      <td>2025-01-20 10:00:45</td>
      <td>1737367245</td>
      <td>5400.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6812.0</td>
      <td>Иркутск</td>
      <td>52.286351</td>
      <td>104.280655</td>
      <td>5.8.12.79</td>
      <td>14763.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>new_ip_and_device_high_amount</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3522</td>
      <td>2025-01-20 00:47:50</td>
      <td>1737334070</td>
      <td>8570.35</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6895.0</td>
      <td>Ярославль</td>
      <td>57.621614</td>
      <td>39.897878</td>
      <td>5.8.28.44</td>
      <td>12616.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>new_ip_and_device_high_amount</td>
    </tr>
  </tbody>
</table>
</div>




```python
compr_fraud_demo.shape
```




    (185, 19)



**Возьмем пример для каждого правила:** последняя легальная транзакция клиента, затем фрод, который у него был


```python
# Подгрузим легальные транзакции. Сразу отсортируем по client_id и времени

legit_txns_full = pd.read_parquet(run_dir + "/legit/legit_txns.parquet").sort_values(["client_id","unix_time"])
legit_txns_full.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1108</th>
      <td>4</td>
      <td>2025-01-02 14:17:00</td>
      <td>1735827420</td>
      <td>1722.00</td>
      <td>purchase</td>
      <td>POS</td>
      <td>grocery_pos</td>
      <td>False</td>
      <td>4336.0</td>
      <td>Ростов-на-Дону</td>
      <td>47.186882</td>
      <td>39.540858</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>1837</th>
      <td>4</td>
      <td>2025-01-03 08:14:00</td>
      <td>1735892040</td>
      <td>1070.83</td>
      <td>purchase</td>
      <td>POS</td>
      <td>shopping_pos</td>
      <td>False</td>
      <td>6038.0</td>
      <td>Ростов-на-Дону</td>
      <td>47.269532</td>
      <td>39.734244</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Возьмем только последнюю легальную транзакцию для каждого клиента
legit_full_last_only = legit_txns_full.groupby("client_id", as_index=False).tail(1)
legit_full_last_only.head(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>18764</th>
      <td>4</td>
      <td>2025-01-19 21:53:00</td>
      <td>1737323580</td>
      <td>1837.07</td>
      <td>purchase</td>
      <td>POS</td>
      <td>grocery_pos</td>
      <td>False</td>
      <td>948.0</td>
      <td>Ростов-на-Дону</td>
      <td>47.264105</td>
      <td>39.643790</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>19043</th>
      <td>5</td>
      <td>2025-01-20 08:48:00</td>
      <td>1737362880</td>
      <td>1.00</td>
      <td>purchase</td>
      <td>POS</td>
      <td>shopping_pos</td>
      <td>False</td>
      <td>2650.0</td>
      <td>Ростов-на-Дону</td>
      <td>47.198945</td>
      <td>39.493719</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>17964</th>
      <td>17</td>
      <td>2025-01-19 18:24:20</td>
      <td>1737311060</td>
      <td>473.47</td>
      <td>purchase</td>
      <td>POS</td>
      <td>home</td>
      <td>False</td>
      <td>6204.0</td>
      <td>Тюмень</td>
      <td>57.211488</td>
      <td>65.566663</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Отфильтруем только те легальные, которые принадлежат клиентам у которых был фрод

fraud_client_ids = compr_fraud_demo.client_id.unique()
legit_txns = legit_full_last_only.loc[legit_full_last_only.client_id.isin(fraud_client_ids)]
legit_txns.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>18666</th>
      <td>66</td>
      <td>2025-01-19 21:34:06</td>
      <td>1737322446</td>
      <td>1.0</td>
      <td>purchase</td>
      <td>POS</td>
      <td>shopping_pos</td>
      <td>False</td>
      <td>2549.0</td>
      <td>Москва</td>
      <td>55.702551</td>
      <td>37.417362</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>19088</th>
      <td>97</td>
      <td>2025-01-20 09:04:00</td>
      <td>1737363840</td>
      <td>1000.0</td>
      <td>purchase</td>
      <td>POS</td>
      <td>gas_transport</td>
      <td>False</td>
      <td>3188.0</td>
      <td>Орёл</td>
      <td>52.934794</td>
      <td>36.058780</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Соединим последние легальные и созданный фрод.

last_leg_and_fraud = pd.concat([legit_txns, compr_fraud_demo]).sort_values(["client_id","unix_time"]).reset_index(drop=True)
last_leg_and_fraud.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>66</td>
      <td>2025-01-19 21:34:06</td>
      <td>1737322446</td>
      <td>1.0</td>
      <td>purchase</td>
      <td>POS</td>
      <td>shopping_pos</td>
      <td>False</td>
      <td>2549.0</td>
      <td>Москва</td>
      <td>55.702551</td>
      <td>37.417362</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>1</th>
      <td>66</td>
      <td>2025-01-19 21:42:01</td>
      <td>1737322921</td>
      <td>7000.0</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6923.0</td>
      <td>Москва</td>
      <td>55.753879</td>
      <td>37.620373</td>
      <td>5.8.30.49</td>
      <td>10821.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>new_device_and_high_amount</td>
    </tr>
  </tbody>
</table>
</div>



<br>

**Правило `fast_geo_change`**  
- резкая смена гео между транзакциями.
- фрод транзакция оффлайн


```python
# Чтобы показать не только фрод но и последнюю легальную транзакцию, нужно фильтровать по client_id
# И взять client_id клиента у которого был нужный фрод
fast_geo_client = last_leg_and_fraud.query("rule == 'fast_geo_change'").client_id.head(1).iat[0]

last_leg_and_fraud.loc[last_leg_and_fraud.client_id == fast_geo_client]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>15</th>
      <td>527</td>
      <td>2025-01-20 12:09:00</td>
      <td>1737374940</td>
      <td>3627.31</td>
      <td>purchase</td>
      <td>POS</td>
      <td>grocery_pos</td>
      <td>False</td>
      <td>3019.0</td>
      <td>Иваново</td>
      <td>56.994509</td>
      <td>40.883388</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>16</th>
      <td>527</td>
      <td>2025-01-20 12:25:34</td>
      <td>1737375934</td>
      <td>32204.00</td>
      <td>purchase</td>
      <td>POS</td>
      <td>misc_pos</td>
      <td>False</td>
      <td>505.0</td>
      <td>Тверь</td>
      <td>56.826437</td>
      <td>35.920547</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>fast_geo_change</td>
    </tr>
  </tbody>
</table>
</div>



<br>

**Правило `fast_geo_change_online`**  
- резкая смена гео между транзакциями.
- фрод транзакция онлайн


```python
fast_geo_net_client = last_leg_and_fraud.query("rule == 'fast_geo_change_online'").client_id.head(1).iat[0]

last_leg_and_fraud.loc[last_leg_and_fraud.client_id == fast_geo_net_client]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>97</td>
      <td>2025-01-20 09:04:00</td>
      <td>1737363840</td>
      <td>1000.0</td>
      <td>purchase</td>
      <td>POS</td>
      <td>gas_transport</td>
      <td>False</td>
      <td>3188.0</td>
      <td>Орёл</td>
      <td>52.934794</td>
      <td>36.058780</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>3</th>
      <td>97</td>
      <td>2025-01-20 09:06:35</td>
      <td>1737363995</td>
      <td>3700.0</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6878.0</td>
      <td>Брянск</td>
      <td>53.242007</td>
      <td>34.365272</td>
      <td>5.8.3.245</td>
      <td>11543.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>fast_geo_change_online</td>
    </tr>
  </tbody>
</table>
</div>



<br>

**Правило `new_ip_and_device_high_amount`**  
- транзакция на большую сумму с другого ip + это ip **другого города**, но без гео скачка; а также с другого устройства.
- фрод транзакция онлайн


```python
new_dev_and_ip_client = last_leg_and_fraud.query("rule == 'new_ip_and_device_high_amount'").client_id.head(1).iat[0]

last_leg_and_fraud.loc[last_leg_and_fraud.client_id == new_dev_and_ip_client]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4</th>
      <td>398</td>
      <td>2025-01-20 12:58:00</td>
      <td>1737377880</td>
      <td>742.4</td>
      <td>purchase</td>
      <td>POS</td>
      <td>personal_care</td>
      <td>False</td>
      <td>5583.0</td>
      <td>Тверь</td>
      <td>56.882587</td>
      <td>35.841957</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>5</th>
      <td>398</td>
      <td>2025-01-20 17:50:22</td>
      <td>1737395422</td>
      <td>4000.0</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6937.0</td>
      <td>Новосибирск</td>
      <td>55.028102</td>
      <td>82.921058</td>
      <td>5.8.19.222</td>
      <td>11861.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>new_ip_and_device_high_amount</td>
    </tr>
  </tbody>
</table>
</div>



<br>

**Правило `new_device_and_high_amount`**  
- транзакция на большую сумму с другого ip, **но это ip города клиента**; а также с другого устройства.
- фрод транзакция онлайн


```python
new_dev_client = last_leg_and_fraud.query("rule == 'new_device_and_high_amount'").client_id.head(1).iat[0]

last_leg_and_fraud.loc[last_leg_and_fraud.client_id == new_dev_client]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>66</td>
      <td>2025-01-19 21:34:06</td>
      <td>1737322446</td>
      <td>1.0</td>
      <td>purchase</td>
      <td>POS</td>
      <td>shopping_pos</td>
      <td>False</td>
      <td>2549.0</td>
      <td>Москва</td>
      <td>55.702551</td>
      <td>37.417362</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>1</th>
      <td>66</td>
      <td>2025-01-19 21:42:01</td>
      <td>1737322921</td>
      <td>7000.0</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6923.0</td>
      <td>Москва</td>
      <td>55.753879</td>
      <td>37.620373</td>
      <td>5.8.30.49</td>
      <td>10821.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>new_device_and_high_amount</td>
    </tr>
  </tbody>
</table>
</div>



<br>

**Правило `trans_freq_increase`**  
- резкое учащение транзакций клиента. Несколько частых транзакций подряд
- фрод транзакции онлайн
- первые несколько транзакций из серии не отмечены как фрод и не отклонены, но подразумевается, что это тоже фрод, просто на тот момент еще не был достигнут порог частых транзакций. Напоминаю, что первая транзакция это последняя легальная транзакция клиента
- суммы в этом типе фрода не слишком большие, подразумевается будто мошенник делает частые транзакции, но суммы не особо большие, чтобы избежать детекта


```python
freq_txns_client = last_leg_and_fraud.query("rule == 'trans_freq_increase'").client_id.head(1).iat[0]

last_leg_and_fraud.loc[last_leg_and_fraud.client_id == freq_txns_client]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client_id</th>
      <th>txn_time</th>
      <th>unix_time</th>
      <th>amount</th>
      <th>type</th>
      <th>channel</th>
      <th>category</th>
      <th>online</th>
      <th>merchant_id</th>
      <th>trans_city</th>
      <th>trans_lat</th>
      <th>trans_lon</th>
      <th>trans_ip</th>
      <th>device_id</th>
      <th>account</th>
      <th>is_fraud</th>
      <th>is_suspicious</th>
      <th>status</th>
      <th>rule</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>6</th>
      <td>444</td>
      <td>2025-01-19 11:08:00</td>
      <td>1737284880</td>
      <td>1030.15</td>
      <td>purchase</td>
      <td>POS</td>
      <td>home</td>
      <td>False</td>
      <td>4250.0</td>
      <td>Санкт-Петербург</td>
      <td>60.029397</td>
      <td>30.307338</td>
      <td>not applicable</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>7</th>
      <td>444</td>
      <td>2025-01-20 03:54:52</td>
      <td>1737345292</td>
      <td>2555.48</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6909.0</td>
      <td>Севастополь</td>
      <td>44.616733</td>
      <td>33.525355</td>
      <td>5.8.3.179</td>
      <td>11052.0</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>8</th>
      <td>444</td>
      <td>2025-01-20 03:55:52</td>
      <td>1737345352</td>
      <td>4500.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6909.0</td>
      <td>Севастополь</td>
      <td>44.616733</td>
      <td>33.525355</td>
      <td>5.8.3.179</td>
      <td>11052.0</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>9</th>
      <td>444</td>
      <td>2025-01-20 03:57:52</td>
      <td>1737345472</td>
      <td>2186.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>grocery_net</td>
      <td>True</td>
      <td>6909.0</td>
      <td>Севастополь</td>
      <td>44.616733</td>
      <td>33.525355</td>
      <td>5.8.3.179</td>
      <td>11052.0</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>approved</td>
      <td>not applicable</td>
    </tr>
    <tr>
      <th>10</th>
      <td>444</td>
      <td>2025-01-20 04:02:52</td>
      <td>1737345772</td>
      <td>2200.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>shopping_net</td>
      <td>True</td>
      <td>6909.0</td>
      <td>Севастополь</td>
      <td>44.616733</td>
      <td>33.525355</td>
      <td>5.8.3.179</td>
      <td>11052.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>trans_freq_increase</td>
    </tr>
    <tr>
      <th>11</th>
      <td>444</td>
      <td>2025-01-20 04:03:52</td>
      <td>1737345832</td>
      <td>3914.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>grocery_net</td>
      <td>True</td>
      <td>6909.0</td>
      <td>Севастополь</td>
      <td>44.616733</td>
      <td>33.525355</td>
      <td>5.8.3.179</td>
      <td>11052.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>trans_freq_increase</td>
    </tr>
    <tr>
      <th>12</th>
      <td>444</td>
      <td>2025-01-20 04:08:52</td>
      <td>1737346132</td>
      <td>4000.00</td>
      <td>purchase</td>
      <td>ecom</td>
      <td>misc_net</td>
      <td>True</td>
      <td>6909.0</td>
      <td>Севастополь</td>
      <td>44.616733</td>
      <td>33.525355</td>
      <td>5.8.3.179</td>
      <td>11052.0</td>
      <td>NaN</td>
      <td>True</td>
      <td>False</td>
      <td>declined</td>
      <td>trans_freq_increase</td>
    </tr>
  </tbody>
</table>
</div>



<br><br>

---------------------------------
# **Оркестрация генерации compromised фрод транзакций**

<br>

## 1. Класс `ComprRunner`
- модуль `data_generator.runner.compr`. **[Ссылка](https://github.com/iaroslav-dzh/fraud_txns_simulator/blob/main/data_generator/runner/compr.py)** на файл в Github 
- Оркестрация генерации compromised client fraud транзакций
- Это финальный уровень для compromised транзакций. Метод `run()` этого класса вызывается в файле запуска генератора всех транзакций `run_generator.py`


```python
class ComprRunner:
    """
    Запуск генерации транзакций compromised фрода.
    ---------
    Атрибуты:
    ---------
    cfg_builder: ComprConfigBuilder.
    configs: ComprClientFraudCfg. Конфиги и данные для генерации транзакций.
    part_data: FraudTxnPartData. Генерация части данных транзакции:
               мерчант, геопозиция, город, IP адрес и др.
    fraud_amts: TransAmount. Генерация суммы транзакций.
    txn_recorder: FraudTxnsRecorder. Запись транзакций в файл.
    text: str. Текст для вставки в спиннер.
    """
    def __init__(self, base_cfg, legit_cfg, time_cfg, fraud_cfg, compr_cfg, run_dir):
        """
        base_cfg: dict. Конфиги из base.yaml
        legit_cfg: dict. Конфиги из legit.yaml
        time_cfg: dict. Конфиги из time.yaml
        fraud_cfg: dict. Общие конфиги фрода из fraud.yaml
        compr_cfg: dict. Конфиги для compromised фрода из compr.yaml
        run_dir: str. Название директории для хранения сгенерированных
                 данных текущей генерации.
        """
        self.cfg_builder = ComprConfigBuilder(base_cfg=base_cfg, legit_cfg=legit_cfg, \
                                              time_cfg=time_cfg, fraud_cfg=fraud_cfg, \
                                              compr_cfg=compr_cfg, run_dir=run_dir)
        self.configs = self.cfg_builder.build_cfg()
        self.part_data = FraudTxnPartData(configs=self.configs)
        self.fraud_amts = TransAmount(configs=self.configs)
        self.txn_recorder = FraudTxnsRecorder(configs=self.configs)
        self.text = "Compromised clients fraud txns generation"


    @spinner_decorator
    def run(self):
        """
        Запуск генератора.
        """
        configs = self.configs
        part_data = self.part_data
        fraud_amts = self.fraud_amts
        txn_recorder = self.txn_recorder

        gen_multi_fraud_txns(configs=configs, part_data=part_data, fraud_amts=fraud_amts, \
                             txn_recorder=txn_recorder)
```
