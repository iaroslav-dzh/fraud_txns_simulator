# Генерация части данных транзакции


# 1.

def get_txn_location_and_merchant(online, merchants_df, category_name, client_info, online_merchant_ids):
    """
    Возвращает id мерчанта, геолокацию транзакции: для оффлайна это координаты и город мерчанта, для онлайна координаты по IP и город по IP.
    Возвращает IP адрес с которого совершена транзакция если это онлайн покупка.
    --------------------------------------------------------------------
    client_info - namedtuple, полученная в результате итерации через датафрейм с информацией о клиентах 
                  с помощью .itertuples()
    category_name - Название категории покупки
    merchants_df - датафрейм с оффлайн мерчантами заранее отфильтрованный по городу клиента если это легальные транзакции
    online_merchant_ids - id для онлайн мерчантов
    """

    # Комментарий себе на будущее. Если универсализировать это под фрод. То возможно надо только ip через доп. блок if-else определить
    # И соотвественно для фрод оффлайн передавать merchants_df без города клиента.

    # Если онлайн покупка
    if online:
        merchant_id = online_merchant_ids.sample(n=1).iloc[0]
        # локация клиента по IP. Т.к. это не фрод. Просто записываем координаты города клиента
        trans_lat = client_info.lat
        trans_lon = client_info.lon
        # Также т.к. это не фрод, то просто берется home_ip и город из данных клиента.
        trans_ip = client_info.home_ip
        trans_city = client_info.city
        
    # Если оффлайн покупка    
    else:
        # Семплируется мерчант
        merchant = merchants_df.loc[merchants_df.category == category_name].sample(1, replace=True)
        # Берется его id, и координаты, как координаты транзакции
        merchant_id = merchant["merchant_id"].iloc[0]
        trans_lat = merchant["merchant_lat"].iloc[0]
        trans_lon = merchant["merchant_lon"].iloc[0]
        trans_ip = "not applicable"
        trans_city = merchant["city"].iloc[0]

    return merchant_id, trans_lat, trans_lon, trans_ip, trans_city