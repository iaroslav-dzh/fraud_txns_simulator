class CreateDropTxn:
    """
    Создание транзакций дропа под разное поведение.
    """
    def __init__(self, timestamps, trans_partial_data, drop_client_cls, in_txns=0, out_txns=0, in_lim=6, out_lim=8, last_txn={}, \
                attempts=0):
        """
        timestamps - pd.DataFrame.
        trans_partial_data - FraudTransPartialData. Генератор части данных транзакции - мерчант, гео, ip, девайс и т.п.
        drop_client_cls - DropClient. Генератор активности дропов: суммы, счета, баланс
        in_txns - int. Количество входящих транзакций
        out_txns - int. Количество исходящих транзакций
        in_lim - int. Лимит входящих транзакций. Транзакции клиента совершенные после достижения этого лимита отклоняются
        out_lim - int. Лимит исходящих транзакций. Транзакции клиента совершенные после достижения этого лимита отклоняются
        last_txn - dict. Полные данные последней транзакции
        attempts - int. Сколько попыток совершить операцию будет сделано дропом после первой отклоненной транзакции.
        """
        self.timestamps = timestamps
        self.trans_partial_data = trans_partial_data
        self.drop_client = drop_client_cls
        self.in_txns = in_txns
        self.out_txns = out_txns
        self.in_lim = in_lim
        self.out_lim = out_lim
        self.last_txn = last_txn
        self.attempts = attempts

    def get_time_delta(self, min=-180, max=180, minutes=True):
        """
        Получение случайного интервала времени в секундах или минутах из равномерного распределения
        ---------------------
        min - int. Минимальное возможное значение
        max - int. Максимальное возможное значение
        minutes - bool. Минуты или секунды
        """
        if minutes:
            return round(np.random.uniform(min, max))
            
        return round(np.random.uniform(min, max) * 60)

    def get_txn_time(self, in_lim=2, out_lim=5, lag_interval=1440):
        """
        Генерация времени транзакции
        ------------------
        in_lim - int. Количество входящих транзакций после которых дроп уходит на паузу указанную в lag_interval
                      Т.е. если на момент генерации времени уже сделано in_lim транзакций, то берется время последней
                      транзакции и прибавляется указанный lag_interval +/- случайное число минут из delta.
        out_lim - int. Количество исходящих транзакций после которых дроп уходит на паузу указанную в lag_interval
        lag_interval - int. Желаемый лаг по времени от последней транзакции в минутах.
                            Используется для перерывов в активности дропа. По умолчанию 1440 минут т.е. 24 часа
        """
        # Если это первая транзакция. Т.к. активность дропа начинается с входящей транзакции
        if self.in_txns == 0:
            time_sample = self.timestamps.sample(1)
            txn_time = time_sample.timestamp.iat[0]
            txn_unix = time_sample.unix_time.iat[0]
            return txn_time, txn_unix

        # Для последующих транзакций
        last_txn_unix = self.last_txn["unix_time"]

        # Если достигнуты лимиты активности дропа на период: входящих или исходящих транзакций
        if self.in_txns == in_lim or self.out_txns == out_lim:
            # Генерация дельты, чтобы время выглядело не ровным, а случайным.
            # Слагаем её с lag_interval
            time_delta = self.get_time_delta(min=-180, max=180)
            lag_interval += time_delta
            return derive_from_last_time(last_txn_unix=last_txn_unix, lag_interval=lag_interval)

        # Тоже дельта, но не может быть <= 0 т.к. тут мы ее используем как lag_interval
        # Это для случаев когда транзакция совершается в тот же период активности что и последняя
        time_delta = self.get_time_delta(min=30, max=180)
        

        return derive_from_last_time(last_txn_unix=last_txn_unix, lag_interval=time_delta)

    
    def stop_after_decline(self, declined):
        """
        Будет ли дроп пытаться еще после отклоненной операции
        или остановится
        """
        if not declined:
            return
        
        if self.attempts == 0:
            return True

        if self.attempts > 0:
            return False

            
    def attempts_after_decline(self, min=0, max=4):
        """
        Определение количества попыток после первой отклоненной транзакции
        ---------------
        min - int. Минимальное число попыток
        max - int. Максимальное число попыток
        """
        self.attempts = np.random.randint(min, max)
            
        
    def deduct_attempts(self, declined, receive):
        """
        Вычитание попытки операции совершенной при статусе declined
        ---------------
        declined - bool. Отклоняется ли текущая транзакция
        receive - bool. Является ли транзакция входящей
        """
        if self.attempts == 0:
            return 
            
        if declined and not receive:
            self.attempts -= 1
            
        
    def single_operation(self, online, declined, in_chunks, to_drop_share=0.2, receive=False):
        """
        Один входящий/исходящий перевод либо одно снятие в банкомате.
        ---------------------
        online - bool. Онлайн перевод или снятие в банкомате.
        declined
        in_chunks
        to_drop_share - float. Вероятность, что дроп пошлет другому дропу
        receive - входящий перевод или нет.
        """
        client_id = self.trans_partial_data.client_info.client_id
        
        # Время транзакции. Оно должно быть создано до увеличения счетчика self.in_txns
        txn_time, txn_unix = self.get_txn_time(in_lim=2, out_lim=5, lag_interval=1440)

        if receive:
            self.in_txns += 1
            amount = self.drop_client.receive(declined=declined)
            account = self.drop_client.account
            
        elif not receive and online:
            to_drop = np.random.choice([True, False], p=[to_drop_share, 1 - to_drop_share])
            self.out_txns += 1
            account = self.drop_client.get_account(to_drop=to_drop)
            
        elif not receive and not online:
            account = self.drop_client.account
            self.out_txns += 1
        
            
        # Генерация части данных транзакции. Здесь прописываются аргументы online и receive
        merchant_id, trans_lat, trans_lon, trans_ip, trans_city, device_id, channel, type = \
                                                self.trans_partial_data.original_data(online=online, receive=receive)
        
        # Генерация суммы если исходящая транзакция
        # Если эта транзакция только часть серии операций для распределения всего баланса
        if in_chunks:
            chunk = self.drop_client.get_chunk_size(online=online, atm_min=10000, start=5000, stop=25000, step=5000)
            amount = self.drop_client.one_operation(amount=chunk, declined=declined, in_chunks=in_chunks)
            
        # Иначе если не по частям и не входящая транзакция
        elif not in_chunks and not receive:
            amount = self.drop_client.one_operation(declined=declined, in_chunks=in_chunks)

        
        if declined:
            status = "declined"
            is_fraud = True
            rule = "client_is_drop"
        else:
            status = "approved"
            is_fraud = False
            rule = "not applicable"

        # Статисчные характеристики
        is_suspicious = False
        category_name="not applicable"

        # Сборка всех данных в транзакцию и запись как послдней транзакции
        self.last_txn = build_transaction(client_id=client_id, txn_time=txn_time, txn_unix=txn_unix, amount=amount, type=type, channel=channel, \
                             category_name=category_name, online=online, merchant_id=merchant_id, trans_city=trans_city, \
                             trans_lat=trans_lat, trans_lon=trans_lon, trans_ip=trans_ip, device_id=device_id, account_to=account, \
                             is_fraud=is_fraud, is_suspicious=is_suspicious, status=status, rule=rule)

        return self.last_txn

    def limit_reached(self):
        """
        Проверка достижения лимитов входящих и исходящих транзакций
        Сверка с self.in_lim и self.out_lim
        ------------------------
        Вернет True если какой либо лимит достигнут
        """
        if self.in_lim == self.in_txns:
            return True
        if self.out_lim == self.out_txns:
            return True
        return False

    def reset_txn_counters(self, in_txns=False, out_txns=False, batch_txns=False):
        """
        Сброс счетчиков входящих и/или исходящих транзакций
        Партия денег это полученные деньги которые дроп должен распределить
        ------------------
        in_txns - bool. Сбросить счетчик входящих.
        out_txns - bool. Сбросить счетчик исходящих.
        batch_txns - bool. Сбросить счетчик транзакций партии денег.
        """
        if in_txns:
            self.in_txns = 0
        if out_txns:
            self.out_txns = 0
        if batch_txns:
            self.batch_txns = 0

    def reset_cache(self, only_counters=True):
        """
        Сброос кэшированных данных
        -------------
        only_counters - bool
                        Если True будут сброшены: self.in_txns, self.out_txns, self.attempts.
                        Если False то также сбросится информация о последней транзакции self.last_txn
        """
        
        self.in_txns = 0
        self.out_txns = 0
        self.attempts = 0

        if only_counters:
            return

        self.last_txn = {}



# Объявление новых объектов классов для тестов.
drop_txn_part_data = FraudTransPartialData(merchants_df=pd.DataFrame(), client_info=clients_with_geo.loc[0], \
                                        online_merchant_ids=pd.DataFrame(), fraud_ips=fraud_ips, used_ips=pd.Series(), \
                                         fraud_devices=fraud_devices,  used_devices=pd.Series(), \
                                        client_devices=client_devices)


drop_client_test2 = DropClient(accounts=accounts, account=1, outer_accounts=outer_accounts)

create_drop_txn_tst = CreateDropTxn(timestamps=drop_stamps, trans_partial_data=drop_txn_part_data, drop_client_cls=drop_client_test2, \
                                      in_txns=0, out_txns=0, in_lim=6, out_lim=8, last_txn={}, attempts=0)

create_drop_txn_tst.in_txns, create_drop_txn_tst.out_txns, create_drop_txn_tst.last_txn, drop_client_test2.batch_txns



create_drop_txn_tst.reset_cache(only_counters=False)
drop_client_test2.reset_cache(balance=True, used_accounts=True, chunk_size=True, batch_txns=True)
all_txns5 = []
declined=False

while create_drop_txn_tst.attempts != 0:
    receive_txn8 = create_drop_txn_tst.single_operation(online=True, declined=declined, in_chunks=False, receive=True)
    all_txns5.append(receive_txn8)
    
    create_drop_txn_tst.attempts = 1
    drop_client_test2.chunk_size = 5000
    drop_client_test2.batch_txns = 1
    i = 1
    while drop_client_test2.balance > 0:
        if declined and create_drop_txn_tst.attempts == 0:
            break
        part_out = create_drop_txn_tst.single_operation(online=True, receive=False, declined=declined, in_chunks=True)
        all_txns5.append(part_out)
        print(f"iter {i}")
        declined = create_drop_txn_tst.limit_reached()
        i += 1
    pd.DataFrame(all_txns5)