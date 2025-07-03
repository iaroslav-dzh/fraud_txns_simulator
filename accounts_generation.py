## Генерация счетов клиентов и внешних счетов

# **Счета клиентов**  
# Пускай начинаются с 10000

accounts = clients_with_geo[["client_id"]].copy()

accounts["account_id"] = 1

accounts.loc[0, "account_id"] = 10000

accounts.head()

# Кумулятивно складываем числа в серии. Получается в каждой записи будет результат сложения текущего и всех предыдущих чисел
# Т.е. 10000, 10000 + 1, 10001 + 1 и т.д. Так будут счета с номерами от 10000 до 10000 + n-1 клиентов

accounts["account_id"] = accounts["account_id"].cumsum()

accounts.head()

accounts.agg({"account_id":["min","max"]})

assert accounts.shape[0] == accounts.account_id.nunique(), "Values in account_id are not unique!"
accounts.shape[0]

# Колонка is_drop. Дроп клиент или нет. Пока нет дропов.
# Они будут обозначаться непосредственно во время генерации активности дропов

accounts["is_drop"] = False

accounts.head()

# **Внешние счета**  
# Счета начинающиеся с максимального номера счета нашего клиента + 1

# Пусть будет 10000 счетов

start_id = accounts.account_id.max() + 1
outer_accounts = pd.Series(data=np.arange(start_id, start_id + 10000, step=1), name="account_id", dtype="int")

outer_accounts

# Не должно быть пересечений по account_id

assert accounts.merge(outer_accounts, on="account_id").empty, "Clients account ids are in the outer account ids"

# **Сохранение счетов в файлы**

accounts.to_csv("./data/generated_data/accounts.csv", index=False)

outer_accounts.to_csv("./data/generated_data/outer_accounts.csv", index=False)