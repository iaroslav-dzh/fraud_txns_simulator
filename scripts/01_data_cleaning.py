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
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, Float, BigInteger, String, Date

# %%
os.getcwd()

# %%
os.listdir('data/raw_data')

# %% [markdown]
# ## Таблица account
#
# | item        | meaning                             | remark                                                                                                                                                              |
# | ----------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
# | account_id  | identification of the account       |                                                                                                                                                                     |
# | district_id | location of the branch              |                                                                                                                                                                     |
# | date        | date of creating of the account     | in the form YYMMDD                                                                                                                                                  |
# | frequency   | frequency of issuance of statements | "POPLATEK MESICNE" stands for monthly issuance<br><br>"POPLATEK TYDNE" stands for weekly issuance<br><br>"POPLATEK PO OBRATU" stands for issuance after transaction |
#
# ***frequency of issuance of statements** - частота выдачи выписок по счету. "Ежемесячно", "Еженедельно", "После транзакции"

# %%
account_df = pd.read_csv('data/raw_data/account.asc', sep=';', dtype={'date':'str'})
account_df.head(3)

# %%
# изначальный размер датафрейма
account_initial_shape = account_df.shape

# %%
account_df.dtypes

# %%
# меняем тип date на datetime64[ns]
account_df.date = pd.to_datetime(account_df.date, format='%y%m%d')
account_df.date.dt.year.unique()


# %%
# Функция проверки на NaN в любом месте датафрейма
def check_for_nans(df: pd.DataFrame):
    rows_isna = df.isna().any(axis=1)
    
    if rows_isna.any():
        print("Following rows have missing values!")
        return df[rows_isna]
    else:
        print("No missing values")


# %%
# проверяем account_df на наличие NaN
check_for_nans(account_df)

# %%
# фактические значения в колонке frequency
account_df.frequency.unique()

# %%
# меняем значения в frequency с чесшского на англоязычные. Функция

freq_mapping = {'POPLATEK MESICNE':'monthly issuance', 'POPLATEK TYDNE':'weekly issuance', \
               'POPLATEK PO OBRATU':'issuance per transaction'}
account_df.frequency = account_df.frequency.replace(freq_mapping).fillna('unspecified')

account_df.frequency.unique()


# %%
# функция проверки датафрейма после изменений
# df_name название датафрейма

def check_df_integrity(initial_df_shape: tuple[int, int], current_df: pd.DataFrame, df_name: str, axis: int | None = None):

    if not isinstance(df_name, str):
        raise TypeError(f'df_name must be a string (name of a DataFrame), but got {type(df_name)}')
        
    current_shape = current_df.shape
    axis_names = {0:'rows', 1:'columns'}
    
    
    if axis != None:
        initial_ax = initial_df_shape[axis]
        current_ax = current_shape[axis]
        other_axis = 1 - axis
        
    
        if initial_ax != current_ax:
            raise ValueError(f'''Something is wrong with {axis_names[axis]}!
            initial {axis_names[axis]} shape is: {initial_ax}
            current {axis_names[axis]} shape is: {current_ax}''')
            
        else:
            initial_other = initial_df_shape[other_axis]
            current_other = current_shape[other_axis]
            
            if current_other != initial_other:
                print(f'''{axis_names[axis]} count is equal.
But changes were made to {axis_names[other_axis]}
initial {axis_names[other_axis]} shape is: {initial_df_shape[other_axis]}
current {axis_names[other_axis]} shape is: {current_shape[other_axis]}''')
            else:
                print(f"{df_name} is intact!")

    else:
        if initial_df_shape != current_shape:
           raise ValueError(f'''Something is wrong!
           initial shape is: {initial_df_shape}
           current shape is: {current_shape}''')
        else:    
            print(f"{df_name} is intact!")

# %%
# проверка размеров датафрейма после изменений
check_df_integrity(account_initial_shape, account_df, 'account_df')

# %%
account_df.head(3)


# %%
# функция сохранения датафрейма в csv

def df_to_csv(df: pd.DataFrame, df_name: str, directory: str = 'data/cleaned_data/', suffix: str = 'clean'):

    # проверка что аргумент df_name это строка
    if not isinstance(df_name, str):
        raise TypeError(f"df_name must be a string (name of the DataFrame), got {type(df_name)}")

    # Создать директорию если она не существует
    os.makedirs(directory, exist_ok=True)

    # название файла
    if suffix:
        filename = f"{df_name}_{suffix}.csv"
    else:
        filename = f"{df_name}.csv"
    # путь к файлу: директория + название файла
    filepath = os.path.join(directory, filename)

    # сохраняем датафрейм в csv, индекс не сохраняем
    df.to_csv(filepath, index=False)
    print(f"Saved cleaned DataFrame to {filepath}")


# %%
df_to_csv(account_df, 'account_df')

# %% [markdown]
# # Таблица client
# | item         | meaning                  | remark                                                                                                                                   |
# | ------------ | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
# | client_id    | record identifier        |                                                                                                                                          |
# | birth number | identification of client | the number is in the form YYMMDD for men,  <br>the number is in the form YYMM+50DD for women,  <br><br>where YYMMDD is the date of birth |
# | district_id  | address of the client    |                                                                                                                                          |
#
# ***YYMM+50DD for women** значит что для мужчины например 1972-04-01 будет 720401, а для женщины 725401. Нужно отфильтровать где средние два числа больше 12 и на этом условии создать колонку с полом; а также заменить две средние цифры для женщин - вычесть из них 50 или 5000 из всего числа
#

# %%
client_df = pd.read_csv(r'data/raw_data/client.asc', sep=';')

# %%
client_df.head(3)

# %%
client_df.dtypes

# %%
# проверка на NaN в любой колонке
check_for_nans(client_df)

# %%
client_initial_shape = client_df.shape
client_initial_shape


# %%
# функция определения пола по дате и возврата обычной даты для женщин, но всё еще в виде целого числа

def check_birth_num(birth_num):
    if birth_num // 100 % 100 > 50:
        return birth_num - 5000, 'female'
    elif birth_num // 100 % 100 <= 12:
        return birth_num, 'male'
    else:
        return 'invalid_date', 'unknown'


# %%
client_df['birth_date'], client_df['sex'] = zip(*client_df.birth_number.map(check_birth_num))


# %%
def parse_int_date(date: int):
    year = date // 10_000
    month = date % 10_000 // 100
    day = date % 10_000 % 100

    full_year = 1900 + year if year > 9 else 2000 + year
    return pd.to_datetime(f'{full_year}-{month}-{day}')
    


# %%
client_df['birth_date'] = client_df['birth_date'].map(parse_int_date)

# %%
client_df.head()

# %%
# Проверка что нет дат с 50+MM и полом "не женщина"
# filtered_df это датафрейм где первые две цифры birth_number отличаются если отнять 5000 и пол "female"
# то есть по ошибке мужчину отметили как женщину

filtered_df =  client_df[((client_df.birth_number - 5000) // 10000 != client_df.birth_number // 10000) \
          & (client_df.sex == 'female')]

if not filtered_df.empty:
    raise ValueError(f'''Wrong rows in client_df!
{filtered_df}''')
else:
    print('client_df is correct.')

# %%
client_df.dtypes

# %%
client_df = client_df.drop(columns='birth_number')
client_df.head(2)

# %%
# Проверка целостности client_df по строкам после изменений
check_df_integrity(client_initial_shape, client_df, 'client_df', 0)

# %%
df_to_csv(client_df, 'client_df')

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Таблица disposition
# | item       | meaning                          | remark                                                   |
# | ---------- | -------------------------------- | -------------------------------------------------------- |
# | disp_id    | record identifier                |                                                          |
# | client_id  | identification of a client       |                                                          |
# | account_id | identification of an account     |                                                          |
# | type       | type of disposition (owner/user) | only owner can issue permanent orders and ask for a loan |
#
# each record relates together a client with an account i.e. this relation describes the rights of clients to operate accounts

# %%
disposition_df = pd.read_csv('data/raw_data/disp.asc', sep=';')

# %%
disposition_df.head()

# %%
disposition_df.dtypes

# %%
disposition_df.type.unique()

# %%
# проверка на NaN-ы
check_for_nans(disposition_df)

# %%
disposition_initial_shape = disposition_df.shape
disposition_initial_shape

# %%
# import warnings

# warnings.warn("Что-то подозрительное, но программа идёт дальше", UserWarning)

# %%
df_to_csv(disposition_df, 'disposition_df')

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Таблица permanent order
# | item       | meaning                          | remark                                                                                                                                                |
# | ---------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
# | order_id   | record identifier                |                                                                                                                                                       |
# | account_id | account, the order is issued for |                                                                                                                                                       |
# | bank_to    | bank of the recipient            | each bank has unique two-letter code                                                                                                                  |
# | account_to | account of the recipient         |                                                                                                                                                       |
# | amount     | debited amount                   |                                                                                                                                                       |
# | K_symbol   | characterization of the payment  | "POJISTNE" stands for insurrance payment<br><br>"SIPO" stands for household<br><br>"LEASING" stands for leasing<br><br>"UVER" stands for loan payment |
#
# - each record describes characteristics of a payment order
# - SIPO is a service provided by Česká pošta (Czech Post). It allows you to combine several regular direct debits into a single payment (e.g. rent, telephone, radio)
# - Czech Post offers SIPO services as an agent collecting payments from individuals on behalf of legal or other entities under a SIPO agency contract. SIPO can be used to pay for rental, electricity, gas, water, radio and television subscription fees, cable television, newspaper and magazine subscription fees, building savings, life and other insurance, and other payments or fees
#

# %%
order_df = pd.read_csv('data/raw_data/order.asc', sep=';')
order_df.head()

# %%
order_initial_shape = order_df.shape
order_initial_shape

# %%
# Проверка на NaN
check_for_nans(order_df)

# %%
order_df.k_symbol.unique()

# %%
order_df.dtypes


# %%
# Функция замены значений в колонке k_symbol

def replace_k_symbol(k_symbol_val):
    if k_symbol_val == 'POJISTNE':
        return 'insurance payment'
    elif k_symbol_val == 'SIPO':
        return 'household'
    elif k_symbol_val == 'LEASING':
        return 'leasing'
    elif k_symbol_val == 'UVER':
        return 'loan payment'
    else:
        return 'unclassified' # для всех остальных значений


# %%
order_mapping = {'POJISTNE':'insurance payment', 'SIPO':'household', 'LEASING':'leasing', 'UVER':'loan payment', '':'unclassified'}

order_df.k_symbol = order_df.k_symbol.str.strip().replace(order_mapping)
order_df.k_symbol.unique()

# %%
order_df.head()

# %%
# Проверка целостности после изменений
check_df_integrity(order_initial_shape, order_df, 'order_df')

# %%
df_to_csv(order_df, 'order_df')

# %% [markdown]
# # Таблица Transaction
# | item       | meaning                             | remark                                                                                                                                                                                                                                                                                                              |
# | ---------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
# | trans_id   | record identifier                   |                                                                                                                                                                                                                                                                                                                     |
# | account_id | account, the transation deals with  |                                                                                                                                                                                                                                                                                                                     |
# | date       | date of transaction                 | in the form YYMMDD                                                                                                                                                                                                                                                                                                  |
# | type       | +/- transaction                     | "PRIJEM" stands for credit<br><br>"VYDAJ" stands for withdrawal                                                                                                                                                                                                                                                     |
# | operation  | mode of transaction                 | "VYBER KARTOU" credit card withdrawal<br><br>"VKLAD" credit in cash<br><br>"PREVOD Z UCTU" collection from another bank<br><br>"VYBER" withdrawal in cash<br><br>"PREVOD NA UCET" remittance to another bank                                                                                                        |
# | amount     | amount of money                     |                                                                                                                                                                                                                                                                                                                     |
# | balance    | balance after transaction           |                                                                                                                                                                                                                                                                                                                     |
# | k_symbol   | characterization of the transaction | "POJISTNE" stands for insurrance payment<br><br>"SLUZBY" stands for payment for statement<br><br>"UROK" stands for interest credited<br><br>"SANKC. UROK" sanction interest if negative balance<br><br>"SIPO" stands for household<br><br>"DUCHOD" stands for old-age pension<br><br>"UVER" stands for loan payment |
# | bank       | bank of the partner                 | each bank has unique two-letter code                                                                                                                                                                                                                                                                                |
# | account    | account of the partner              |                                                                                                                                                                                                                                                                                                                     |

# %%
transaction_df = pd.read_csv('data/raw_data/trans.asc', sep=';', \
                             dtype={'trans_id':'str','account_id':'str','bank':'str', 'account':'str'})

# %%
transaction_df.dtypes

# %%
transaction_df.head()

# %%
transaction_df.isna().sum()

# %%
trans_initial_shape = transaction_df.shape

# %%
# переводим целые числа в date в формат даты.
transaction_df['date'] = pd.to_datetime(transaction_df['date'], format='%y%m%d')
transaction_df.head(3)

# %%
# проверка значений в колонке type
transaction_df.type.unique()

# %%
# видно что в type есть значение VYBER, которого там по аннотации к данным быть не должно. Посмотрим количество VYBER в колонке type
transaction_df[transaction_df.type == 'VYBER'].type.count()

# %%
# копирую датафрейм для дальнейшего удобства при случае если надо откатить датафрейм к состоянию до изменений
transaction_df_copy = transaction_df.copy()

# %%
# В этой ячейке мы создаем датафрейм с транзакциями аккаунтов у которых есть type == 'VYBER'.
# Чтобы потом посмотреть где предыдущий баланс и текущий баланс на момент транзакции с type == 'VYBER' сходятся, а где нет.
# И в связи с этим поменять VYBER на VYDAJ либо на что-то типа uknown или unidentified

# фильтрация аккаунтов где тип тразакции VYBER
vyber_accounts = transaction_df_copy[transaction_df_copy.type == 'VYBER'].account_id.unique()

# Сортируем по account_id и trans_id для дальнейшего создания колонки с балансом на момент предыдущей транзакции
vyber_acc_trans = transaction_df_copy.loc[transaction_df_copy.account_id.isin(vyber_accounts)].sort_values(by=['account_id','trans_id'])

# Создаем колонку с балансом на предыдущую транзакцию
vyber_acc_shift = vyber_acc_trans.groupby('account_id', as_index=False).balance.shift(1)
vyber_acc_trans['prev_balance'] = vyber_acc_shift

# ставим prev_balance рядом с balance
vyber_acc_trans = vyber_acc_trans.loc[:,['trans_id', 'account_id', 'date', 'type', 'operation', 'amount', \
       'balance','prev_balance', 'k_symbol', 'bank', 'account']]

# %%
vyber_acc_trans.shape

# %%
vyber_acc_trans.head(3)

# %%
# Создание серий с trans_id по критерию консистентности баланса для последующей смены либо на VYDAJ либо на unknown
# То есть если type равно VYBER и текущий баланс отличается от баланса предыдущей транзакции на amount в отрицательную сторону,
# то записываем VYDAJ вместо VYBER либо unknown если баланс непоследовательный

# создаем датафрейм где type равняется VYBER
vyber_trans = vyber_acc_trans[vyber_acc_trans.type == 'VYBER']

# создаем булеву маску где текущий баланс некосистентен с балансом предыдущей транзакции
inconsistent_vyber_mask = vyber_trans.prev_balance != vyber_trans.balance + vyber_trans.amount

# применяем булеву маску для неконсистентых записей, которые попадут в новую категорию unknown
trans_id_type_to_unknown = vyber_trans[inconsistent_vyber_mask].trans_id

# применяем булеву маску для консистентных записей, которые попадут в категорию VYDAJ
trans_id_type_to_vydaj = vyber_trans[~inconsistent_vyber_mask].trans_id

# сверяем количество записей после изменений с vyber_trans. при сложении должно быть количество равное vyber_trans
to_vydaj_and_unknown_count = trans_id_type_to_unknown.count() + trans_id_type_to_vydaj.count()
vyber_trans_count = vyber_trans.trans_id.count()

# %%
if to_vydaj_and_unknown_count != vyber_trans_count:
    raise ValueError(f'''Rows count is not equal after changes!
vyber_trans count is {vyber_trans_count} while to_vydaj + to_unknown count is {to_vydaj_and_unknown_count}''')
else:
    print('vyber_trans count is equal to to_vydaj + to_unknown count. Everything is OK.')

# Посмотрим сколько значений попадают под смену на VYDAJ
print(f'\nTransactions count whose type to be changed to VYDAJ: {trans_id_type_to_vydaj.count()}')

# %%
# Меняем VYBER в колонке type соответственно, делая булевы маски по отобранным транзакциям

transaction_df_copy.loc[transaction_df_copy.trans_id.isin(trans_id_type_to_vydaj), 'type'] = 'VYDAJ'
transaction_df_copy.loc[transaction_df_copy.trans_id.isin(trans_id_type_to_unknown), 'type'] = 'unknown'

# смотрим результат: какие теперь значения в колонке type
transaction_df_copy.type.unique()

# %%
# продолжаем дальше знакомиться с данными в transactions
# посмотрим какие пары type и operation есть при условии что bank, k_symbol, account пустые

transaction_df_copy.loc[transaction_df_copy[['bank', 'k_symbol', 'account']].isna().all(axis=1), ['type','operation']].value_counts()

# %% [markdown]
# ## Значения в type
#
# "PRIJEM" stands for credit
#
# "VYDAJ" stands for withdrawal

# %%
# Меняем значения в колонке type на англоязычные
type_mapping = {"PRIJEM":"credit", "VYDAJ":"withdrawal"}
transaction_df_copy.type = transaction_df_copy.type.replace(type_mapping)

# смотрим какие значения после замены
transaction_df_copy.type.unique()

# %%
transaction_df_copy.head(1)

# %%
# посмотрим есть ли транзакции где operation это PREVOD NA UCET(перевод в другой банк)
# или PREVOD Z UCTU (перевод из другого банка)
# и при этом пустое значение account(счет на который переводили)

from_to_another_acc_mask = transaction_df_copy.operation.isin(['PREVOD NA UCET', 'PREVOD Z UCTU'])
acc_isna_mask = transaction_df_copy['account'].isna()
transaction_df_copy.loc[(from_to_another_acc_mask) & (acc_isna_mask)]

# %% [markdown]
# "PREVOD Z UCTU" collection from another bank
#
# "VYBER" withdrawal in cash
#
# "PREVOD NA UCET" remittance to another bank

# %%
# посмотрим теперь где operation это PREVOD NA UCET(перевод в другой банк)
# или PREVOD Z UCTU (перевод из другого банка), но при этом пустое значение bank

bank_isna_mask = transaction_df_copy['bank'].isna()
transaction_df_copy.loc[(from_to_another_acc_mask) & (bank_isna_mask)]

# %%
transaction_df_copy.isna().sum()

# %%
# проверка комбинаций значений: k_symbol, bank, account, где в строке есть operation == NaN

k_sym_bank_or_acc_notna = transaction_df_copy[['k_symbol','bank','account']].notna().any(axis=1)
op_isna_mask = transaction_df_copy['operation'].isna()
transaction_df_copy.loc[(k_sym_bank_or_acc_notna) & (op_isna_mask),['operation','k_symbol','bank','account']].drop_duplicates()

# %% [markdown]
# ## Значения в operation
#
# "VYBER KARTOU" credit card withdrawal
#
# "VKLAD" credit in cash
#
# "PREVOD Z UCTU" collection from another bank
#
# "VYBER" withdrawal in cash
#
# "PREVOD NA UCET" remittance to another bank

# %%
# Смена значений колонки operation на англоязычные

operation_mapping = {"VYBER KARTOU":"credit card withdrawal", "VKLAD":"credit in cash", \
                    "PREVOD Z UCTU":"collection from another bank", "VYBER":"withdrawal in cash", \
                    "PREVOD NA UCET":"remittance to another bank"}
transaction_df_copy.operation = transaction_df_copy.operation.replace(operation_mapping).fillna('unspecified')

# %%
transaction_df_copy.operation.unique()

# %% [markdown]
# ## Значения в k_symbol
# "POJISTNE" stands for insurrance payment
#
# "SLUZBY" stands for payment for statement
#
# "UROK" stands for interest credited
#
# "SANKC. UROK" sanction interest if negative balance
#
# "SIPO" stands for household
#
# "DUCHOD" stands for old-age pension
#
# "UVER" stands for loan payment

# %%
# смотрим что есть в k_symbol
transaction_df_copy.k_symbol.str.strip().unique()

# %%
# меняем значения в k_symbol на анлийский. Пустые строки обозначаем 'unspecified'. NaN-ы не трогаем

k_sym_mapping = {"POJISTNE":"insurance payment", "SLUZBY":"payment for statement", "UROK":"interest credited", \
                "SANKC. UROK": "negative balance sanction interest", "SIPO":"household", \
                 "DUCHOD":"old-age pension", "UVER":"loan payment", "":"unspecified"}

transaction_df_copy.k_symbol = transaction_df_copy.k_symbol.str.strip().replace(k_sym_mapping)
transaction_df_copy.k_symbol.unique()

# %%
# проверка значений в bank

transaction_df_copy.bank.unique()

# %%
# проверяем датафрейм на целостность после изменений. Должны совпадать и строки и колонки

check_df_integrity(trans_initial_shape, transaction_df_copy, 'transaction_df_copy')

# %%
transaction_df_copy.head(3)

# %%
# Сохраняем очищенные данные в csv

df_to_csv(transaction_df_copy, 'transaction_df')

# %% [markdown]
# # Таблица Loan
# | item       | meaning                        | remark                                                                                                                                                                                                            |
# | ---------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
# | loan_id    | record identifier              |                                                                                                                                                                                                                   |
# | account_id | identification of the account  |                                                                                                                                                                                                                   |
# | date       | date when the loan was granted | in the form YYMMDD                                                                                                                                                                                                |
# | amount     | amount of money                |                                                                                                                                                                                                                   |
# | duration   | duration of the loan           |                                                                                                                                                                                                                   |
# | payments   | monthly payments               |                                                                                                                                                                                                                   |
# | status     | status of paying off the loan  | 'A' stands for contract finished, no problems,<br><br>'B' stands for contract finished, loan not paid,<br><br>'C' stands for running contract, OK so far,<br><br>'D' stands for running contract, client in debt |

# %%
loan_df = pd.read_csv('./data/raw_data/loan.asc', sep=';', dtype={'loan_id':'str', 'account_id':'str', 'amount':'float64'})

# %%
loan_initial_shape = loan_df.shape
loan_initial_shape

# %%
loan_df.date = pd.to_datetime(loan_df.date, format='%y%m%d')

# %%
loan_df.date.dt.year.unique()

# %%
loan_df.dtypes

# %%
loan_df.isna().sum()

# %%
loan_df.head()

# %% [markdown]
# 'A' stands for contract finished, no problems,
#
# 'B' stands for contract finished, loan not paid,
#
# 'C' stands for running contract, OK so far,
#
# 'D' stands for running contract, client in debt

# %%
# меняем буквенные обозначения в status на подробные
status_mapping = {'A':'contract finished, no problems', 'B':'contract finished, loan not paid', \
                 'C': 'running contract, OK so far', 'D': 'running contract, client in debt'}

loan_df.status = loan_df.status.replace(status_mapping)
loan_df.status.unique()

# %%
loan_df.describe()

# %%
# целостность датафрейма после изменений

check_df_integrity(loan_initial_shape, loan_df, 'loan_df')

# %%
# Сохраняем очищенные данные в csv

df_to_csv(loan_df, 'loan_df')

# %% [markdown]
# # Таблица Credit Card
#
# | item    | meaning                   | remark                                          |
# | ------- | ------------------------- | ----------------------------------------------- |
# | card_id | record identifier         |                                                 |
# | disp_id | disposition to an account |                                                 |
# | type    | type of card              | possible values are "junior", "classic", "gold" |
# | issued  | issue date                | in the form YYMMDD                              |

# %%
card_df = pd.read_csv('./data/raw_data/card.asc', sep=';', dtype={'card_id':'str', 'disp_id':'str'})

# %%
card_initial_shape = card_df.shape
card_initial_shape

# %%
card_df.dtypes

# %%
card_df.isna().sum()

# %%
card_df.head()

# %%
card_df.issued = pd.to_datetime(card_df.issued, format='%y%m%d %H:%M:%S')
card_df.issued.dt.year.unique()

# %%
card_df.head(3)

# %%
# проверка на целостность

check_df_integrity(card_initial_shape, card_df, 'card_df')

# %%
# Сохраняем очищенные данные в csv

df_to_csv(card_df, 'card_df')

# %% [markdown]
# # Таблица Demographic data (District)
# | item             | meaning                                          | remark |
# | ---------------- | ------------------------------------------------ | ------ |
# | A1 = district_id | district code                                    |        |
# | A2               | district name                                    |        |
# | A3               | region                                           |        |
# | A4               | no. of inhabitants                               |        |
# | A5               | no. of municipalities with inhabitants < 499     |        |
# | A6               | no. of municipalities with inhabitants 500-1999  |        |
# | A7               | no. of municipalities with inhabitants 2000-9999 |        |
# | A8               | no. of municipalities with inhabitants >10000    |        |
# | A9               | no. of cities                                    |        |
# | A10              | ratio of urban inhabitants                       |        |
# | A11              | average salary                                   |        |
# | A12              | unemploymant rate '95                            |        |
# | A13              | unemploymant rate '96                            |        |
# | A14              | no. of enterpreneurs per 1000 inhabitants        |        |
# | A15              | no. of commited crimes '95                       |        |
# | A16              | no. of commited crimes '96                       |        |

# %%
district_df = pd.read_csv("./data/raw_data/district.asc", sep=';')

# %%
district_initial_shape = district_df.shape
district_initial_shape

# %%
district_df.head()

# %%
# строка со значениями колонок по порядку

district_col_raw = ''' district code                                    |
| district name                                    |
| region                                           |
| population                               |
| no_of_mun_below_500     |
| no of mun_between_500_1999  |
| no of mun_between_2000_9999 |
| no of mun_above_10000    |
| no. of cities                                    |
| ratio of urban population                       |
| avg_salary                                   |
| unemployment rate '95                            |
| unemployment rate '96                            |
| enterpreneurs per_1000      |
| crimes num '95                       |
| crimes num '96                       '''

# %%
# разбиваем строку по символу |
district_col_split = district_col_raw.split('|')

# %%
district_col_split

# %%
# убираем пробелы с начала и конца каждой строки
# меняем пробелы на _
# удаляем апострофы
# удаляем точки

dist_col_series = pd.Series(district_col_split).str.strip() \
.str.replace(' ','_') \
.str.replace('\'', '') \
.str.replace('.', '')

# Фильтруем серию, чтобы избавиться от пустых строк. Сбрасываем индекс
dist_col_clean = dist_col_series[dist_col_series != ''].reset_index(drop=True)
dist_col_clean

# %%
# назначаем district_df очищенные названия колонок

district_df.columns = dist_col_clean
district_df.head()

# %%
district_df.dtypes

# %%
# Замена значений '?' в unemployment_rate_95 на NaN, и приведение к float64

unemp95_mapping = {'?':np.nan}
district_df.unemployment_rate_95 = district_df.unemployment_rate_95.replace(unemp95_mapping).astype('float64')
district_df.unemployment_rate_95.unique()

# %%
# Замена значений '?' в crimes_num_95 на NaN, и приведение к float64

crime95_mapping = {'?':pd.NA}
district_df.crimes_num_95 = district_df.crimes_num_95.replace(crime95_mapping).astype('Int64')
district_df.crimes_num_95.unique()

# %%
district_df.isna().sum()

# %%
# проверка целостности

check_df_integrity(district_initial_shape, district_df, 'district_df')

# %%
# Сохраняем очищенные данные в csv

df_to_csv(district_df, 'district_df')

# %%
# В конце сверимся с количеством оригинальных файлов с данными и файлов с очищенными данными

raw_count = len(os.listdir('data/raw_data'))
cleaned_count = len(os.listdir('data/cleaned_data'))

if raw_count != cleaned_count:
    raise ValueError(f'''Cleaned files count differs from raw files count!
Raw files: {raw_count}
Cleaned files: {cleaned_count}
''')

else:
    print('Raw and cleaned files count is equal.')

# %%
