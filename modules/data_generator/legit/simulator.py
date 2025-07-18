# Симуляция легальных транзакций


from data_generator.legit.txns import gen_multiple_legit_txns


class LegitTxnsSimulator:
    """
    """

    def __init__(self):
        pass

    def generate_in_chunks(self):
        """
        """
        for row in clients_index_chunks.itertuples():
            clients_subset = clients_sample_df.loc[row.lower_bound:row.upper_bound]
            trans_df = transactions.copy()
            chunk_num = row.Index + 1
            
            chunk = gen_multiple_legit_txns(configs, ignore_index=True)

            print(f"Chunk #{chunk_num}")
            # Запись чанка в файл с название по типу legit_000.parquet
            chunk.to_parquet(f"./data/generated_data/legit_{chunk_num:03d}.parquet", engine="pyarrow")