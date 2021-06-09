from pyspark.sql import SparkSession
from pyspark.sql import Row
import matplotlib.pyplot as plt
spark = SparkSession.builder.appName("Total de acidentes com vitima por bairro em acidentes com embriaguez").enableHiveSupport().getOrCreate()
spark.sql("use gpdb")
df = spark.sql("SELECT trim(log.`nome_bairro`) as nome_bairro, COUNT(log.`nome_bairro`) as Quantidade FROM si_log log JOIN si_bol bol ON bol.`NUMERO_BOLETIM` = log.`Nº_boletim` WHERE EXISTS (SELECT * FROM si_env env where env.`Embreagues` = 'SIM' AND bol.`NUMERO_BOLETIM` = env.`num_boletim`) AND  bol.`DESC_TIPO_ACIDENTE` NOT LIKE '%SEM VITIMA%' GROUP BY log.`Nº_bairro`, log.`nome_bairro` ORDER BY log.`nome_bairro`")
dfpandas=df.toPandas()

size = 10
df_dict = {n: dfpandas.iloc[n:n+size, :] for n in range(0, len(dfpandas), size)}

for key in df_dict:
    plt.clf()
    plt.close()
    df_dict[key].plot.barh(y='Quantidade', x='nome_bairro', rot=75, figsize=(12, 12), fontsize=12, title='Número de Acidentes com Vítimas por Bairro', xlabel='Bairros')
    plt.savefig('output/output'+str(key)+'.png')