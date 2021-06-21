from pyspark.sql import SparkSession
from pyspark.sql import Row
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("Trabalho final - Big Data").enableHiveSupport().getOrCreate()
spark.sql("use gpdb")

#Total de acidentes com vítima por bairro em acidentes com embriaguez;

df = spark.sql("SELECT trim(log.`nome_bairro`) as nome_bairro, COUNT(log.`nome_bairro`) as Quantidade FROM si_log log JOIN si_bol bol ON bol.`NUMERO_BOLETIM` = log.`Nº_boletim` WHERE EXISTS (SELECT * FROM si_env env where env.`Embreagues` = 'SIM' AND bol.`NUMERO_BOLETIM` = env.`num_boletim`) AND  bol.`DESC_TIPO_ACIDENTE` NOT LIKE '%SEM VITIMA%' GROUP BY log.`Nº_bairro`, log.`nome_bairro` ORDER BY log.`nome_bairro`")
dfpandas=df.toPandas()
size = 10
df_dict = {n: dfpandas.iloc[n:n+size, :] for n in range(0, len(dfpandas), size)}

for key in df_dict:
    plt.clf()
    plt.close()
    df_dict[key].plot.barh(y='Quantidade', x='nome_bairro', rot=75, figsize=(12, 12), fontsize=12, title='Número de Acidentes com Vítimas por Bairro', xlabel='Bairros')
    plt.savefig('output/acidentevitimabairroembreaguez'+str(key)+'.png')

#Total de acidentes por tipo de pavimento e condição do tempo;

df = spark.sql(" SELECT CONCAT_WS(\" e \", trim(bol.DESC_TIPO_ACIDENTE), trim(bol.DESC_TEMPO)) as description, COUNT(bol.`PAVIMENTO`) as Pavimento FROM  si_bol bol GROUP BY bol.`DESC_TIPO_ACIDENTE`, bol.`DESC_TEMPO`")
dfpandas=df.toPandas()
plt.clf()
plt.close()
dfpandas.plot.barh(x="description", figsize=(30, 30), fontsize=12, title='Total de acidentes por tipo de pavimento e condição do tempo')
plt.savefig('output/acidentepavimentotempo.png')

#Total de pessoas acidentadas por tipo de veiculo e tipo de pavimentação;

df = spark.sql("SELECT CONCAT_WS(\" e \", trim(env.especie_veiculo), trim(bol.PAVIMENTO)) as description, count(*) as amount FROM si_log log JOIN si_bol bol ON bol.`NUMERO_BOLETIM` = log.`Nº_boletim` JOIN si_env env ON env.`num_boletim` = log.`Nº_boletim` GROUP BY env.especie_veiculo, bol.PAVIMENTO")
dfpandas=df.toPandas()

plt.clf()
plt.close()
dfpandas.plot.barh(x="description", figsize=(30, 30), fontsize=12, title='Total de pessoas acidentadas por tipo de veiculo e tipo de pavimentação')
plt.savefig('output/acidentesveiculopavimentacao.png')

#Média de idade dos condutores por tipo de veículo e tipo de acidente;

df = spark.sql("SELECT CONCAT_WS(\" e \", trim(env.especie_veiculo), trim(bol.DESC_TIPO_ACIDENTE)) as Tipo_Acidente, AVG(env.`Idade`)AS Media_Idade FROM si_log log JOIN si_bol bol ON bol.`NUMERO_BOLETIM` = log.`Nº_boletim` JOIN si_env env ON env.`num_boletim` = log.`Nº_boletim` GROUP BY env.especie_veiculo, bol.DESC_TIPO_ACIDENTE")
dfpandas=df.toPandas()
size = 10
df_dict = {n: dfpandas.iloc[n:n+size, :] for n in range(0, len(dfpandas), size)}
for dabla in df_dict:
    plt.clf()
    plt.close()
    df_dict[dabla].plot.barh(y='Media_Idade', x='Tipo_Acidente',rot=75, figsize=(12, 12), fontsize=12, title='Média de idade dos condutores por tipo de veículo e tipo de acidente', xlabel='Média')
    plt.savefig('output/idadeveiculotipoacidente'+str(dabla)+'.png')

#Média de idade dos condutores por indicativo de embriaguez;

df = spark.sql("SELECT CONCAT_WS(\" e \", trim(env.idade), trim(env.`Embreagues`)) as Embreagues, COUNT(env.`Idade`) as Idade FROM si_env env GROUP BY env.`Idade`, env.`Embreagues`")
dfpandas=df.toPandas()
size = 10
df_dict = {n: dfpandas.iloc[n:n+size, :] for n in range(0, len(dfpandas), size)}

for key in df_dict:
    plt.clf()
    plt.close()
    df_dict[key].plot.barh(y='Quantidade', x='nome_bairro', rot=75, figsize=(12, 12), fontsize=12, title='Número de Acidentes com Vítimas por Bairro', xlabel='Bairros')
    plt.savefig('output/idadeembriaguez'+str(key)+'.png')