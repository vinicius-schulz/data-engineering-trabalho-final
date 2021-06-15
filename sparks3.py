from pyspark.sql import SparkSession
from pyspark.sql import Row
import matplotlib.pyplot as plt
spark = SparkSession.builder.appName("Total de pessoas acidentadas por tipo de veiculo e tipo de pavimentação").enableHiveSupport().getOrCreate()
spark.sql("use gpdb")
df = spark.sql("SELECT CONCAT_WS(\" e \", trim(env.especie_veiculo), trim(bol.PAVIMENTO)) as description, count(*) as amount FROM si_log log JOIN si_bol bol ON bol.`NUMERO_BOLETIM` = log.`Nº_boletim` JOIN si_env env ON env.`num_boletim` = log.`Nº_boletim` GROUP BY env.especie_veiculo, bol.PAVIMENTO")
dfpandas=df.toPandas()

plt.clf()
plt.close()
dfpandas.plot.barh(x="description", figsize=(30, 30), fontsize=12, title='Total de pessoas acidentadas por tipo de veiculo e tipo de pavimentação')
plt.savefig('output/output.png')