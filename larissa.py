from pyspark.sql import SparkSession
from pyspark.sql import Row
import matplotlib.pyplot as plt
spark = SparkSession.builder.appName("Total de acidentes por tipo de pavimento e condição do tempo;").enableHiveSupport().getOrCreate()
spark.sql("use gpdb")
df = spark.sql(" SELECT CONCAT_WS(\" e \", trim(bol.DESC_TIPO_ACIDENTE), trim(bol.DESC_TEMPO)) as description, COUNT(bol.`PAVIMENTO`) as Pavimento FROM  si_bol bol GROUP BY bol.`DESC_TIPO_ACIDENTE`, bol.`DESC_TEMPO`")
dfpandas=df.toPandas()
plt.clf()
plt.close()
dfpandas.plot.barh(x="description", figsize=(30, 30), fontsize=12, title='Total de acidentes por tipo de pavimento e condição do tempo')
plt.savefig('output/output.png')