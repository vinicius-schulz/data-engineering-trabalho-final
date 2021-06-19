# Disciplina: Data Engineering (Big Data & Analitycs)
## Professor: 
* Antonio Claudio Lopes 

## Integrantes:
* Dabla Arévalo Ferreira
* Gabrielle Brito Cadurim
* Larissa Alves da Silva
* Mateus Soares da Silva
* Vinicius Miranda Lopes Schulz

## Avaliação final - Trabalho em grupo
### Decrição
#### Utilizando os arquivos de dados em anexo:

- [Relação de pessoas envolvidas em acidentes de trânsito](https://ckan.pbh.gov.br/dataset/b127c1d8-9e1b-4820-884a-8bd8129ba5e3/resource/903286ca-b77f-44ad-aa6a-958aa018c33f/download/si_env-2019.csv)
- [Relação de logradouros dos locais de acidentes de trânsito](https://ckan.pbh.gov.br/dataset/a07412b3-8371-402a-8ca6-68a518bf2403/resource/ad490dd1-7af5-4868-b35f-9471f15be744/download/si-log-2019.csv)
- [Relação de ocorrências de trânsito](https://ckan.pbh.gov.br/dataset/6511cb66-3635-4560-95cc-d0c39aafb547/resource/ab6db535-b706-4e7b-9fdc-3bc1e823401e/download/si-bol-2019.csv)

#### Construa uma solução de big data com os seguintes requisitos:

1) Gráficos:
- Total de acidentes com vítima por bairro em acidentes com embriaguez;
- Total de acidentes por tipo de pavimento e condição do tempo;
- Total de pessoas acidentadas por tipo de veiculo e tipo de pavimentação;
- Média de idade dos condutores por tipo de veículo e tipo de acidente;
- Média de idade dos condutores por indicativo de embriaguez;

2) Ser implementada utilizando os recursos apresentados na disciplina;

#### A entrega deve conter:
- Os códigos fonte;
- Documento pdf com os gráficos.

## Provisionando projeto vagrant para construção da máquina virtual com o Hadoop

### Pré-requisitos:

- Pelo menos 4Gb RAM
- Vagrant - https://www.vagrantup.com/downloads 
- Virtual Box - https://www.virtualbox.org/wiki/Downloads ou Docker - https://docs.docker.com/engine/install/
- Git - https://git-scm.com/downloads
- Python 3.7 - Deve ser instalado na VM (passo-a-passo abaixo)
- pip3 - Deve ser instalado na VM (passo-a-passo abaixo)

### Passos para iniciar a máquina:

1) Abra o prompt de comando (se o SO do host for windows, abra como administrador) e digite: 

`git clone https://github.com/vinicius-schulz/data-engineering-trabalho-final.git`

2) Navegue até o diretório do projeto pelo usando o terminal de sua preferência

3) Digite o comando para provisionar a VM usando o vagrant: 

`vagrant up --provider=virtualbox`

ou (para máquina linux usando docker)

`vagrant up --provider=docker` 

4) Aguarde até o final do processo de provisionamento (deve demorar mais de 1h, dependendo da sua conexão com a internet)

5) Editar o arquivo hosts (Windows - C:\Windows\System32\drivers\etc\hosts) ou (Linux - /etc/hosts) adicionando a linha abaixo

`10.211.55.101 node1`

6) Para verificar se tudo está funcionando corretamente, utilize os links abaixo:

- YARN resource manager: (http://node1:8088)
- HBase: (http://node1:16010)
- HDFS: (http://node1:50070/dfshealth.html)
- Spark history server: (http://node1:18080)

## Criando estrutura de diretórios na VM e HDFS e cópia de arquivos

**Faça o upload dos arquivos si_env-2019.csv, si-bol-2019.csv e si-log-2019.csv para a VM**

`vagrant upload "si_env-2019.csv" /home/vagrant/si_env.csv`

`vagrant upload "si-bol-2019.csv" /home/vagrant/si_bol.csv`

`vagrant upload "si-log-2019.csv" /home/vagrant/si_log.csv`

**Conecte-se à VM usando o comando abaixo**

`vagrant ssh`

**Crie o diretorio output na pasta /home/vagrant/ da VM**

`mkdir /home/vagrant/output`

**Crie a estrutura de pastas no HDFS**

`hdfs dfs -mkdir /user/vagrant/`

`hdfs dfs -mkdir /user/vagrant/env`

`hdfs dfs -mkdir /user/vagrant/bol`

`hdfs dfs -mkdir /user/vagrant/log`

`hdfs dfs -mkdir /user/vagrant/output`

**Envie os arquivos da VM para o HDFS**

`hdfs dfs -put /home/vagrant/si_env.csv /user/vagrant/env/`

`hdfs dfs -put /home/vagrant/si_bol.csv /user/vagrant/bol/`

`hdfs dfs -put /home/vagrant/si_log.csv /user/vagrant/log/`

## Instalar Python3.7 e Libs Pandas e Matplotlib

**Adicione o repositório com o Python3.7**

`sudo add-apt-repository ppa:deadsnakes/ppa`

**Atualize os pacotes**

`sudo apt-get update`

**Instale o Python3.7 (necessário instalar o Python3.6 antes)**

`sudo apt-get install python3.6`

`sudo apt-get install python3.7`

**Instale o PIP**

`sudo apt install python3-pip`

**Rode os comandos abaixo setar a prioridades de uso do Python**

`sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 1`

`sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2`

**Inicializar o python3.7 como padrão na inicialização do sistema**

`nano ~/.bashrc.`

**Adicione as linhas abaixo ao final do arquivo aberto no nano para trocar a versão padrão do python do spark para o python3. Salve o arquivo após a edição.**

```console
alias python=python3
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

**Escreva o comando abaixo para recarregar o .bashrc**

`source ~/.bashrc`

**Instale as libs pandas e matplotlib**

`pip3 install pandas`

`pip3 install matplotlib`

## Inicializando o HIVE e criando tabelas

**A partir da linha de comando da VM, inicie o HIVE com o comando abaixo**

`hive`

**Crie um database**

`CREATE DATABASE gpdb;`

**Conecte-se ao database criado**

`USE gpdb;`

**Crie as EXTERNAL TABLE**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS si_env(`num_boletim` STRING, `data_hora_boletim` STRING, `Nº_envolvido` STRING, `condutor` STRING, `cod_severidade` STRING, `desc_severidade` STRING, `sexo` STRING, `cinto_seguranca` STRING, `Embreagues` STRING, `Idade` STRING, `nascimento` STRING, `categoria_habilitacao` STRING, `descricao_habilitacao` STRING, `declaracao_obito` STRING, `cod_severidade_antiga` STRING, `especie_veiculo` STRING, `pedestre` STRING, `passageiro` STRING)
COMMENT 'TABELA SI_ENV'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/vagrant/env/';

CREATE EXTERNAL TABLE IF NOT EXISTS si_bol(`NUMERO_BOLETIM` STRING, `DATA HORA_BOLETIM` STRING, `DATA_INCLUSAO` STRING, `TIPO_ACIDENTE` STRING, `DESC_TIPO_ACIDENTE` STRING, `COD_TEMPO` STRING, `DESC_TEMPO` STRING, `COD_PAVIMENTO` STRING, `PAVIMENTO` STRING, `COD_REGIONAL` STRING, `DESC_REGIONAL` STRING, `ORIGEM_BOLETIM` STRING, `LOCAL_SINALIZADO` STRING, `VELOCIDADE_PERMITIDA` STRING, `COORDENADA_X` STRING, `COORDENADA_Y` STRING, `HORA_INFORMADA` STRING, `INDICADOR_FATALIDADE` STRING, `VALOR_UPS` STRING, `DESCRIÇÃO_UPS` STRING, `DATA_ALTERACAO_SMSA` STRING, `VALOR_UPS_ANTIGA` STRING, `DESCRIÇÃO_UPS_ANTIGA` STRING)
COMMENT 'TABELA SI_BOL'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/vagrant/bol/';

CREATE EXTERNAL TABLE IF NOT EXISTS si_log(`Nº_boletim` STRING, `data_boletim` STRING, `Nº_municipio` STRING, `nome_municipio` STRING, `seq_logradouros` STRING, `Nº_logradouro` STRING, `tipo_logradouro` STRING, `nome_logradouro` STRING, `tipo_logradouro_anterior` STRING, `nome_logradouro_anterior` STRING, `Nº_bairro` STRING, `nome_bairro` STRING, `tipo_bairro` STRING, `descricao_tipo_bairro` STRING, `Nº_imovel` STRING, `Nº_imovel_proximo` STRING)
COMMENT 'TABELA SI_LOG'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/vagrant/log/';
```

## Geração dos resultados usando pyspark

### Total de acidentes com vítima por bairro em acidentes com embriaguez;

**Execute o comando pyspark**

`pyspark`

**Execute o código python abaixo dentro na linha de comando do pyspark**

```python
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
```

**Após a execução e geração dos arquivos de gráfico saida do console do pyspark apertando Ctrl+D**

### Total de acidentes por tipo de pavimento e condição do tempo;

FALTA INCLUIR

### Total de pessoas acidentadas por tipo de veiculo e tipo de pavimentação;

FALTA INCLUIR

### Média de idade dos condutores por tipo de veículo e tipo de acidente;

FALTA INCLUIR

### Média de idade dos condutores por indicativo de embriaguez;

FALTA INCLUIR

## Enviar arquivos gerados para o HDFS

1) Execute o comando abaixo para copiar as imagens geradas para o HDFS

`hdfs dfs -put -f /home/vagrant/output/ /user/vagrant/output/`

2) Os arquivos de imagem serão disponibilizados no diretorio  '/user/vagrant/output/'. Será possível acessar o mesmo por meio do link

[http://node1:50070/explorer.html#/user/vagrant/output](http://node1:50070/explorer.html#/user/vagrant/output)

## Parar a máquina virtual

**Pelo linha de comando digite e aguarde o encerramento da VM**

`vagrant halt`

## Créditos
[Antonio Claudio Lopes](https://github.com/aclaraujo/vagrant-hadoop-hive-spark)