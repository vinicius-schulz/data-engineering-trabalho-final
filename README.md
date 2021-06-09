# Disciplina: Data Engineering (Big Data & Analitycs)
# Professor: Antonio Claudio Lopes 
# Integrantes:
* Dabla Arévalo Ferreira
* Gabrielle Brito Cadurim
* Larissa Alves da Silva
* Mateus Moreira Santana
* Vinicius Miranda Lopes Schulz

## Avaliação final - Trabalho em grupo
### Decrição
#### Utilizando os arquivos de dados em anexo:

[Relação de pessoas envolvidas em acidentes de trânsito](https://ckan.pbh.gov.br/dataset/b127c1d8-9e1b-4820-884a-8bd8129ba5e3/resource/903286ca-b77f-44ad-aa6a-958aa018c33f/download/si_env-2019.csv)
[Relação de logradouros dos locais de acidentes de trânsito](https://ckan.pbh.gov.br/dataset/a07412b3-8371-402a-8ca6-68a518bf2403/resource/ad490dd1-7af5-4868-b35f-9471f15be744/download/si-log-2019.csv)
[Relação de ocorrências de trânsito](https://ckan.pbh.gov.br/dataset/6511cb66-3635-4560-95cc-d0c39aafb547/resource/ab6db535-b706-4e7b-9fdc-3bc1e823401e/download/si-bol-2019.csv)

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

- Ao menos 4Gb RAM
- Vagrant - https://www.vagrantup.com/downloads 
- Virtual Box - https://www.virtualbox.org/wiki/Downloads ou Docker - https://docs.docker.com/engine/install/
- Git - https://git-scm.com/downloads

### Passos para iniciar a máquina:

1) Abra o prompt de comando (se o SO do host for windows, abra como administrador) e digite: 

`git clone https://github.com/vinicius-schulz/hive_hadoop.git`

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

## Criando estrutura de diretórios e cópia de arquivo

Conecte-se à VM usando o comando abaixo

`vagrant ssh`

Faça o upload do arquivo professores.csv para a VM provisionada pelo Vagrant

`vagrant upload "professores.csv" /home/vagrant/professor.csv`

Crie uma pasta chamada 'vagrant' em /users/ dentro da estrutura do hdfs

`hdfs dfs -mkdir /user/vagrant/`

Crie uma pasta chamada 'professor' em /users/vagrant/ dentro da estrutura do hdfs

`hdfs dfs -mkdir /user/vagrant/professor`

Crie uma pasta chamada 'result' em /users/vagrant/ dentro da estrutura do hdfs (esta pasta conterá os resultados)

`hdfs dfs -mkdir /user/vagrant/result`

Copie o arquito professor.csv da VM para dentro do hdfs

`hdfs dfs -put /home/vagrant/professor.csv /user/vagrant/professor/`

## Inicializando o HIVE e criando tabelas

A partir da linha de comando da VM, inicie o HIVE com o comando abaixo

`hive`

Crie um database

`CREATE DATABASE professorDB;`

Conecte-se ao database criado

`USE professorDB;`

Crie a EXTERNAL TABLE 

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS professor(matricula STRING,nome STRING,turno STRING,carga_horaria STRING,lotacao STRING,atividade STRING)
COMMENT 'Tabela de Professores'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/vagrant/professor/';
```

## Execução de comandos no HIVE e visualização de resultados

1) Execute o comando abaixo para agrupar os professores por lotação e armazenar o resultado no diretório '/user/vagrant/result/'

```sql
INSERT OVERWRITE DIRECTORY '/user/vagrant/result/' SELECT lotacao, COUNT(*) FROM professor GROUP BY lotacao;
```

2) Será criado um arquivo contendo os resultados no diretório '/user/vagrant/result/' com o nome 000000_0. Será possível acessar o mesmo por meio do link 

[http://node1:50070/explorer.html#/user/vagrant/result](http://node1:50070/explorer.html#/user/vagrant/result)

3) Alternativamente, já disponibilizei o arquivo gerado no diretório do projeto com o nome 'Resultados.txt'

## Parar a máquina virtual

Pelo linha de comando digite e aguarde o encerramento da VM

`vagrant halt`

## Créditos
[Antonio Claudio Lopes](https://github.com/aclaraujo/vagrant-hadoop-hive-spark)