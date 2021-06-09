# Data Engineering (Big Data & Analitycs)
# Professor: Antonio Claudio Lopes 
# Aluno: Vinicius Miranda Lopes Schulz

## Atividade 3 - HIVE
### Decrição
Utilizando o arquivo em anexo (professores.csv), crie as tabelas no hive e execute a seguinte consulta SQL: Total de professores por lotação. Conteúdo do arquivo: (matricula;nome;turno;carga_horaria;lotacao;atividade)

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