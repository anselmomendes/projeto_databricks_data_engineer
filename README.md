# Desafio Engenheiro de Dados

##### Autor: Anselmo Oliveira
##### Data: 14/11/2024
##### Desafio: Desenvolver Arquitetura de Dados
##### Questão 1: 
##### Projete uma solução para uma plataforma de comunicação que computa a utilização de notificações Whatsapp de milhares de clientes a cada minuto.

##### Ingestão de Dados com Apache Kafka

Podemos utilizar o Kafka para realizar a ingestão de dados em tempo real, permitindo processamento de alta escala e baixa latência.

##### Processamento em Tempo Real com Databricks e Apache Spark

O Databricks pode ser utilizado para processar os dados do kafka utilizando o Databricks Structured Streaming que implementa os recursos do Apache Spark. Os dados ingeridos serão armazenados no Delta Lake, com janela de agregação de 1 minuto.


##### Armazenamento e Consultas com Delta Lake

O Delta Lake do Databkicks será implementado com uma arquitetura de camadas: Bronze, Prata e Ouro. Os dados ingeridos pelo Databricks será carregado as-is diretamente na camada Bronze.
As camadas seguintes, Prata e Ouro serão responsaveis por realizar o processamento, limpeza para criação do data mart com as informações refinadas para os relatórios.

##### Visualização e Relatórios

A criação dos relatórios irá consumir as tabelas com os calculos refinados dos relatórios detalhados de notificação e Cobrança, que poderá ser realizada por ferramentas como Power BI, Metabase, Superset entre outros.

##### Informações importantes

- O topico do kafka possui capacidade de suportar milhores de mensagens por dia;
- Possui capacidade de escalabilidade horizontal para garantir a performance;
- Os dados serão transmitidos pelas camadas em tempo real de maneira rápida e eficiente.
- O Spark poderá ser utilizado para ser uma das ferramentas de analises exploratória e visualização dos dados.