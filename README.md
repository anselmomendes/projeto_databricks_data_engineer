# Desafio Engenheiro de Dados

##### Autor: Anselmo Oliveira
##### Data: 14/11/2024
##### Desafio: Desenvolver Arquitetura de Dados
##### Questão 1: 
##### Projete uma solução para uma plataforma de comunicação que computa a utilização de notificações Whatsapp de milhares de clientes a cada minuto.

##### Ingestão de Dados com Apache Kafka

Podemos utilizar o Kafka para realizar a ingestão de dados em tempo real, permitindo processamento de alta escala e baixa latência.

##### Processamento em Tempo Real com Databricks e Apache Spark

O Databricks pode ser utilizado para processar os dados do Kafka utilizando o Databricks Structured Streaming que implementa os recursos do Apache Spark. Os dados ingeridos serão armazenados no Delta Lake, com janela de agregação de 1 minuto.

##### Armazenamento e Consultas com Delta Lake

O Delta Lake do Databkicks será implementado com uma arquitetura de camadas: Bronze, Prata e Ouro. Os dados ingeridos pelo Databricks serão carregado as-is diretamente na camada Bronze.
As camadas seguintes, Prata e Ouro serão responsáveis por realizar o processamento, limpeza para criação do data Mart com as informações refinadas para os relatórios.

##### Visualização e Relatórios

A criação dos relatórios irá consumir as tabelas com os cálculos refinados dos relatórios detalhados de notificação e Cobrança, que poderá ser realizada por ferramentas como Power BI, Metabase, Superset entre outros.

##### Informações importantes

- O tópico do Kafka possui capacidade de suportar milhões de mensagens por dia;
- Possui capacidade de escalabilidade horizontal para garantir a performance;
- Os dados serão transmitidos pelas camadas em tempo real de maneira rápida e eficiente.
- O Spark poderá ser utilizado para ser uma das ferramentas de análises exploratória e visualização dos dados.

##### Questão 2:

Foi criado dois scripts (Python e SQL) para realizar o calculo das faturas retroativas com as regras de negocio apresentadas

![Solução SQL](questao_2/imagem_1.png)
Figura 1 - Saída do programa SQL

![Solução SQL](questao_2/imagem_2.png)
Figura 2 - Saída do programa Python

##### Questão 3:

Foi criado o script Python e SQL para realizar o calculo das conversas do chatbot com as regras apresentadas.

As evidências com a saída solicitada segue abaixo.

![Solução SQL](questao_3/imagem_1.png)
Figura 1 - Saída do programa SQL

![Solução SQL](questao_3/imagem_2.png)
Figura 2 - Saída do programa Python