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

##### O resultado final o fluxo seguiria da seguinte forma

![Solução SQL](questao_1/imagem_1.png)
Figura 1 - Fluxograma das ferramentas

##### Informações importantes

- O tópico do Kafka possui capacidade de suportar milhões de mensagens por dia;
- Possui capacidade de escalabilidade horizontal para garantir a performance;
- Os dados serão transmitidos pelas camadas em tempo real de maneira rápida e eficiente.
- O Spark poderá ser utilizado para ser uma das ferramentas de análises exploratória e visualização dos dados.

##### Questão 2:

Foi criado dois scripts (Python e SQL) para realizar o calculo das faturas retroativas com as regras de negocio apresentadas

![Solução SQL](questao_2/imagem_1.png)
Figura 2 - Saída do programa SQL

![Solução SQL](questao_2/imagem_2.png)
Figura 3 - Saída do programa Python

Passo a passo da solução:

- Filtrar os registros para para listar apenas os registros antes de 2020-01-01
- Salvar os dados no banco de dados (para a solução SQL)
- Contar quantos registros existem entre a data de referência, a data de referência - 3 meses, e a data de referência - 6 meses.
- Atribuir nulo para as os registros que não atingirem a quantidade mínima e calcular a média para os dados com a quantidade minima.
- Construir um novo dataframe para armazenar os dados com customer e account e médias.

##### Questão 3:

Foi criado o script Python e SQL para realizar o calculo das conversas do chatbot com as regras apresentadas.

Passo a passo da solução:

- Fazer a leitura dos dados e fazer a ingesão no banco de dados.
- utilizar a "ROW_NUMBER" para selecionar a ultima mensagem valída do do customer, flow e session.
- Remover os resultros nulos e strings vazias.
- Fazer uma subquery para selecionar a data de incio e fim da conversa para os mesmos customer, flow e session.
- Concatenar e ter a tabela final no formato desejado.
- Processar cada flow individualmente, pois cada um possui uma estrutura de tabela diferente.
- Acessar cada combinação possível de customer, flow, e session para obter todas as conversas.
- Efetuar a leitura das colunas customer, flow e session, first_answer_dt e last_answer_dt que serão iguais devido ao filtro por conversa, obtendo apenas a primeira linha.
- Transpor os registros de key e value, ordenados pela data de envio.
- Concatenar os dados e compor a tabela de saida como apresentada no desafio.

As evidências com a saída solicitada segue abaixo.

![Solução SQL](questao_3/imagem_1.png)
Figura 4 - Saída do programa SQL

![Solução SQL](questao_3/imagem_2.png)
Figura 5 - Saída do programa Python