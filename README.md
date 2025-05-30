# MC536_Projeto2

### Grupo:
```
Nome-----------------------------                        RA------
Gabriel Cunha Marchetti                                  (251055)
Felipe Scalabrin Dosso                                   (236110)
José Maurício Vasconcellos Junior                        (219255)
```
### Cenário:
Você foi contratado para reformular um sistema de consulta a dados altamente estruturados. As principais operações consistem em realizar análises estatísticas sobre grandes volumes de dados históricos e imutáveis. As consultas acessam frequentemente um número pequeno de atributos, mas um número grande de registros. O sistema é utilizado por analistas de dados que preferem uma integração direta com linguagens como Python ou R.

Requisitos Técnicos:
-Predominância de operações de leitura e agregação sobre grandes datasets.
-Alta compressão e performance em operações de leitura.
-Baixa frequência de escrita ou atualização.
-Integração com notebooks ou scripts de análise.
-Confiabilidade em leituras, mas sem exigência de controle transacional complexo.

### Justificativa uso do Banco de Dados.
O grupo escolheu trabalhar com o **DuckDB**, pois se tratando de gerar relatórios estatísticos da base de dados de grande volume não precisaremos realizar nenhuma (ou poucas) operações de inserção e muitas operações de leitura. No nosso projeto ainda precisaremos de algum grau de normalização, portanto, conseguimos reduzir a quantidade de tabelas para três tabelas, em contraste ao primeiro projeto, que possuia 9 tabelas. Além disso, como as consultas terão poucos atributos podemos assumir que haverá pouca complexidade dos JOINS mas com alto volume de dados. 

A forma de armazenamento do **DuckDB** é muito útil para esse tipo de tarefa, pois se tratando de extrair queries para analytics é necessário ter processamento acelerado de altas porções dos dados dentro do banco. Através da documentação (https://duckdb.org/why_duckdb#fast) podemos perceber que ele atinge isso através da redução do número de ciclos que a CPU usa para puxar os dados do banco de dados. Ele consegue reduzir esse ciclos armazenando os dados em formato colunar e, portanto, se a CPU conseguir suportar operações de vetores, então puxamos altos dados em um ciclo. Note, por exemplo, que se quisermos pegar o atributo de um dados em um banco relacional tradicional, então teríamos que iterar sobre todas as linhas, enquanto aqui realizamos a operação sobre as colunas desejadas. 

Sobre a linguagem e processamento das consultas o **DuckDB** oferece uma interface com consultas SQL além de oferecer suporte tanto para features novas e quanto suporte para comunidade para a linguagem R e Python como informado em (https://duckdb.org/docs/stable/clients/overview.html). 

Além disso, o processamento e controle de transações seguem as propriedades ACID e podem ser armazenadas de forma persistente em banco de dados single-file. Garantindo alta consistência dos dados e uma facilidade na manutenção do banco.

Ainda por cima temos que o mecanismo de recuperação do **DuckDB** nos permite criar estados dentro do nosso banco para que possamos tanto realizar o ROLLBACK quanto um COMMIT de estado. Contudo, por padrão as queries são isoladas das outras queries, portanto, precisamos tornar clara a transação quando queremos que ela crie um novo estado do banco.

Por fim, a segurança do **DuckDB** é variável e pode ser configurada conforme o usuário desejar. Note que essa facilidade de configuração ocorre porque muitas vezes podemos gerar um relatório de modo local em vez de utilizar uma máquina na rede. Mas isso apresenta certos perigos como: Para termos controle sobre SQL injection precisamos que haja uma camada de controle para quais consultas irão ser executadas, isso cria a burocracia de não podemos usar a engine de consultas a nossa vontade. Portanto, precisamos da camada da API das linguagens para executar consultas SQL seguras. Além disso, para proteger dados sensíveis há o controle de acesso de arquivos.


### Esquema Lógico do Projeto:
<p align="center">
  <img src="EsquemaLogico.svg" alt="Modelo Lógico do Projeto" width="700"/>
</p>
