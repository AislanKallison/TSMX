TSMX - Desafio Prático: Importação, Validação e Implementação de Dados
📋 Descrição do Projeto
Este repositório contém a implementação de um desafio prático para a posição de Analista de Dados na TSMX. O objetivo é desenvolver um pipeline em Python para:

Importar dados de um arquivo Excel (dados_importacao.xlsx) para um banco de dados PostgreSQL.
Validar dados tratando inconsistências, como dados ausentes, variações de formato e duplicatas baseadas em CPF/CNPJ.
Garantir unicidade de cadastros de clientes, associando entradas duplicadas ao registro existente.
Gerar relatórios com resumo de registros importados/não importados, incluindo motivos de falha, exportados para Excel e TXT.

O projeto foi desenvolvido como parte de uma entrevista prática e demonstra habilidades em manipulação de dados com Pandas, conexão com PostgreSQL via psycopg2, e automação de validações.
Repositório GitHub: https://github.com/AislanKallison/TSMX
🛠️ Ambiente e Requisitos
Configuração do Ambiente

Banco de Dados: PostgreSQL (versão 17.0) com pgAdmin 4 para gerenciamento.
Linguagem: Python 3.9.
Bibliotecas Principais:
pandas: Manipulação e análise de dados.
psycopg2: Conexão com o PostgreSQL.
openpyxl: Leitura/escrita de arquivos Excel.

Sistema Operacional: Windows 11 (testado; compatível com outros SOs via adaptações).

Instalação de Dependências
Crie um ambiente virtual e instale as bibliotecas necessárias:
Bashpython -m venv venv
# Ative o ambiente virtual (Windows)
venv\Scripts\activate
# Instale as dependências
pip install pandas psycopg2-binary openpyxl
Configuração do Banco de Dados

Instale e configure o PostgreSQL 17.0.
Use o pgAdmin 4 para restaurar o schema do banco:
Execute o script schema_database_pgsql.sql para criar as tabelas necessárias (ex.: clientes, transações, etc., conforme o schema).

Atualize as credenciais de conexão no script import_data.py (variáveis DB_HOST, DB_NAME, DB_USER, DB_PASSWORD).

📁 Estrutura de Arquivos

Arquivo/FolderDescriçãoREADME.mdEste arquivo: documentação do projeto.dados_importacao.xlsxArquivo de entrada com dados brutos para importação (clientes, CPFs/CNPJs, etc.).validador_de_dados.pyScript responsável pela validação de inconsistências (dados ausentes, formatos inválidos) e tratamento de duplicatas. Gera logs em TXT e relatórios em Excel.import_data.pyScript principal para leitura do Excel, conexão com o BD e inserção de dados validados.schema_database_pgsql.sqlScript SQL para criação do schema do banco de dados (tabelas, chaves primárias, etc.).LEIA-ME/Pasta com arquivos auxiliares, como logs de validação (data_validation.log) e imagens de testes.
🚀 Como Executar
1. Preparação

Certifique-se de que o PostgreSQL está rodando e o schema foi restaurado via schema_database_pgsql.sql.
Coloque o arquivo dados_importacao.xlsx na raiz do projeto.

2. Execução dos Scripts
Os scripts podem ser executados sequencialmente ou integrados. Recomenda-se rodar na ordem:
a) Validação de Dados
Bashpython validador_de_dados.py

Saídas:
Arquivos Excel e TXT na pasta downloads/ com:
Registros validados e importáveis.
Lista de não importados com motivos (ex.: CPF inválido, dados ausentes).

Log de validação: data_validation.log (gerado automaticamente).

Tratamentos Implementados:
Verificação de formatos (ex.: CPF/CNPJ válidos).
Preenchimento de dados ausentes com valores padrão ou remoção.
Detecção e associação de duplicatas por CPF/CNPJ (unicidade garantida).


b) Importação de Dados
Bashpython import_data.py

Saídas:
Dados validados inseridos nas tabelas do PostgreSQL.
Relatório de importação: Total de registros processados, importados e rejeitados.
Arquivos Excel/TXT na pasta downloads/ para resumo imediato.


Exemplo de Saída de Relatório

Importados: 150 registros (com detalhes em Excel).
Não Importados: 10 registros (motivos: "CPF inválido", "Dados ausentes em campo obrigatório").

🧪 Testes Realizados
O projeto foi testado em ambiente local com os seguintes resultados:

Conexão com BD: Schema restaurado com sucesso (ver imagens em LEIA-ME/ para layout das tabelas).
Importação: Leitura completa do Excel e inserção sem erros.
Validação: 100% de cobertura para inconsistências; duplicatas associadas corretamente.
Relatórios: Geração automática de arquivos para fácil visualização.
