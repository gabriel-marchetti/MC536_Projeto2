import duckdb
import pandas as pd
import numpy as np
import re

# --- Configuração ---
PATH_DADOS = 'data/'
ARQUIVOS_ENADE = [
    f'{PATH_DADOS}conceito_enade_2021.csv',
    f'{PATH_DADOS}conceito_enade_2022.csv',
    f'{PATH_DADOS}conceito_enade_2023.csv'
]
ARQUIVO_IDEB = f'{PATH_DADOS}ideb_saeb_2017_2019_2021_2023.csv'
DB_FILE = 'database.duckdb'

def criar_tabelas(con):
    """
    Cria as tabelas Municipio, Escola e Curso no banco de dados DuckDB
    usando exatamente o schema fornecido pelo usuário.
    """
    # Usa 'DROP TABLE IF EXISTS' para garantir que o script possa ser executado várias vezes
    con.execute("DROP TABLE IF EXISTS Curso;")
    con.execute("DROP TABLE IF EXISTS Escola;")
    con.execute("DROP TABLE IF EXISTS Municipio;")
    
    # Criação da tabela Municipio
    con.execute('''
        CREATE TABLE Municipio (
            SIGLA_UF VARCHAR(2),
            NOME_UF VARCHAR(50),
            NOME_MUNICIPIO VARCHAR(100),
            QUANTIDADE_IES INTEGER,
            QUANTIDADE_ESCOLA INTEGER,
            PRIMARY KEY (SIGLA_UF, NOME_MUNICIPIO)
        );
    ''')

    # Criação da tabela Escola
    con.execute('''
        CREATE TABLE Escola (
            NOME_ESCOLA VARCHAR(200),
            CODIGO_ESCOLA VARCHAR(20),
            REDE_ESCOLA VARCHAR(20),
            ANO_ESCOLA INTEGER,
            IDEB_REND1 DOUBLE,
            IDEB_REND2 DOUBLE,
            IDEB_REND3 DOUBLE,
            IDEB_REND4 DOUBLE,
            IDEB_REND_MEDIO DOUBLE,
            IDEB_NOTA DOUBLE,
            SAEB_NOTA_MAT DOUBLE,
            SAEB_NOTA_PORT DOUBLE,
            SAEB_NOTA_PADRAO DOUBLE,
            SIGLA_UF VARCHAR(2),
            NOME_MUNICIPIO VARCHAR(100),
            PRIMARY KEY (CODIGO_ESCOLA, ANO_ESCOLA),
            FOREIGN KEY (SIGLA_UF, NOME_MUNICIPIO) REFERENCES Municipio(SIGLA_UF, NOME_MUNICIPIO)
        );
    ''')
    
    # Criação da tabela Curso
    con.execute('''
        CREATE TABLE Curso (
            NOME_IES VARCHAR(200),
            SIGLA_IES VARCHAR(20),
            CODIGO_IES VARCHAR(20),
            NOME_CURSO VARCHAR(200),
            TOTAL_INSCRITOS INTEGER,
            TOTAL_CONCLUINTES INTEGER,
            NOTA_BRUTA_CE DOUBLE,
            NOTA_PADRONIZADA_CE DOUBLE,
            NOTA_BRUTA_FG DOUBLE,
            NOTA_PADRONIZADA_FG DOUBLE,
            NOTA_ENADE_CONTINUA DOUBLE,
            NOTA_ENADE_FAIXA INTEGER,
            ANO_ENADE INTEGER,
            SIGLA_UF VARCHAR(2),
            NOME_MUNICIPIO VARCHAR(100),
            PRIMARY KEY (CODIGO_IES, NOME_CURSO, ANO_ENADE),
            FOREIGN KEY (SIGLA_UF, NOME_MUNICIPIO) REFERENCES Municipio(SIGLA_UF, NOME_MUNICIPIO)
        );
    ''')

def str_to_float(value):
    """Função auxiliar para converter uma série de strings com vírgula para float."""
    if isinstance(value, pd.Series):
        return pd.to_numeric(value.astype(str).str.replace(',', '.', regex=False), errors='coerce')
    return pd.to_numeric(value, errors='coerce')

def find_column_name(columns, expected_name):
    """
    Encontra o nome real de uma coluna em uma lista de colunas,
    ignorando maiúsculas/minúsculas, espaços e hífens/sublinhados.
    """
    normalized_expected = re.sub(r'[\s_-]', '', expected_name).lower()
    for col in columns:
        normalized_col = re.sub(r'[\s_-]', '', col).lower()
        if normalized_expected == normalized_col:
            return col
    # Retorna None se não encontrar para permitir tratamento de erro
    return None

def carregar_dados(con):
    """
    Orquestra o carregamento, transformação e inserção dos dados nas tabelas.
    """

    # --- 1. Preparar Dados do IDEB/SAEB (para tabela Escola) ---
    df_ideb_raw = pd.read_csv(
        ARQUIVO_IDEB, 
        sep=',', 
        on_bad_lines='skip', 
        dtype={'Código da Escola': str}, 
        encoding='latin1',
        low_memory=False
    )
    
    df_ideb_raw = df_ideb_raw.iloc[:-14]
    df_ideb_raw.columns = df_ideb_raw.columns.str.strip()

    anos_ideb = [2017, 2019, 2021, 2023]
    lista_df_anos = []

    base_cols_map = {
        'CODIGO_ESCOLA': find_column_name(df_ideb_raw.columns, 'Código da Escola'),
        'NOME_ESCOLA': find_column_name(df_ideb_raw.columns, 'Nome da Escola'),
        'REDE_ESCOLA': find_column_name(df_ideb_raw.columns, 'Rede'),
        'SIGLA_UF': find_column_name(df_ideb_raw.columns, 'Sigla da UF'),
        'NOME_MUNICIPIO': find_column_name(df_ideb_raw.columns, 'Nome do Município')
    }
    if any(v is None for v in base_cols_map.values()):
        raise ValueError(f"Uma ou mais colunas base não foram encontradas no arquivo IDEB. Verifique os nomes: {base_cols_map}")

    for ano in anos_ideb:
        df_ano = df_ideb_raw[list(base_cols_map.values())].copy()
        df_ano['ANO_ESCOLA'] = ano
        
        try:
            df_ano['IDEB_NOTA'] = str_to_float(df_ideb_raw[f'Nota_ideb_{ano}'])
            df_ano['SAEB_NOTA_MAT'] = str_to_float(df_ideb_raw[f'Nota_SAEB_{ano}_Mat'])
            df_ano['SAEB_NOTA_PORT'] = str_to_float(df_ideb_raw[f'Nota_SAEB_{ano}_Port'])
            df_ano['SAEB_NOTA_PADRAO'] = str_to_float(df_ideb_raw[f'Nota_padronizada_SAEB_{ano}'])
        except KeyError as e:
            continue

        df_ano.dropna(subset=['IDEB_NOTA', 'SAEB_NOTA_MAT', 'SAEB_NOTA_PORT', 'SAEB_NOTA_PADRAO'], how='all', inplace=True)
        
        if not df_ano.empty:
            lista_df_anos.append(df_ano)

    if not lista_df_anos:
        raise ValueError("Nenhum dado válido do IDEB/SAEB foi encontrado após a transformação. Verifique o arquivo CSV.")

    df_escola_final = pd.concat(lista_df_anos, ignore_index=True)
    df_escola_final.rename(columns={v: k for k, v in base_cols_map.items()}, inplace=True)

    for col in ['IDEB_REND1', 'IDEB_REND2', 'IDEB_REND3', 'IDEB_REND4', 'IDEB_REND_MEDIO']:
        df_escola_final[col] = np.nan
    df_escola_final.dropna(subset=['CODIGO_ESCOLA', 'ANO_ESCOLA'], inplace=True)
    df_escola_final = df_escola_final.drop_duplicates(subset=['CODIGO_ESCOLA', 'ANO_ESCOLA'])
    
    # --- 2. Preparar Dados do ENADE (para tabela Curso) ---
    df_enade = pd.concat(
        [pd.read_csv(
            f, 
            sep=',', 
            on_bad_lines='skip', 
            dtype={'Código da IES': str, 'Código do Curso': str}, 
            encoding='latin1',
            low_memory=False
        ) for f in ARQUIVOS_ENADE],
        ignore_index=True
    )
    df_enade.columns = df_enade.columns.str.strip()

    enade_cols = {
        'faixa': find_column_name(df_enade.columns, 'Conceito Enade (Faixa)'),
        'uf': find_column_name(df_enade.columns, 'Sigla da UF'), 
        'fg_bruta': find_column_name(df_enade.columns, 'Nota Bruta - FG'),
        'fg_pad': find_column_name(df_enade.columns, 'Nota Padronizada - FG'),
        'ce_bruta': find_column_name(df_enade.columns, 'Nota Bruta - CE'),
        'ce_pad': find_column_name(df_enade.columns, 'Nota Padronizada - CE'),
        'continua': find_column_name(df_enade.columns, 'Conceito Enade (Contínuo)'),
        'sigla_ies': find_column_name(df_enade.columns, 'Sigla da IES'),
        'nome_ies': find_column_name(df_enade.columns, 'Nome da IES'),
        'cod_ies': find_column_name(df_enade.columns, 'Código da IES'),
        'nome_curso': find_column_name(df_enade.columns, 'Área de Avaliação'), 
        'inscritos': find_column_name(df_enade.columns, 'Nº de Concluintes Inscritos'),
        'concluintes': find_column_name(df_enade.columns, 'Nº de Concluintes Participantes'),
        'ano': find_column_name(df_enade.columns, 'Ano'),
        'municipio': find_column_name(df_enade.columns, 'Município do Curso')
    }
    if any(v is None for v in enade_cols.values()):
        raise ValueError(f"Uma ou mais colunas não foram encontradas no arquivo ENADE. Verifique os nomes: {enade_cols}")

    df_enade = df_enade[df_enade[enade_cols['faixa']] != 'SC']
    df_enade.dropna(subset=[enade_cols['uf']], inplace=True)
    colunas_essenciais = [
        enade_cols['fg_bruta'], enade_cols['fg_pad'], enade_cols['ce_bruta'], 
        enade_cols['ce_pad'], enade_cols['continua'], enade_cols['faixa'], enade_cols['sigla_ies']
    ]
    df_enade.dropna(subset=colunas_essenciais, inplace=True)
    df_enade = df_enade.reset_index(drop=True)

    df_curso_final = pd.DataFrame()
    df_curso_final['NOME_IES'] = df_enade[enade_cols['nome_ies']]
    df_curso_final['SIGLA_IES'] = df_enade[enade_cols['sigla_ies']]
    df_curso_final['CODIGO_IES'] = df_enade[enade_cols['cod_ies']]
    df_curso_final['NOME_CURSO'] = df_enade[enade_cols['nome_curso']].str.slice(0, 200)
    df_curso_final['TOTAL_INSCRITOS'] = pd.to_numeric(df_enade[enade_cols['inscritos']], errors='coerce')
    df_curso_final['TOTAL_CONCLUINTES'] = pd.to_numeric(df_enade[enade_cols['concluintes']], errors='coerce')
    df_curso_final['NOTA_BRUTA_CE'] = str_to_float(df_enade[enade_cols['ce_bruta']])
    df_curso_final['NOTA_PADRONIZADA_CE'] = str_to_float(df_enade[enade_cols['ce_pad']])
    df_curso_final['NOTA_BRUTA_FG'] = str_to_float(df_enade[enade_cols['fg_bruta']])
    df_curso_final['NOTA_PADRONIZADA_FG'] = str_to_float(df_enade[enade_cols['fg_pad']])
    df_curso_final['NOTA_ENADE_CONTINUA'] = str_to_float(df_enade[enade_cols['continua']])
    df_curso_final['NOTA_ENADE_FAIXA'] = pd.to_numeric(df_enade[enade_cols['faixa']], errors='coerce')
    df_curso_final['ANO_ENADE'] = df_enade[enade_cols['ano']]
    df_curso_final['SIGLA_UF'] = df_enade[enade_cols['uf']]
    df_curso_final['NOME_MUNICIPIO'] = df_enade[enade_cols['municipio']]
    df_curso_final = df_curso_final.drop_duplicates(subset=['CODIGO_IES', 'NOME_CURSO', 'ANO_ENADE'])
    
    # --- 3. Criar e Popular Tabela Municipio (com agregações) ---
    escola_counts = df_escola_final.groupby(['SIGLA_UF', 'NOME_MUNICIPIO'])['CODIGO_ESCOLA'].nunique().reset_index()
    escola_counts = escola_counts.rename(columns={'CODIGO_ESCOLA': 'QUANTIDADE_ESCOLA'})
    
    ies_counts = df_curso_final.groupby(['SIGLA_UF', 'NOME_MUNICIPIO'])['CODIGO_IES'].nunique().reset_index()
    ies_counts = ies_counts.rename(columns={'CODIGO_IES': 'QUANTIDADE_IES'})
    
    df_municipio = pd.merge(escola_counts, ies_counts, on=['SIGLA_UF', 'NOME_MUNICIPIO'], how='outer')
    df_municipio.fillna(0, inplace=True)
    
    actual_sigla_uf_col = base_cols_map['SIGLA_UF']
    actual_nome_uf_col = find_column_name(df_ideb_raw.columns, 'Nome da UF')
    if actual_nome_uf_col is None:
        actual_nome_uf_col = actual_sigla_uf_col # Fallback
    
    map_df = df_ideb_raw[[actual_sigla_uf_col, actual_nome_uf_col]].drop_duplicates()
    map_df.columns = ['SIGLA_UF', 'NOME_UF']
    map_series = map_df.set_index('SIGLA_UF')['NOME_UF']
    
    df_municipio['NOME_UF'] = df_municipio['SIGLA_UF'].map(map_series)
    
    df_municipio_final = df_municipio[['SIGLA_UF', 'NOME_UF', 'NOME_MUNICIPIO', 'QUANTIDADE_IES', 'QUANTIDADE_ESCOLA']]
    df_municipio_final.dropna(subset=['SIGLA_UF', 'NOME_MUNICIPIO'], inplace=True)
    
    con.register('df_municipio_final', df_municipio_final)
    con.execute("INSERT INTO Municipio SELECT * FROM df_municipio_final")

    # --- 4. Inserir dados nas tabelas Escola e Curso ---
    df_escola_final = df_escola_final.merge(df_municipio_final[['SIGLA_UF', 'NOME_MUNICIPIO']], on=['SIGLA_UF', 'NOME_MUNICIPIO'], how='inner')
    con.register('df_escola_para_inserir', df_escola_final)
    colunas_escola_db = [desc[0] for desc in con.execute("DESCRIBE Escola").fetchall()]
    con.execute(f"INSERT INTO Escola SELECT {','.join(colunas_escola_db)} FROM df_escola_para_inserir")
    
    df_curso_final = df_curso_final.merge(df_municipio_final[['SIGLA_UF', 'NOME_MUNICIPIO']], on=['SIGLA_UF', 'NOME_MUNICIPIO'], how='inner')
    con.register('df_curso_para_inserir', df_curso_final)
    colunas_curso_db = [desc[0] for desc in con.execute("DESCRIBE Curso").fetchall()]
    con.execute(f"INSERT INTO Curso SELECT {','.join(colunas_curso_db)} FROM df_curso_para_inserir")

def main():
    """
    Função principal que orquestra todo o processo.
    """
    con = duckdb.connect(database=DB_FILE, read_only=False)
    
    criar_tabelas(con)
    carregar_dados(con)
    
        
    print("\nAmostra de dados da tabela 'Municipio':")
    print(con.execute("SELECT * FROM Municipio ORDER BY QUANTIDADE_IES DESC, QUANTIDADE_ESCOLA DESC LIMIT 5").df())

    print("\nAmostra de dados da tabela 'Escola':")
    print(con.execute("SELECT NOME_ESCOLA, ANO_ESCOLA, NOME_MUNICIPIO, IDEB_NOTA FROM Escola WHERE IDEB_NOTA IS NOT NULL LIMIT 5").df())

    print("\nAmostra de dados da tabela 'Curso':")
    print(con.execute("SELECT NOME_IES, NOME_CURSO, NOTA_ENADE_CONTINUA FROM Curso WHERE NOTA_ENADE_CONTINUA IS NOT NULL ORDER BY NOTA_ENADE_CONTINUA DESC LIMIT 5").df())
    
    con.close()

if __name__ == "__main__":
    main()
