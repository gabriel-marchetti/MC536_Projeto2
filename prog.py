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


def get_uf_full_name(sigla : str) -> str:
    if not isinstance(sigla, str):
        raise TypeError("sigla em get_uf_full_name(sigla) deve ser do tipo str")

    uf_dict = {
        'AC': 'Acre',
        'AL': 'Alagoas',
        'AP': 'Amapá',
        'AM': 'Amazonas',
        'BA': 'Bahia',
        'CE': 'Ceará',
        'DF': 'Distrito Federal',
        'ES': 'Espírito Santo',
        'GO': 'Goiás',
        'MA': 'Maranhão',
        'MT': 'Mato Grosso',
        'MS': 'Mato Grosso do Sul',
        'MG': 'Minas Gerais',
        'PA': 'Pará',
        'PB': 'Paraíba',
        'PR': 'Paraná',
        'PE': 'Pernambuco',
        'PI': 'Piauí',
        'RJ': 'Rio de Janeiro',
        'RN': 'Rio Grande do Norte',
        'RS': 'Rio Grande do Sul',
        'RO': 'Rondônia',
        'RR': 'Roraima',
        'SC': 'Santa Catarina',
        'SP': 'São Paulo',
        'SE': 'Sergipe',
        'TO': 'Tocantins'
    }
    
    key = sigla.strip().upper()
    try:
        return uf_dict[key]
    except KeyError:
        raise ValueError(f"Sigla não contida em uf_dict: {sigla}")



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

    for col in ['IDEB_REND1', 'IDEB_REND2', 'IDEB_REND3', 'IDEB_REND4']:
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
    
    # --- 3. Criar e Popular Tabela Municipio ---
    # Obter municípios únicos das escolas e cursos
    municipios_escola = df_escola_final[['SIGLA_UF', 'NOME_MUNICIPIO']].drop_duplicates()
    municipios_curso = df_curso_final[['SIGLA_UF', 'NOME_MUNICIPIO']].drop_duplicates()
    
    # Combinar todos os municípios
    df_municipio = pd.concat([municipios_escola, municipios_curso]).drop_duplicates()
    
    # Usa a função get_uf_full_name para obter o nome completo da UF
    df_municipio['NOME_UF'] = df_municipio['SIGLA_UF'].apply(get_uf_full_name)
    
    df_municipio_final = df_municipio[['SIGLA_UF', 'NOME_UF', 'NOME_MUNICIPIO']]
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

def salvar_resultado_txt(resultado_df, titulo, arquivo_txt):
    """
    Salva o resultado de uma consulta em um arquivo de texto.
    """
    with open(arquivo_txt, 'a', encoding='utf-8') as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"{titulo}\n")
        f.write(f"{'='*80}\n")
        f.write(resultado_df.to_string(index=False))
        f.write(f"\n{'='*80}\n\n")

def main():
    """
    Função principal que orquestra todo o processo.
    """
    con = duckdb.connect(database=DB_FILE, read_only=False)
    
    criar_tabelas(con)
    carregar_dados(con)
    
    # Nome do arquivo de saída
    arquivo_resultados = 'resultados_consultas.txt'
    # Limpar arquivo de resultados se existir
    with open(arquivo_resultados, 'w', encoding='utf-8') as f:
        f.write("="*80)
    
    # CONSULTA 1: Ranking de Estados por Performance IDEB
    print("\n1. RANKING DE ESTADOS POR PERFORMANCE IDEB")
    print("-"*60)
    query1 = """
    WITH estados_ideb AS (
        SELECT 
            SIGLA_UF,
            AVG(IDEB_NOTA) as media_ideb,
            COUNT(DISTINCT CODIGO_ESCOLA) as total_escolas,
            MIN(IDEB_NOTA) as min_ideb,
            MAX(IDEB_NOTA) as max_ideb,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY IDEB_NOTA) as mediana_ideb
        FROM Escola 
        WHERE IDEB_NOTA IS NOT NULL
        GROUP BY SIGLA_UF
    )
    SELECT 
        e.SIGLA_UF,
        (SELECT NOME_UF FROM Municipio m WHERE m.SIGLA_UF = e.SIGLA_UF LIMIT 1) as NOME_UF,
        ROUND(e.media_ideb, 3) as "IDEB Médio",
        e.total_escolas as "Total Escolas",
        ROUND(e.min_ideb, 3) as "IDEB Mínimo",
        ROUND(e.max_ideb, 3) as "IDEB Máximo",
        ROUND(e.mediana_ideb, 3) as "IDEB Mediana"
    FROM estados_ideb e
    ORDER BY e.media_ideb DESC
    LIMIT 10;
    """
    result1 = con.execute(query1).df()
    print(result1.to_string(index=False))
    salvar_resultado_txt(result1, "1. RANKING DE ESTADOS POR PERFORMANCE IDEB", arquivo_resultados)
    
    # CONSULTA 2: Top Cursos por Nota ENADE
    print("\n\n2. TOP CURSOS POR NOTA ENADE")
    print("-"*60)
    query2 = """
    WITH cursos_stats AS (
        SELECT 
            NOME_CURSO,
            AVG(NOTA_ENADE_CONTINUA) as nota_media,
            COUNT(*) as total_ofertas,
            COUNT(DISTINCT SIGLA_UF) as estados_presentes,
            COUNT(DISTINCT CODIGO_IES) as ies_diferentes,
            SUM(TOTAL_INSCRITOS) as total_inscritos_geral
        FROM Curso 
        WHERE NOTA_ENADE_CONTINUA IS NOT NULL
        GROUP BY NOME_CURSO
        HAVING COUNT(*) >= 30  -- Cursos com pelo menos 30 ofertas
    )
    SELECT 
        NOME_CURSO as "Nome do Curso",
        ROUND(nota_media, 3) as "Nota ENADE Média",
        total_ofertas as "Total Ofertas",
        estados_presentes as "Estados Presentes", 
        ies_diferentes as "IES Diferentes",
        total_inscritos_geral as "Total Inscritos"
    FROM cursos_stats
    ORDER BY nota_media DESC
    LIMIT 15;
    """
    result2 = con.execute(query2).df()
    print(result2.to_string(index=False))
    salvar_resultado_txt(result2, "2. TOP CURSOS POR NOTA ENADE", arquivo_resultados)
    
    # CONSULTA 3: Análise Temporal do IDEB
    print("\n\n3. EVOLUÇÃO TEMPORAL DO IDEB POR ESTADO")
    print("-"*60)
    query3 = """
    WITH evolucao_temporal AS (
        SELECT 
            SIGLA_UF,
            AVG(CASE WHEN ANO_ESCOLA = 2017 THEN IDEB_NOTA END) as ideb_2017,
            AVG(CASE WHEN ANO_ESCOLA = 2019 THEN IDEB_NOTA END) as ideb_2019,
            AVG(CASE WHEN ANO_ESCOLA = 2021 THEN IDEB_NOTA END) as ideb_2021,
            AVG(CASE WHEN ANO_ESCOLA = 2023 THEN IDEB_NOTA END) as ideb_2023
        FROM Escola
        WHERE IDEB_NOTA IS NOT NULL
        GROUP BY SIGLA_UF
    )
    SELECT 
        e.SIGLA_UF,
        (SELECT NOME_UF FROM Municipio m WHERE m.SIGLA_UF = e.SIGLA_UF LIMIT 1) as "Estado",
        ROUND(e.ideb_2017, 3) as "IDEB 2017",
        ROUND(e.ideb_2019, 3) as "IDEB 2019",
        ROUND(e.ideb_2021, 3) as "IDEB 2021", 
        ROUND(e.ideb_2023, 3) as "IDEB 2023",
        ROUND(e.ideb_2023 - e.ideb_2017, 3) as "Variação 2017-2023",
        CASE 
            WHEN e.ideb_2023 > e.ideb_2017 THEN 'Crescimento'
            WHEN e.ideb_2023 < e.ideb_2017 THEN 'Declínio'
            ELSE 'Estável'
        END as "Tendência"
    FROM evolucao_temporal e
    WHERE e.ideb_2017 IS NOT NULL AND e.ideb_2023 IS NOT NULL
    ORDER BY "Variação 2017-2023" DESC;
    """
    result3 = con.execute(query3).df()
    print(result3.to_string(index=False))
    salvar_resultado_txt(result3, "3. EVOLUÇÃO TEMPORAL DO IDEB POR ESTADO", arquivo_resultados)
    
    # CONSULTA 4: Comparação Detalhada de Redes de Ensino
    print("\n\n4. ANÁLISE COMPARATIVA DETALHADA DE REDES DE ENSINO")
    print("-"*60)
    query4 = """
    WITH escolas_por_rede AS (
        SELECT 
            SIGLA_UF,
            REDE_ESCOLA,
            COUNT(DISTINCT CODIGO_ESCOLA) as total_escolas,
            AVG(IDEB_NOTA) as ideb_medio,
            AVG(SAEB_NOTA_MAT) as saeb_mat_medio,
            AVG(SAEB_NOTA_PORT) as saeb_port_medio
        FROM Escola
        WHERE IDEB_NOTA IS NOT NULL AND REDE_ESCOLA IN ('Estadual', 'Federal', 'Privada')
        GROUP BY SIGLA_UF, REDE_ESCOLA
    ),
    ies_por_categoria AS (
        SELECT 
            SIGLA_UF,
            CASE 
                WHEN UPPER(NOME_IES) LIKE '%FEDERAL%' OR UPPER(NOME_IES) LIKE '%UNIVERSIDADE FEDERAL%' 
                     OR UPPER(NOME_IES) LIKE '%INSTITUTO FEDERAL%' THEN 'Pública Federal'
                WHEN UPPER(NOME_IES) LIKE '%FUNDAÇÃO%' OR UPPER(NOME_IES) LIKE '%FILANTRÓPICA%'
                     OR UPPER(NOME_IES) LIKE '%BENEFICENTE%' THEN 'Privada sem fins lucrativos'
                ELSE 'Privada com fins lucrativos'
            END as categoria_ies,
            COUNT(DISTINCT CODIGO_IES) as total_ies,
            AVG(NOTA_ENADE_CONTINUA) as enade_medio
        FROM Curso
        WHERE NOTA_ENADE_CONTINUA IS NOT NULL
        GROUP BY SIGLA_UF, categoria_ies
    ),
    escolas_consolidado AS (
        SELECT 
            SIGLA_UF,
            MAX(CASE WHEN REDE_ESCOLA = 'Estadual' THEN total_escolas END) as escolas_estadual,
            MAX(CASE WHEN REDE_ESCOLA = 'Federal' THEN total_escolas END) as escolas_federal,
            MAX(CASE WHEN REDE_ESCOLA = 'Privada' THEN total_escolas END) as escolas_privada,
            MAX(CASE WHEN REDE_ESCOLA = 'Estadual' THEN ideb_medio END) as ideb_estadual,
            MAX(CASE WHEN REDE_ESCOLA = 'Federal' THEN ideb_medio END) as ideb_federal,
            MAX(CASE WHEN REDE_ESCOLA = 'Privada' THEN ideb_medio END) as ideb_privada
        FROM escolas_por_rede
        GROUP BY SIGLA_UF
    ),
    ies_consolidado AS (
        SELECT 
            SIGLA_UF,
            MAX(CASE WHEN categoria_ies = 'Pública Federal' THEN total_ies END) as ies_publica_federal,
            MAX(CASE WHEN categoria_ies = 'Privada sem fins lucrativos' THEN total_ies END) as ies_privada_sem_fins,
            MAX(CASE WHEN categoria_ies = 'Privada com fins lucrativos' THEN total_ies END) as ies_privada_com_fins,
            MAX(CASE WHEN categoria_ies = 'Pública Federal' THEN enade_medio END) as enade_publica_federal,
            MAX(CASE WHEN categoria_ies = 'Privada sem fins lucrativos' THEN enade_medio END) as enade_privada_sem_fins,
            MAX(CASE WHEN categoria_ies = 'Privada com fins lucrativos' THEN enade_medio END) as enade_privada_com_fins
        FROM ies_por_categoria
        GROUP BY SIGLA_UF
    )
    SELECT 
        e.SIGLA_UF,
        (SELECT NOME_UF FROM Municipio m WHERE m.SIGLA_UF = e.SIGLA_UF LIMIT 1) as "Estado",
        COALESCE(e.escolas_estadual, 0) as "Esc. Estadual",
        COALESCE(e.escolas_federal, 0) as "Esc. Federal", 
        COALESCE(e.escolas_privada, 0) as "Esc. Privada",
        ROUND(COALESCE(e.ideb_estadual, 0), 3) as "IDEB Estadual",
        ROUND(COALESCE(e.ideb_federal, 0), 3) as "IDEB Federal",
        ROUND(COALESCE(e.ideb_privada, 0), 3) as "IDEB Privada",
        COALESCE(i.ies_publica_federal, 0) as "IES Públ Fed",
        COALESCE(i.ies_privada_sem_fins, 0) as "IES Priv S/Fins",
        COALESCE(i.ies_privada_com_fins, 0) as "IES Priv C/Fins",
        ROUND(COALESCE(i.enade_publica_federal, 0), 3) as "ENADE Públ Fed",
        ROUND(COALESCE(i.enade_privada_sem_fins, 0), 3) as "ENADE Priv S/Fins",
        ROUND(COALESCE(i.enade_privada_com_fins, 0), 3) as "ENADE Priv C/Fins"
    FROM escolas_consolidado e
    LEFT JOIN ies_consolidado i ON e.SIGLA_UF = i.SIGLA_UF
    WHERE (e.escolas_estadual > 0 OR e.escolas_federal > 0 OR e.escolas_privada > 0)
       OR (i.ies_publica_federal > 0 OR i.ies_privada_sem_fins > 0 OR i.ies_privada_com_fins > 0)
    ORDER BY COALESCE(i.enade_publica_federal, 0) DESC, COALESCE(e.ideb_federal, 0) DESC
    LIMIT 20;
    """
    result4 = con.execute(query4).df()
    print(result4.to_string(index=False))
    salvar_resultado_txt(result4, "4. ANÁLISE COMPARATIVA DETALHADA DE REDES DE ENSINO", arquivo_resultados)
    
    # CONSULTA 5: Análise de Distribuição Geográfica das IES
    print("\n\n5. DISTRIBUIÇÃO GEOGRÁFICA DAS IES POR QUALIDADE")
    print("-"*60)
    query5 = """
    WITH ies_qualidade AS (
        SELECT 
            CODIGO_IES,
            NOME_IES,
            SIGLA_UF,
            AVG(NOTA_ENADE_CONTINUA) as nota_media_ies,
            COUNT(DISTINCT NOME_CURSO) as cursos_oferecidos,
            SUM(TOTAL_INSCRITOS) as total_alunos,
            CASE 
                WHEN AVG(NOTA_ENADE_CONTINUA) >= 4.0 THEN 'Excelente'
                WHEN AVG(NOTA_ENADE_CONTINUA) >= 3.0 THEN 'Boa'
                WHEN AVG(NOTA_ENADE_CONTINUA) >= 2.0 THEN 'Regular'
                ELSE 'Baixa'
            END as categoria_qualidade
        FROM Curso
        WHERE NOTA_ENADE_CONTINUA IS NOT NULL
        GROUP BY CODIGO_IES, NOME_IES, SIGLA_UF
        HAVING COUNT(DISTINCT NOME_CURSO) >= 3  -- IES com pelo menos 3 cursos
    ),
    distribuicao_por_estado AS (
        SELECT 
            SIGLA_UF,
            categoria_qualidade,
            COUNT(*) as qtd_ies,
            AVG(nota_media_ies) as nota_media_categoria,
            SUM(total_alunos) as total_alunos_categoria
        FROM ies_qualidade
        GROUP BY SIGLA_UF, categoria_qualidade
    )
    SELECT 
        d.SIGLA_UF,
        (SELECT NOME_UF FROM Municipio m WHERE m.SIGLA_UF = d.SIGLA_UF LIMIT 1) as "Estado",
        d.categoria_qualidade as "Categoria",
        d.qtd_ies as "Qtd IES",
        ROUND(d.nota_media_categoria, 3) as "Nota Média",
        d.total_alunos_categoria as "Total Alunos"
    FROM distribuicao_por_estado d
    
    ORDER BY d.nota_media_categoria DESC, d.qtd_ies DESC;
    """
    result5 = con.execute(query5).df()
    print(result5.to_string(index=False))
    salvar_resultado_txt(result5, "5. DISTRIBUIÇÃO GEOGRÁFICA DAS IES POR QUALIDADE", arquivo_resultados)
    con.close()

if __name__ == "__main__":
    main()
