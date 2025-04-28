import pandas as pd
import psycopg2
from datetime import datetime
import re
import os
import sys
import logging
import psycopg2.extras

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('import_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'dbname': 'tsmx_db',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

# File path for the Excel file
EXCEL_FILE_PATH = 'dados_importacao.xlsx'

# Output directory for report files
OUTPUT_DIR = "C:/Users/Aisla/Downloads"
TOTAL_REGISTROS_FILE = os.path.join(OUTPUT_DIR, "import_totalregistros.xlsx")
ERRORS_FILE = os.path.join(OUTPUT_DIR, "import_erros.xlsx")

# Expected columns in the Excel file
EXPECTED_COLUMNS = [
    'CPF/CNPJ', 'Nome/Razão Social', 'Nome Fantasia', 'Data Nasc.', 'Data Cadastro cliente',
    'Celulares', 'Telefones', 'Emails', 'Plano', 'Plano Valor', 'Vencimento', 'Isento',
    'Endereço', 'Número', 'Bairro', 'Cidade', 'Complemento', 'CEP', 'UF', 'Status'
]

# Mapeamento de UFs
UF_MAPPING = {
    'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPÁ': 'AP', 'AMAZONAS': 'AM', 'BAHIA': 'BA',
    'CEARÁ': 'CE', 'DISTRITO FEDERAL': 'DF', 'ESPÍRITO SANTO': 'ES', 'GOIÁS': 'GO',
    'MARANHÃO': 'MA', 'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS', 'MINAS GERAIS': 'MG',
    'PARÁ': 'PA', 'PARAÍBA': 'PB', 'PARANÁ': 'PR', 'PERNAMBUCO': 'PE', 'PIAUÍ': 'PI',
    'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN', 'RIO GRANDE DO SUL': 'RS',
    'RONDÔNIA': 'RO', 'RORAIMA': 'RR', 'SANTA CATARINA': 'SC', 'SÃO PAULO': 'SP',
    'SERGIPE': 'SE', 'TOCANTINS': 'TO'
}

# Helper functions
def clean_cpf_cnpj(value):
    """Clean CPF/CNPJ by removing non-numeric characters."""
    if pd.isna(value):
        return None
    return re.sub(r'[^\d]', '', str(value))

def convert_excel_date(excel_date):
    """Convert Excel numeric date to Python date."""
    if pd.isna(excel_date) or not isinstance(excel_date, (int, float)):
        return None
    try:
        return (datetime(1899, 12, 30) + pd.Timedelta(days=excel_date)).date()
    except Exception as e:
        logger.warning(f"Failed to convert Excel date {excel_date}: {e}")
        return None

def clean_phone(phone):
    """Clean phone number by removing non-numeric characters."""
    if pd.isna(phone):
        return None
    cleaned = re.sub(r'[^\d]', '', str(phone))
    if len(cleaned) < 10:
        logger.warning(f"Invalid phone number (less than 10 digits): {cleaned}")
        return None
    return cleaned

def clean_cep(cep, row_index):
    """Clean and normalize CEP to 8 digits."""
    if pd.isna(cep):
        logger.warning(f"Row {row_index + 1}: CEP missing")
        return None
    cep = re.sub(r'[^\d]', '', str(cep))
    if len(cep) != 8:
        if len(cep) < 8:
            cep = cep.zfill(8)
            logger.info(f"Row {row_index + 1}: Fixed CEP by padding: {cep}")
        else:
            logger.warning(f"Row {row_index + 1}: Invalid CEP (too long): {cep}")
            return None
    return cep

def encode_string(value, max_length=None):
    """Encode string to UTF-8, truncate if necessary, and handle encoding errors."""
    if pd.isna(value) or value is None:
        return None
    try:
        value_str = str(value).encode('utf-8', errors='replace').decode('utf-8')
        if max_length and len(value_str) > max_length:
            value_str = value_str[:max_length]
        return value_str
    except Exception as e:
        logger.warning(f"Encoding error for value '{value}': {e}")
        return None

def normalize_uf(uf, row_index):
    """Normalize UF to 2-letter code."""
    if pd.isna(uf):
        logger.warning(f"Row {row_index + 1}: UF missing")
        return None
    uf = str(uf).strip().upper()
    if len(uf) == 2 and uf in UF_MAPPING.values():
        return uf
    if uf in UF_MAPPING:
        return UF_MAPPING[uf]
    logger.warning(f"Row {row_index + 1}: Invalid UF: {uf}")
    return None

def validate_dia_vencimento(dia, row_index):
    """Validate day of payment (1-31)."""
    if pd.isna(dia):
        logger.warning(f"Row {row_index + 1}: Dia de vencimento missing")
        return None
    try:
        dia = int(dia)
        if dia < 1 or dia > 31:
            logger.warning(f"Row {row_index + 1}: Invalid dia de vencimento (1-31): {dia}")
            return None
        return dia
    except ValueError:
        logger.warning(f"Row {row_index + 1}: Dia de vencimento is not a number: {dia}")
        return None

def get_or_create_plano(cursor, descricao, valor):
    """Get or create plano ID."""
    try:
        descricao = encode_string(descricao, 255)
        cursor.execute("SELECT id FROM tbl_planos WHERE descricao = %s", (descricao,))
        result = cursor.fetchone()
        if result:
            return result[0]
        
        cursor.execute(
            "INSERT INTO tbl_planos (descricao, valor) VALUES (%s, %s) RETURNING id",
            (descricao, float(valor))
        )
        return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error in get_or_create_plano for {descricao}: {e}")
        raise

def get_status_id(cursor, status):
    """Get status ID from status name."""
    try:
        status = encode_string(status)
        cursor.execute("SELECT id FROM tbl_status_contrato WHERE status = %s", (status,))
        result = cursor.fetchone()
        return result[0] if result else 2  # Default to 'Velocidade Reduzida'
    except Exception as e:
        logger.error(f"Error in get_status_id for {status}: {e}")
        raise

def main(excel_file_path=EXCEL_FILE_PATH):
    """Main function to import data from Excel to PostgreSQL."""
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check if the Excel file exists
    if not os.path.exists(excel_file_path):
        logger.error(f"The file '{excel_file_path}' does not exist.")
        sys.exit(1)
    
    # Read Excel file
    try:
        df = pd.read_excel(excel_file_path)
        logger.info(f"Successfully read Excel file: {excel_file_path}")
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        sys.exit(1)
    
    # Validate Excel columns
    missing_columns = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing columns in Excel file: {missing_columns}")
        sys.exit(1)
    
    # Initialize counters and lists
    total_clientes = 0
    total_contatos = 0
    total_contratos = 0
    contratos_importados = 0
    total_erros = 0
    errors_list = []
    success_list = []

    # Connect to database
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_session(autocommit=False)
        cursor = conn.cursor()
        logger.info("Connected to database successfully")
        
        # Process each row
        for index, row in df.iterrows():
            error_row = row.copy()
            has_error = False

            # Clean and prepare data
            cpf_cnpj = clean_cpf_cnpj(row['CPF/CNPJ'])
            if not cpf_cnpj or len(cpf_cnpj) not in (11, 14):
                error_row['Motivo do Erro'] = f"Defina CPF/CNPJ como um CPF válido de 11 dígitos (por exemplo, {cpf_cnpj + '0' if cpf_cnpj else '12345678901'}) ou um CNPJ de 14 dígitos."
                logger.warning(f"Row {index + 1}: Invalid CPF/CNPJ: {cpf_cnpj}")
                errors_list.append(error_row)
                total_erros += 1
                continue
                
            # Insert or get client
            data_nascimento = convert_excel_date(row['Data Nasc.'])
            data_cadastro = convert_excel_date(row['Data Cadastro cliente'])
            nome_razao_social = encode_string(row['Nome/Razão Social'], 255)
            if not nome_razao_social:
                error_row['Motivo do Erro'] = "Defina Nome/Razão Social como um valor válido."
                logger.warning(f"Row {index + 1}: Nome/Razão Social missing")
                errors_list.append(error_row)
                total_erros += 1
                continue

            try:
                cursor.execute("""
                    INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (cpf_cnpj) DO UPDATE 
                    SET nome_razao_social = EXCLUDED.nome_razao_social,
                        nome_fantasia = EXCLUDED.nome_fantasia,
                        data_nascimento = EXCLUDED.data_nascimento,
                        data_cadastro = EXCLUDED.data_cadastro
                    RETURNING id, (xmax = 0) AS is_new
                """, (
                    nome_razao_social,
                    encode_string(row['Nome Fantasia'], 255),
                    cpf_cnpj,
                    data_nascimento,
                    data_cadastro
                ))
                
                cliente_id, is_new = cursor.fetchone()
                if is_new:
                    logger.info(f"Row {index + 1}: Inserted new client with CPF/CNPJ {cpf_cnpj}")
                else:
                    logger.info(f"Row {index + 1}: Updated existing client with CPF/CNPJ {cpf_cnpj}")
                total_clientes += 1
            except Exception as e:
                error_row['Motivo do Erro'] = f"Erro ao inserir cliente: {str(e)}"
                logger.error(f"Row {index + 1}: Error inserting client {cpf_cnpj}: {e}")
                conn.rollback()
                errors_list.append(error_row)
                total_erros += 1
                continue

            # Insert contacts (Celulares, Telefones, Emails)
            contatos = [
                ('Celular', clean_phone(row['Celulares']), 2),  # 2 = Celular
                ('Telefone', clean_phone(row['Telefones']), 1),  # 1 = Telefone
                ('E-Mail', encode_string(row['Emails'], 255), 3)  # 3 = E-Mail
            ]

            for tipo, contato, tipo_contato_id in contatos:
                if contato:
                    try:
                        cursor.execute("""
                            INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                            RETURNING id
                        """, (cliente_id, tipo_contato_id, contato))
                        if cursor.fetchone():
                            total_contatos += 1
                        else:
                            logger.info(f"Row {index + 1}: Skipped duplicate {tipo} contact {contato} for client {cpf_cnpj}")
                    except Exception as e:
                        error_row['Motivo do Erro'] = f"Erro ao inserir contato {tipo}: {str(e)}"
                        logger.error(f"Row {index + 1}: Error inserting {tipo} contact for client {cpf_cnpj}: {e}")
                        conn.rollback()
                        # Continue despite contact error

            # Insert contract
            try:
                plano_id = get_or_create_plano(cursor, str(row['Plano']), float(row['Plano Valor']))
                status_id = get_status_id(cursor, str(row['Status']))
                dia_vencimento = validate_dia_vencimento(row['Vencimento'], index)
                if not dia_vencimento:
                    error_row['Motivo do Erro'] = "Defina Vencimento como um número válido entre 1 e 31."
                    errors_list.append(error_row)
                    total_erros += 1
                    conn.rollback()
                    continue

                cep = clean_cep(row['CEP'], index)
                uf = normalize_uf(row['UF'], index)
                endereco_logradouro = encode_string(row['Endereço'], 255)
                isento = row['Isento'] == 'Sim' if not pd.isna(row['Isento']) else False

                # Validate required fields for tbl_cliente_contratos
                if cep is None:
                    error_row['Motivo do Erro'] = "Defina CEP como 00000000 (ou um CEP válido)."
                    logger.warning(f"Row {index + 1}: Missing endereco_cep for client {cpf_cnpj}")
                    errors_list.append(error_row)
                    total_erros += 1
                    conn.rollback()
                    continue
                if endereco_logradouro is None:
                    error_row['Motivo do Erro'] = "Coloque Endereço na Rua Desconhecida (ou um endereço válido)."
                    logger.warning(f"Row {index + 1}: Missing endereco_logradouro for client {cpf_cnpj}")
                    errors_list.append(error_row)
                    total_erros += 1
                    conn.rollback()
                    continue

                cursor.execute("""
                    INSERT INTO tbl_cliente_contratos (
                        cliente_id, plano_id, dia_vencimento, isento, 
                        endereco_logradouro, endereco_numero, endereco_bairro,
                        endereco_cidade, endereco_complemento, endereco_cep,
                        endereco_uf, status_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                """, (
                    cliente_id,
                    plano_id,
                    dia_vencimento,
                    isento,
                    endereco_logradouro,
                    encode_string(row['Número'], 15),
                    encode_string(row['Bairro'], 255),
                    encode_string(row['Cidade'], 255),
                    encode_string(row['Complemento'], 500),
                    cep,
                    uf,
                    status_id
                ))
                if cursor.fetchone():
                    total_contratos += 1
                    contratos_importados += 1
                    # Add to success list if contract was inserted
                    success_row = row.copy()
                    success_row['Motivo do Erro'] = ''  # Empty for successful imports
                    success_list.append(success_row)
                else:
                    logger.info(f"Row {index + 1}: Skipped duplicate contract for client {cpf_cnpj}")
            except Exception as e:
                error_row['Motivo do Erro'] = f"Erro ao inserir contrato: {str(e)}"
                logger.error(f"Row {index + 1}: Error inserting contract for client {cpf_cnpj}: {e}")
                conn.rollback()
                errors_list.append(error_row)
                total_erros += 1
                continue
        
            # Commit transaction per row
            try:
                conn.commit()
            except Exception as e:
                error_row['Motivo do Erro'] = f"Erro ao confirmar transação: {str(e)}"
                logger.error(f"Row {index + 1}: Error committing transaction: {e}")
                conn.rollback()
                errors_list.append(error_row)
                total_erros += 1
                continue

        # Save reports to Excel
        if errors_list or success_list:
            # Define columns including Motivo do Erro
            columns = df.columns.tolist() + ['Motivo do Erro']
            
            # Create DataFrames
            errors_df = pd.DataFrame(errors_list, columns=columns) if errors_list else pd.DataFrame(columns=columns)
            success_df = pd.DataFrame(success_list, columns=columns) if success_list else pd.DataFrame(columns=columns)
            
            # Save errors report (only failed records)
            if not errors_df.empty:
                errors_header = pd.DataFrame([columns], columns=columns)
                errors_final_df = pd.concat([errors_header, errors_df], ignore_index=True)
                errors_final_df.to_excel(ERRORS_FILE, index=False)
                logger.info(f"Errors report saved to '{ERRORS_FILE}'")
            
            # Save total records report (only imported records)
            if not success_df.empty:
                success_header = pd.DataFrame([columns], columns=columns)
                total_records_df = pd.concat([
                    success_header,
                    success_df
                ], ignore_index=True)
                
                total_records_df.to_excel(TOTAL_REGISTROS_FILE, index=False)
                logger.info(f"Imported records report saved to '{TOTAL_REGISTROS_FILE}'")

        # Log final metrics
        logger.info(f"Total de clientes processados: {total_clientes}")
        logger.info(f"Total de contatos processados: {total_contatos}")
        logger.info(f"Total de contratos processados: {total_contratos}")
        logger.info(f"Total de contratos importados: {contratos_importados}")
        logger.info(f"Total de erros: {total_erros}")
        
    except Exception as e:
        logger.error(f"Error during database operation: {e}")
        if conn is not None:
            conn.rollback()
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    # Allow file path to be passed as command-line argument
    excel_file_path = EXCEL_FILE_PATH
    if len(sys.argv) > 1:
        excel_file_path = sys.argv[1]
    
    main(excel_file_path)