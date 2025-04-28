import pandas as pd
import re
from datetime import datetime
import logging
from dateutil.parser import parse as parse_date
import os

# Configure logging for detailed traceability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Output directory for report files
OUTPUT_DIR = "C:/Users/Aisla/Downloads"
os.makedirs(OUTPUT_DIR, exist_ok=True)
ERRORS_FILE = os.path.join(OUTPUT_DIR, "validation_erros.xlsx")
SUCCESS_FILE = os.path.join(OUTPUT_DIR, "validation_success.xlsx")

# Mapeamento de UFs (Brazilian states)
UF_MAPPING = {
    'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPÁ': 'AP', 'AMAZONAS': 'AM', 'BAHIA': 'BA',
    'CEARÁ': 'CE', 'DISTRITO FEDERAL': 'DF', 'ESPÍRITO SANTO': 'ES', 'GOIÁS': 'GO',
    'MARANHÃO': 'MA', 'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS', 'MINAS GERAIS': 'MG',
    'PARÁ': 'PA', 'PARAÍBA': 'PB', 'PARANÁ': 'PR', 'PERNAMBUCO': 'PE', 'PIAUÍ': 'PI',
    'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN', 'RIO GRANDE DO SUL': 'RS',
    'RONDÔNIA': 'RO', 'RORAIMA': 'RR', 'SANTA CATARINA': 'SC', 'SÃO PAULO': 'SP',
    'SERGIPE': 'SE', 'TOCANTINS': 'TO'
}

def clean_cpf_cnpj(value, row_index):
    """
    Clean and validate CPF/CNPJ with checksum.
    Returns cleaned value or '00000000000' if invalid, with error reason if applicable.
    """
    raw_value = value
    error_reason = None
    if pd.isna(value) or value is None or str(value).strip() == '':
        error_reason = "CPF/CNPJ ausente ou vazio."
        logger.warning(f"Row {row_index + 1}: CPF/CNPJ missing or empty [raw: {raw_value}]")
        return '00000000000', error_reason
    
    cleaned = re.sub(r'[^\d]', '', str(value))
    if not cleaned:
        error_reason = "CPF/CNPJ vazio após limpeza."
        logger.warning(f"Row {row_index + 1}: CPF/CNPJ empty after cleaning [raw: {raw_value}]")
        return '00000000000', error_reason
    
    # CPF validation (11 digits)
    if len(cleaned) == 11:
        if len(set(cleaned)) == 1:
            error_reason = "CPF inválido (todos os dígitos iguais)."
            logger.warning(f"Row {row_index + 1}: Invalid CPF (all digits identical) [raw: {raw_value}, cleaned: {cleaned}]")
            return '00000000000', error_reason
        
        if cleaned == ''.join(str(i % 10) for i in range(int(cleaned[0]), int(cleaned[0]) + 11)):
            error_reason = "CPF inválido (dígitos sequenciais)."
            logger.warning(f"Row {row_index + 1}: Invalid CPF (sequential digits) [raw: {raw_value}, cleaned: {cleaned}]")
            return '00000000000', error_reason
        
        def calculate_cpf_digit(cpf, weights):
            total = sum(int(d) * w for d, w in zip(cpf, weights))
            remainder = total % 11
            return 0 if remainder < 2 else 11 - remainder
        
        weights1 = list(range(10, 1, -1))
        digit1 = calculate_cpf_digit(cleaned[:9], weights1)
        weights2 = list(range(11, 2, -1))
        digit2 = calculate_cpf_digit(cleaned[:9] + str(digit1), weights2)
        
        provided_check_digits = cleaned[9:11]
        expected_check_digits = f"{digit1}{digit2}"
        if provided_check_digits == expected_check_digits:
            logger.info(f"Row {row_index + 1}: Valid CPF [raw: {raw_value}, cleaned: {cleaned}]")
            return cleaned, None
        error_reason = f"Checksum de CPF inválido (esperado: {expected_check_digits}, fornecido: {provided_check_digits})."
        logger.warning(f"Row {row_index + 1}: Invalid CPF checksum [raw: {raw_value}, cleaned: {cleaned}]")
        return '00000000000', error_reason
    
    # CNPJ validation (14 digits)
    elif len(cleaned) == 14:
        if len(set(cleaned)) == 1:
            error_reason = "CNPJ inválido (todos os dígitos iguais)."
            logger.warning(f"Row {row_index + 1}: Invalid CNPJ (all digits identical) [raw: {raw_value}, cleaned: {cleaned}]")
            return '00000000000', error_reason
        
        def calculate_cnpj_digit(cnpj, weights):
            total = sum(int(d) * w for d, w in zip(cnpj, weights))
            remainder = total % 11
            return 0 if remainder < 2 else 11 - remainder
        
        weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit1 = calculate_cnpj_digit(cleaned[:12], weights1)
        digit2 = calculate_cnpj_digit(cleaned[:12] + str(digit1), weights2)
        
        expected_check_digits = f"{digit1}{digit2}"
        provided_check_digits = cleaned[12:14]
        if provided_check_digits == expected_check_digits:
            logger.info(f"Row {row_index + 1}: Valid CNPJ [raw: {raw_value}, cleaned: {cleaned}]")
            return cleaned, None
        error_reason = f"Checksum de CNPJ inválido (esperado: {expected_check_digits}, fornecido: {provided_check_digits})."
        logger.warning(f"Row {row_index + 1}: Invalid CNPJ checksum [raw: {raw_value}, cleaned: {cleaned}]")
        return '00000000000', error_reason
    
    error_reason = f"Comprimento de CPF/CNPJ inválido ({len(cleaned)} dígitos, esperado 11 ou 14)."
    logger.warning(f"Row {row_index + 1}: Invalid CPF/CNPJ length [raw: {raw_value}, cleaned: {cleaned}]")
    return '00000000000', error_reason

def convert_excel_date(excel_date, row_index, field_name):
    """
    Convert Excel date to Python date.
    Returns date or None, with error reason if applicable.
    """
    raw_value = excel_date
    error_reason = None
    if pd.isna(excel_date) or excel_date is None or str(excel_date).strip() == '':
        error_reason = f"{field_name} ausente ou vazio."
        logger.warning(f"Row {row_index + 1}: {field_name} missing or empty [raw: {raw_value}]")
        return None, error_reason
    
    if isinstance(excel_date, (int, float)):
        try:
            result = (datetime(1899, 12, 30) + pd.Timedelta(days=excel_date)).date()
            logger.info(f"Row {row_index + 1}: Converted numeric {field_name} [raw: {raw_value}, result: {result}]")
            return result, None
        except Exception as e:
            error_reason = f"Falha ao converter data numérica para {field_name}: {e}."
            logger.warning(f"Row {row_index + 1}: Failed to convert numeric {field_name} [raw: {raw_value}]: {e}")
            return None, error_reason
    
    try:
        parsed_date = parse_date(str(excel_date), dayfirst=True)
        result = parsed_date.date()
        logger.info(f"Row {row_index + 1}: Parsed string {field_name} [raw: {raw_value}, result: {result}]")
        return result, None
    except Exception as e:
        error_reason = f"Falha ao parsear data de string para {field_name}: {e}."
        logger.warning(f"Row {row_index + 1}: Failed to parse string {field_name} [raw: {raw_value}]: {e}")
        return None, error_reason

def clean_phone(phone, row_index, field_name):
    """
    Clean and validate Brazilian phone number.
    Returns cleaned phone or None, with error reason if applicable.
    """
    raw_value = phone
    error_reason = None
    if pd.isna(phone) or phone is None or str(phone).strip() == '':
        logger.info(f"Row {row_index + 1}: {field_name} missing or empty [raw: {raw_value}]")
        return None, None
    
    cleaned = re.sub(r'[^\d]', '', str(phone))
    if not cleaned:
        logger.info(f"Row {row_index + 1}: {field_name} empty after cleaning [raw: {raw_value}]")
        return None, None
    
    if cleaned.startswith('55') and len(cleaned) == 13:
        cleaned = cleaned[2:]
    elif cleaned.startswith('+55') and len(cleaned) == 14:
        cleaned = cleaned[3:]
    
    if len(cleaned) == 11 and cleaned[2] in '9876':
        result = f"+55{cleaned}"
        logger.info(f"Row {row_index + 1}: Valid {field_name} (mobile) [raw: {raw_value}, result: {result}]")
        return result, None
    elif len(cleaned) == 10:
        result = f"+55{cleaned}"
        logger.info(f"Row {row_index + 1}: Valid {field_name} (landline) [raw: {raw_value}, result: {result}]")
        return result, None
    
    error_reason = f"{field_name} inválido (10 dígitos para fixo, 11 dígitos com terceiro dígito após DDD como 9/8/7/6 para móvel)."
    logger.warning(f"Row {row_index + 1}: Invalid {field_name} [raw: {raw_value}, cleaned: {cleaned}]")
    return None, error_reason

def clean_email(email, row_index):
    """
    Validate email format.
    Returns cleaned email or None, with error reason if applicable.
    """
    raw_value = email
    error_reason = None
    if pd.isna(email) or email is None or str(email).strip() == '':
        logger.info(f"Row {row_index + 1}: Email missing or empty [raw: {raw_value}]")
        return None, None
    
    email = str(email).strip()
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_regex, email):
        error_reason = "Formato de email inválido."
        logger.warning(f"Row {row_index + 1}: Invalid email format [raw: {raw_value}, cleaned: {email}]")
        return None, error_reason
    
    logger.info(f"Row {row_index + 1}: Valid email [raw: {raw_value}, cleaned: {email}]")
    return email, None

def clean_cep(cep, row_index):
    """
    Clean and normalize CEP to 8 digits.
    Returns cleaned CEP or '00000000', with error reason if applicable.
    """
    raw_value = cep
    error_reason = None
    if pd.isna(cep) or cep is None or str(cep).strip() == '':
        error_reason = "CEP ausente ou vazio."
        logger.warning(f"Row {row_index + 1}: CEP missing or empty [raw: {raw_value}]")
        return '00000000', error_reason
    
    cep = re.sub(r'[^\d]', '', str(cep))
    if not cep.isdigit():
        error_reason = "CEP inválido (contém caracteres não numéricos)."
        logger.warning(f"Row {row_index + 1}: Invalid CEP (non-numeric) [raw: {raw_value}, cleaned: {cep}]")
        return '00000000', error_reason
    
    if len(cep) != 8:
        if len(cep) < 8:
            cep = cep.zfill(8)
            logger.info(f"Row {row_index + 1}: Padded CEP to 8 digits [raw: {raw_value}, cleaned: {cep}]")
        else:
            error_reason = "Comprimento de CEP inválido (deve ter 8 dígitos)."
            logger.warning(f"Row {row_index + 1}: Invalid CEP length [raw: {raw_value}, cleaned: {cep}]")
            return '00000000', error_reason
    
    logger.info(f"Row {row_index + 1}: Valid CEP [raw: {raw_value}, cleaned: {cep}]")
    return cep, None

def encode_string(value, max_length=None, default=None):
    """
    Encode string to UTF-8, truncate if necessary.
    Returns encoded string or default, with error reason if applicable.
    """
    raw_value = value
    error_reason = None
    if pd.isna(value) or value is None or str(value).strip() == '':
        logger.info(f"Encoding: Value missing or empty [raw: {raw_value}]")
        return default, None
    
    try:
        value_str = str(value).strip().encode('utf-8', errors='replace').decode('utf-8')
        if not value_str:
            logger.info(f"Encoding: Value empty after stripping [raw: {raw_value}]")
            return default, None
        if max_length and len(value_str) > max_length:
            value_str = value_str[:max_length]
            logger.info(f"Encoding: Truncated value to {max_length} chars [raw: {raw_value}, cleaned: {value_str}]")
        logger.info(f"Encoding: Successfully encoded value [raw: {raw_value}, cleaned: {value_str}]")
        return value_str, None
    except Exception as e:
        error_reason = f"Erro de codificação: {e}."
        logger.warning(f"Encoding error [raw: {raw_value}]: {e}")
        return default, error_reason

def normalize_uf(uf, row_index):
    """
    Normalize UF to 2-letter code.
    Returns normalized UF or 'XX', with error reason if applicable.
    """
    raw_value = uf
    error_reason = None
    if pd.isna(uf) or uf is None or str(uf).strip() == '':
        error_reason = "UF ausente ou vazio."
        logger.warning(f"Row {row_index + 1}: UF missing or empty [raw: {raw_value}]")
        return 'XX', error_reason
    
    uf = str(uf).strip().upper()
    if len(uf) == 2 and uf in UF_MAPPING.values():
        logger.info(f"Row {row_index + 1}: Valid UF [raw: {raw_value}, cleaned: {uf}]")
        return uf, None
    
    for key, value in UF_MAPPING.items():
        if uf == key or uf.lower() == key.lower():
            logger.info(f"Row {row_index + 1}: Normalized UF [raw: {raw_value}, from: {uf}, to: {value}]")
            return value, None
    
    error_reason = "UF inválido."
    logger.warning(f"Row {row_index + 1}: Invalid UF [raw: {raw_value}, cleaned: {uf}]")
    return 'XX', error_reason

def validate_dia_vencimento(dia, row_index):
    """
    Validate day of payment (1-31).
    Returns validated day or 1, with error reason if applicable.
    """
    raw_value = dia
    error_reason = None
    if pd.isna(dia) or dia is None or str(dia).strip() == '':
        error_reason = "Dia de vencimento ausente ou vazio."
        logger.warning(f"Row {row_index + 1}: Dia de vencimento missing or empty [raw: {raw_value}]")
        return 1, error_reason
    
    try:
        dia = int(float(str(dia).strip()))
        if dia < 1 or dia > 31:
            error_reason = "Dia de vencimento inválido (deve ser entre 1 e 31)."
            logger.warning(f"Row {row_index + 1}: Invalid dia de vencimento [raw: {raw_value}, cleaned: {dia}]")
            return 1, error_reason
        logger.info(f"Row {row_index + 1}: Valid dia de vencimento [raw: {raw_value}, cleaned: {dia}]")
        return dia, None
    except (ValueError, TypeError):
        error_reason = "Dia de vencimento não é um número."
        logger.warning(f"Row {row_index + 1}: Dia de vencimento is not a number [raw: {raw_value}]")
        return 1, error_reason

def validate_plano_valor(valor, row_index):
    """
    Validate plano valor as a float.
    Returns validated value or 0.0, with error reason if applicable.
    """
    raw_value = valor
    error_reason = None
    if pd.isna(valor) or valor is None or str(valor).strip() == '':
        error_reason = "Plano Valor ausente ou vazio."
        logger.warning(f"Row {row_index + 1}: Plano Valor missing or empty [raw: {raw_value}]")
        return 0.0, error_reason
    
    try:
        valor_str = str(valor).replace(',', '')
        result = float(valor_str)
        logger.info(f"Row {row_index + 1}: Valid Plano Valor [raw: {raw_value}, cleaned: {result}]")
        return result, None
    except (ValueError, TypeError):
        error_reason = "Plano Valor inválido."
        logger.warning(f"Row {row_index + 1}: Invalid Plano Valor [raw: {raw_value}]")
        return 0.0, error_reason

def validate_isento(isento, row_index):
    """
    Validate isento field.
    Returns boolean or False, with error reason if applicable.
    """
    raw_value = isento
    error_reason = None
    if pd.isna(isento) or isento is None or str(isento).strip() == '':
        logger.info(f"Row {row_index + 1}: Isento missing or empty [raw: {raw_value}]")
        return False, None
    
    isento_str = str(isento).strip().lower()
    if isento_str in ('sim', 's', 'yes', 'true', '1'):
        logger.info(f"Row {row_index + 1}: Isento set to True [raw: {raw_value}, cleaned: {isento_str}]")
        return True, None
    if isento_str in ('não', 'nao', 'n', 'no', 'false', '0'):
        logger.info(f"Row {row_index + 1}: Isento set to False [raw: {raw_value}, cleaned: {isento_str}]")
        return False, None
    
    error_reason = f"Valor de Isento inválido ({isento_str})."
    logger.warning(f"Row {row_index + 1}: Invalid Isento value [raw: {raw_value}, cleaned: {isento_str}]")
    return False, error_reason

def run_tests():
    """Run tests for all validation functions and export results to Excel."""
    print("Starting validation tests...\n")

    # Sample test data
    test_data = [
        {'CPF/CNPJ': '123.456.789-09', 'Data Nasc.': 44562, 'Celulares': '11987654321', 'Emails': 'test@example.com', 'CEP': '12345678', 'UF': 'São Paulo', 'Vencimento': 15, 'Plano Valor': '100.50', 'Isento': 'Sim'},  # Invalid CPF
        {'CPF/CNPJ': '529.982.247-25', 'Data Nasc.': '01/01/2022', 'Celulares': '+5511987654321', 'Emails': '  test@example.com  ', 'CEP': '12345', 'UF': 'SP', 'Vencimento': '15', 'Plano Valor': '1,234.56', 'Isento': '1'},  # Valid
        {'CPF/CNPJ': '12.345.678/0001-95', 'Data Nasc.': '2022-01-01', 'Celulares': '1133334444', 'Emails': 'invalid-email', 'CEP': '12345-678', 'UF': 'ZZ', 'Vencimento': 'invalid', 'Plano Valor': 'invalid', 'Isento': 'No'},  # Mixed errors
        {'CPF/CNPJ': None, 'Data Nasc.': 'invalid', 'Celulares': '12345', 'Emails': None, 'CEP': 'abc', 'UF': None, 'Vencimento': 32, 'Plano Valor': None, 'Isento': 'maybe'},  # Multiple missing/invalid
    ]
    
    df = pd.DataFrame(test_data)
    errors_list = []
    success_list = []
    total_erros = 0

    # Process each row
    for index, row in df.iterrows():
        error_row = row.copy()
        error_reasons = []
        is_valid = True

        # Test clean_cpf_cnpj
        cpf_cnpj, cpf_error = clean_cpf_cnpj(row['CPF/CNPJ'], index)
        if cpf_error:
            error_reasons.append(cpf_error)
            is_valid = False

        # Test convert_excel_date
        data_nasc, date_error = convert_excel_date(row['Data Nasc.'], index, 'Data Nasc.')
        if date_error:
            error_reasons.append(date_error)
            is_valid = False

        # Test clean_phone
        celular, phone_error = clean_phone(row['Celulares'], index, 'Celulares')
        if phone_error:
            error_reasons.append(phone_error)
            is_valid = False

        # Test clean_email
        email, email_error = clean_email(row['Emails'], index)
        if email_error:
            error_reasons.append(email_error)
            is_valid = False

        # Test clean_cep
        cep, cep_error = clean_cep(row['CEP'], index)
        if cep_error:
            error_reasons.append(cep_error)
            is_valid = False

        # Test normalize_uf
        uf, uf_error = normalize_uf(row['UF'], index)
        if uf_error:
            error_reasons.append(uf_error)
            is_valid = False

        # Test validate_dia_vencimento
        vencimento, vencimento_error = validate_dia_vencimento(row['Vencimento'], index)
        if vencimento_error:
            error_reasons.append(vencimento_error)
            is_valid = False

        # Test validate_plano_valor
        plano_valor, valor_error = validate_plano_valor(row['Plano Valor'], index)
        if valor_error:
            error_reasons.append(valor_error)
            is_valid = False

        # Test validate_isento
        isento, isento_error = validate_isento(row['Isento'], index)
        if isento_error:
            error_reasons.append(isento_error)
            is_valid = False

        # Log results
        if is_valid:
            success_row = row.copy()
            success_row['Motivo do Erro'] = ''
            success_list.append(success_row)
            logger.info(f"Row {index + 1}: All validations passed")
        else:
            error_row['Motivo do Erro'] = "; ".join(error_reasons)
            errors_list.append(error_row)
            total_erros += 1
            logger.warning(f"Row {index + 1}: Validation errors: {error_row['Motivo do Erro']}")

    # Save reports to Excel
    columns = df.columns.tolist() + ['Motivo do Erro']
    errors_df = pd.DataFrame(errors_list, columns=columns) if errors_list else pd.DataFrame(columns=columns)
    success_df = pd.DataFrame(success_list, columns=columns) if success_list else pd.DataFrame(columns=columns)

    # Save errors report
    if not errors_df.empty:
        errors_header = pd.DataFrame([columns], columns=columns)
        errors_final_df = pd.concat([errors_header, errors_df], ignore_index=True)
        errors_final_df.to_excel(ERRORS_FILE, index=False)
        logger.info(f"Errors report saved to '{ERRORS_FILE}'")

    # Save success report
    if not success_df.empty:
        success_title = pd.DataFrame([['Registros Válidos'] + [''] * (len(columns) - 1)], columns=columns)
        success_header = pd.DataFrame([columns], columns=columns)
        success_final_df = pd.concat([success_title, success_header, success_df], ignore_index=True)
        success_final_df.to_excel(SUCCESS_FILE, index=False)
        logger.info(f"Success report saved to '{SUCCESS_FILE}'")

    # Log final metrics
    logger.info(f"Total de registros processados: {len(df)}")
    logger.info(f"Total de registros válidos: {len(success_list)}")
    logger.info(f"Total de erros: {total_erros}")

    print(f"\nTests completed. Check data_validation.log for detailed logs.")
    print(f"Errors report saved to: {ERRORS_FILE}")
    print(f"Success report saved to: {SUCCESS_FILE}")

if __name__ == "__main__":
    run_tests()