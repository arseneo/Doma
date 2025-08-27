import os
import re
import sys
import logging
from collections import defaultdict, Counter
import pandas as pd
import glob

# ========== Настройки путей и параметров ==========
CURRENT_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(CURRENT_DIR, "Обработанные файлы")
os.makedirs(OUTPUT_DIR, exist_ok=True)

INPUT_XLSX = (glob.glob(os.path.join(CURRENT_DIR, "Дома*.xlsx")) or [None])[0]
INN_XLSX = (glob.glob(os.path.join(CURRENT_DIR, "Реестр поставщиков информации*.xlsx")) or [None])[0]
CSV_DIR = CURRENT_DIR

OUTPUT_XLSX = os.path.join(OUTPUT_DIR, "result.xlsx")
MISSED_CSV = os.path.join(OUTPUT_DIR, "missed.csv")

CHUNK_SIZE = 200_000
PROGRESS_EVERY = 500

if not INPUT_XLSX:
    print("Не найден файл с адресами (Дома*.xlsx)")
    sys.exit(1)
if not INN_XLSX:
    print("Не найден файл с ИНН (Реестр поставщиков информации*.xlsx)")
    sys.exit(1)

# ========== Логирование ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ========== Сокращения ==========
REPLACEMENTS = [
    (r'\bул\.?\b', 'улица'), (r'\bу\.?\b', 'улица'),
    (r'\bшос\.?\b', 'шоссе'), (r'\bш\b', 'шоссе'), (r'\bш\.\b', 'шоссе'),
    (r'\bпр-д\b', 'проезд'), (r'\bпрд\.?\b', 'проезд'), (r'\bпр\.\b', 'проезд'),
    (r'\bпр-т\.?\b', 'проспект'), (r'\bпр-кт\.?\b', 'проспект'), (r'\bпросп\.?\b', 'проспект'),
    (r'\bтуп\.?\b', 'тупик'), (r'\bнаб\.?\b', 'набережная'),
    (r'\bбул\.?\b', 'бульвар'), (r'\bб-р\b', 'бульвар'), (r'\bбульв\.?\b', 'бульвар'),
    (r'\bпер\.?\b', 'переулок'), (r'\bпл\.?\b', 'площадь'),
    (r'\bмкр[н]?\.?\b', 'микрорайон'), (r'\bмкр-?[н]\b', 'микрорайон'), (r'\bмкр\b', 'микрорайон'),
    (r'\bкв-л\b', 'квартал'), (r'\bкварт\.?\b', 'квартал'),
    (r'\bаллея\b', 'аллея'), (r'\bалл?\.?\b', 'аллея'), (r'\bпарк\b', 'парк'),
    (r'\bлиния\b', 'линия'), (r'\bдер\.?\b', 'деревня'), (r'\bд\.\b', 'деревня'),
    (r'\bпос\.?\b', 'поселок'), (r'\bпгт\b', 'поселок'), (r'\bрп\b', 'поселок'),
    (r'\bснт\b', 'садовое товарищество'), (r'\bднп\b', 'дачное неком-е партнерство'),
    (r'\bтер\.?\b', 'территория'),
    (r'\bб\.?\b', 'большая'), (r'\bбол\.?\b', 'большая'), (r'\bбольшой\b', 'большая'),
    (r'\bм\.?\b', 'малая'), (r'\bмал\.?\b', 'малая'), (r'\bмалый\b', 'малая'),
    (r'\bверх\.?\b', 'верхняя'), (r'\bверхн\.?\b', 'верхняя'), (r'\bниж\.?\b', 'нижняя'), (r'\bнижн\.?\b', 'нижняя'),
    (r'\bср\.?\b', 'средняя'), (r'\bсред\.?\b', 'средняя'), (r'\bсредн\.?\b', 'средняя'), (r'\bсредний\b', 'средняя'), (r'\bсреднее\b', 'средняя'),
    (r'\bстр\.?\b', 'строение'), (r'\bкорп\.?\b', 'корпус'), (r'\bлит\.?\b', 'литера'),
    (r'\bжк\b', 'жилой комплекс'), (r'\bкомпл\.?\b', 'комплекс'),
]

SERVICE_WORDS = {
    'россия', 'рф', 'обл', 'область', 'г', 'город', 'р-н', 'район', 'м о', 'московская',
    'мо', 'поселение', 'пос', 'пгт', 'рп', 'д', 'дер', 'деревня', 'снт', 'днп', 'тер', 'территория',
    'округ', 'городской', 'гп'
}

CITY_REMOVE = re.compile(r'\bмосква\b', re.IGNORECASE)
ADDR_LABELS = re.compile(r'\b(д\.?|дом|влад(?:ение)?|участок)\b', re.IGNORECASE)
ADJ_DISTRICT_RE = re.compile(
    r'\b[а-яё]+ск(?:ий|ая|ое)\s+(?:район|городской\s+округ)\b',
    re.IGNORECASE
)
GO_ABBR_RE = re.compile(r'\bг\s*\.?\s*о\s*\.?\b', re.IGNORECASE)

HOUSE_TOKEN_RE = re.compile(r'^\d+[а-я]?(?:[\/-]\d+[а-я]?)?$', re.IGNORECASE)
HOUSE_KEYWORDS = {'строение', 'стр', 'корпус', 'корп', 'к', 'литера', 'лит'}

# ========== Вспомогательные функции ==========
def clean_field(x):
    """Привести поле к безопасной строке: NaN/None/'nan' -> ''"""
    if x is None:
        return ''
    if pd.isna(x):
        return ''
    s = str(x).strip()
    if s.lower() == 'nan':
        return ''
    return s

def inn_digits_only(v):
    s = '' if pd.isna(v) else str(v)
    return re.sub(r'\D', '', s)

# ========== НОРМАЛИЗАЦИЯ АДРЕСОВ ==========
def split_attached_numbers(s: str) -> str:
    s = re.sub(r'(\d)(?=(строение|стр|корпус|корп|к|литера|лит))', r'\1 ', s)
    s = re.sub(r'\bстр(?=\d)', 'строение ', s)
    s = re.sub(r'\b(строение|стр|корпус|корп|k|литера|лит)\s*([0-9]+[а-я]?)\b', r'\1 \2', s)
    s = re.sub(r'(\d+)\s*к(?=\d)', r'\1 корпус ', s)
    return s

def normalize_address(addr: str) -> str:
    if not isinstance(addr, str):
        addr = '' if pd.isna(addr) else str(addr)
    s = addr.strip().lower().replace('ё', 'е')

    # Убираем "г.о." (городской округ) до прочих замен
    s = GO_ABBR_RE.sub(' ', s)
    # Удаляем возможный индекс в начале
    s = re.sub(r'^\s*\d{5,6}[,\s]+', ' ', s)

    for pat, repl in REPLACEMENTS:
        s = re.sub(pat, repl, s)

    s = split_attached_numbers(s)
    s = CITY_REMOVE.sub(' ', s)

    # Удаляем конструкции типа "Красногорский район" целиком
    s = ADJ_DISTRICT_RE.sub(' ', s)

    for word in SERVICE_WORDS:
        s = re.sub(r'\b' + re.escape(word) + r'\b', ' ', s)

    s = ADDR_LABELS.sub(' ', s)
    s = re.sub(r'(\d+)\s*[\/-]\s*([0-9]+[а-я]?)', r'\1/\2', s)
    s = re.sub(r'[;,.:]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def extract_house_and_street_tokens(norm: str):
    tokens = norm.split()
    if not tokens:
        return set(), ''
    start_idx = None
    # ищем справа-налево начало домовой части (чтобы не принять "микрорайон 4" за дом)
    for i in range(len(tokens) - 1, -1, -1):
        t = tokens[i]
        if t in HOUSE_KEYWORDS:
            start_idx = i
            break
        if HOUSE_TOKEN_RE.match(t):
            prev = tokens[i - 1] if i > 0 else ''
            if prev in {'микрорайон', 'квартал', 'линия'}:
                continue
            next_t = tokens[i + 1] if i + 1 < len(tokens) else ''
            if i >= len(tokens) - 2 or next_t in HOUSE_KEYWORDS:
                start_idx = i
                break
    if start_idx is None:
        return set(tokens), ''
    street = tokens[:start_idx]
    house_parts = tokens[start_idx:]
    return set(street), ' '.join(house_parts)

def make_key(norm: str) -> str:
    street_set, house = extract_house_and_street_tokens(norm)
    street_part = ' '.join(sorted(street_set))
    return f"{street_part} {house}".strip() if house else street_part

# ========== НОРМАЛИЗАЦИЯ НАЗВАНИЙ ОРГАНИЗАЦИЙ ==========
LEGAL_FORMS_RE = re.compile(
    r'\b('
    r'муниципальн\w*\s+(?:унитарн\w*\s+)?предприяти\w*|'
    r'муниципальн\w*\s+бюджетн\w*\s+учреждени\w*|'
    r'муниципальн\w*\s+казенн\w*\s+учреждени\w*|'
    r'управляющ(?:ая|ую)|управляющая(?:\s+компан(?:ия|ии))?|управляющая\s+организац\w*'
    r')\b',
    re.IGNORECASE
)
QUOTES_RE = re.compile(r'[\"\'«»“”„]')

def org_norm(s: str) -> str:
    if not isinstance(s, str):
        s = '' if pd.isna(s) else str(s)
    s = s.strip().lower().replace('ё', 'е')
    s = QUOTES_RE.sub(' ', s)
    s = LEGAL_FORMS_RE.sub(' ', s)
    s = re.sub(r'[.,()\\/]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def org_tokens(s: str) -> set:
    return {t for t in org_norm(s).split() if len(t) > 1 and t not in {
        'ук', 'уо', 'жкх', 'жку', 'жк', 'компания', 'организация', 'дирекция', 'управляющая', 'управление'
    }}

# ========== Утилиты поиска колонок и чтения CSV ==========
def find_column(df, patterns):
    low = {c: re.sub(r'\s+', ' ', str(c)).strip().lower() for c in df.columns}
    for pat in patterns:
        reg = re.compile(pat, re.IGNORECASE)
        for c, name in low.items():
            if reg.search(name):
                return c
    return None

def read_csv_chunks_robust(path, chunksize):
    for enc in ('utf-8', 'cp1251', 'utf-8-sig'):
        for sep in ('|', ';', ',', None):
            try:
                return pd.read_csv(path, sep=sep, dtype=str, encoding=enc, engine='python', chunksize=chunksize)
            except Exception:
                continue
    raise RuntimeError(f"Не удалось прочитать CSV {path}")

# ========== Построение индекса адресов ==========
def build_addr_dict_from_csv():
    addr_dict = {}
    house_index = defaultdict(list)

    files = [os.path.join(CSV_DIR, f) for f in os.listdir(CSV_DIR) if f.lower().endswith('.csv')]
    files.sort()
    logger.info(f"Найдено CSV файлов: {len(files)}")
    if not files:
        logger.error("Не найдено CSV-файлов")
        sys.exit(1)

    for fi, path in enumerate(files, 1):
        logger.info(f"Обработка ({fi}/{len(files)}) {os.path.basename(path)}")
        try:
            df_iter = read_csv_chunks_robust(path, CHUNK_SIZE)
        except Exception as e:
            logger.warning(f"Пропускаю {path}: {e}")
            continue

        for chunk in df_iter:
            addr_col = find_column(chunk, [r'адрес.*ожф', r'адрес\s*дома', r'\bадрес\b'])
            org_col = find_column(chunk, [r'наимен.*управ', r'управляющ.*организац'])
            ogrn_col = find_column(chunk, [r'\bогрн\b'])

            if not addr_col or not org_col:
                logger.warning(f"В файле {os.path.basename(path)} не найдены необходимые колонки")
                continue

            for _, row in chunk.iterrows():
                raw_addr = clean_field(row.get(addr_col, ''))
                org = clean_field(row.get(org_col, ''))
                ogrn = clean_field(row.get(ogrn_col, '')) if ogrn_col else ''
                ogrn = inn_digits_only(ogrn) if ogrn else ''

                norm = normalize_address(raw_addr)
                if not norm:
                    continue
                key = make_key(norm)
                if not key:
                    continue

                addr_dict[key] = (org, ogrn)
                street_set, house = extract_house_and_street_tokens(norm)
                if house:
                    house_index[house].append((street_set, key, org, ogrn))

    logger.info(f"Построен индекс адресов: ключей={len(addr_dict)}, домов={len(house_index)}")
    return addr_dict, house_index

# ========== Построение индекса ИНН (с fallback удаления 2 строк шапки) ==========
def build_inn_lookup():
    logger.info("Загрузка файла ИНН...")
    # Попробуем несколько вариантов чтения: обычный, с пропуском 2 строк, либо с другим header
    tried_variants = []
    inn_df = None
    for attempt in range(3):
        try:
            if attempt == 0:
                inn_df_try = pd.read_excel(INN_XLSX, dtype=str)
                tried_variants.append("header=0")
            elif attempt == 1:
                # Если есть две служебные строки сверху - пропускаем
                inn_df_try = pd.read_excel(INN_XLSX, dtype=str, skiprows=2)
                tried_variants.append("skiprows=2")
            else:
                # Попробуем найти заголовок в первых трёх строк: header=1 или header=2
                # перебор header=1,2
                inn_df_try = None
                for header_row in (1, 2):
                    try:
                        temp = pd.read_excel(INN_XLSX, dtype=str, header=header_row)
                        if temp is not None:
                            # проверим наличие основных колонок
                            full_col = find_column(temp, [r'полное.*наимен', r'\bнаимен'])
                            short_col = find_column(temp, [r'сокращ.*наимен', r'\bсокр'])
                            inn_col = find_column(temp, [r'\bинн\b'])
                            if full_col and short_col and inn_col:
                                inn_df_try = temp
                                tried_variants.append(f"header={header_row}")
                                break
                    except Exception:
                        continue
                if inn_df_try is None:
                    # последнее средство: просто прочитать с header=None и возьмём первый набор
                    inn_df_try = pd.read_excel(INN_XLSX, dtype=str, header=None)
                    tried_variants.append("header=None")
            # Проверим на наличие колонок
            if inn_df_try is None:
                continue
            # Подготовка: приведём NaN -> '' для безопасной обработки
            inn_df_try = inn_df_try.fillna('')
            # Найдём колонки
            full_col = find_column(inn_df_try, [r'полное.*наимен', r'\bнаимен'])
            short_col = find_column(inn_df_try, [r'сокращ.*наимен', r'\bсокр'])
            inn_col = find_column(inn_df_try, [r'\bинн\b'])
            if full_col and short_col and inn_col:
                inn_df = inn_df_try
                break
            else:
                # если попытка 0 не обнаружила — попробуем следующую (skiprows=2)
                continue
        except Exception as e:
            logger.warning(f"Попытка чтения ИНН ({attempt}) не удалась: {e}")
            continue

    if inn_df is None:
        logger.error(f"Не удалось прочитать и определить колонки ИНН (пробовали: {tried_variants})")
        sys.exit(1)

    # Обнаруженные колонки (точные имена)
    full_col = find_column(inn_df, [r'полное.*наимен', r'\bнаимен'])
    short_col = find_column(inn_df, [r'сокращ.*наимен', r'\bсокр'])
    inn_col = find_column(inn_df, [r'\bинн\b'])
    ogrn_col = find_column(inn_df, [r'\bогрн\b'])
    site_col = find_column(inn_df, [r'официальный.*сайт', r'сайт'])
    phone_col = find_column(inn_df, [r'телефон'])
    email_col = find_column(inn_df, [r'адрес.*электрон', r'e-mail', r'email'])

    # Соберём индексы
    exact_index = {}   # точное совпадение названия
    clean_index = {}   # нормализованное название
    ogrn_index = {}    # по ОГРН
    token_to_keys = defaultdict(set)

    def tpl_from_row(r):
        full_raw = clean_field(r.get(full_col, '')) if full_col else ''
        short_raw = clean_field(r.get(short_col, '')) if short_col else ''
        inn = inn_digits_only(r.get(inn_col, '')) if inn_col else ''
        ogrn = inn_digits_only(r.get(ogrn_col, '')) if ogrn_col else ''
        site = clean_field(r.get(site_col, '')) if site_col else ''
        phone = clean_field(r.get(phone_col, '')) if phone_col else ''
        email = clean_field(r.get(email_col, '')) if email_col else ''
        display_short = short_raw if short_raw else full_raw
        return (inn, display_short, ogrn, site, phone, email, full_raw, short_raw)

    for _, row in inn_df.iterrows():
        tpl = tpl_from_row(row)
        inn_val, display_short, ogrn_val, site_val, phone_val, email_val, full_raw, short_raw = tpl

        # Добавляем в exact_index по полному и по короткому (если не пусто)
        if full_raw:
            key_full = full_raw.lower().replace('ё', 'е').strip()
            if key_full:
                exact_index[key_full] = tpl
            clean = org_norm(full_raw)
            if clean:
                clean_index.setdefault(clean, []).append(tpl)
                for tok in org_tokens(full_raw):
                    token_to_keys[tok].add(clean)

        if short_raw:
            key_short = short_raw.lower().replace('ё', 'е').strip()
            if key_short:
                exact_index[key_short] = tpl
            clean_s = org_norm(short_raw)
            if clean_s:
                clean_index.setdefault(clean_s, []).append(tpl)
                for tok in org_tokens(short_raw):
                    token_to_keys[tok].add(clean_s)

        if ogrn_val:
            ogrn_index[ogrn_val] = tpl

    logger.info(f"Построен индекс ИНН: exact={len(exact_index)}, clean={len(clean_index)}, ogrn={len(ogrn_index)}")
    return exact_index, clean_index, ogrn_index, token_to_keys

# ========== Поиск организации по адресу ==========
def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0

def find_org_for_address(raw_addr: str, addr_dict: dict, house_index: dict):
    norm = normalize_address(raw_addr)
    key = make_key(norm)

    # 1 точное совпадение
    if key in addr_dict:
        org, ogrn = addr_dict[key]
        if org:
            return org, 'exact', key, ogrn

    # 2 без 'мо'
    key_wo_mo = re.sub(r'\bмо\b', '', key, flags=re.IGNORECASE).strip()
    key_wo_mo = re.sub(r'\s+', ' ', key_wo_mo)
    if key_wo_mo and key_wo_mo in addr_dict:
        org, ogrn = addr_dict[key_wo_mo]
        if org:
            return org, 'without_mo', key_wo_mo, ogrn

    # 3 по номеру дома
    street_set, house = extract_house_and_street_tokens(norm)
    if house and not any(w in house for w in ['строение', 'стр', 'корпус', 'корп', 'литера', 'лит']):
        candidates = house_index.get(house, [])
        best_match = None
        best_score = 0.0
        for st_set, cand_key, org, ogrn in candidates:
            score = jaccard(street_set, st_set)
            if score > best_score:
                best_score = score
                best_match = (org, f'street_match({int(score*100)}%)', cand_key, ogrn)
        if best_match and best_score >= 0.60:
            return best_match

    # 4 без окончаний
    key2 = re.sub(r'\b[а-яё]+ск(?:ий|ая|ое)\b', ' ', key).strip()
    key2 = re.sub(r'\s+', ' ', key2)
    if key2 and key2 in addr_dict:
        org, ogrn = addr_dict[key2]
        if org:
            return org, 'adj_removed', key2, ogrn

    return '', '', key, None

# ========== Поиск ИНН по названию организации ==========
def org_type_bonus(src_name: str, cand_full: str, cand_short: str) -> float:
    s = (src_name or '').lower()
    cand = ((cand_full or '') + ' ' + (cand_short or '')).lower()
    bonus = 0.0
    for token, b in (('тсж', 0.08), ('жск', 0.06), ('ук', 0.05)):
        if token in s and token in cand:
            bonus = max(bonus, b)
    return bonus

def find_inn_for_org(org: str, ogrn: str,
                     exact_index: dict, clean_index: dict,
                     ogrn_index: dict, token_to_keys: dict):
    if not org:
        return '', '', '', '', '', 'no_org'

    org_lower = clean_field(org).lower().replace('ё', 'е')
    org_clean = org_norm(org)
    org_tok = org_tokens(org)
    ogrn_digits = inn_digits_only(ogrn) if ogrn else ''

    # --- Ограничение по региону ---
    def region_ok(inn: str) -> bool:
        return inn.startswith("77") or inn.startswith("50") or inn.startswith("97")

    # 0 по огрн
    if ogrn_digits and ogrn_digits in ogrn_index:
        inn, short_name, index_ogrn, site, phone, email, full_raw, short_raw = ogrn_index[ogrn_digits]
        short_name = clean_field(short_name) or clean_field(full_raw) or ''
        if region_ok(inn):
            return inn, short_name, site, phone, email, 'ogrn_exact'
        else:
            return '', '', '', '', '', 'ogrn_wrong_region'

    # 1 идеальное сходство
    if org_lower in exact_index:
        tpl = exact_index[org_lower]
        inn, short_name, index_ogrn, site, phone, email, full_raw, short_raw = tpl
        short_name = clean_field(short_name) or clean_field(full_raw)
        if region_ok(inn):
            if ogrn_digits and index_ogrn and ogrn_digits == inn_digits_only(index_ogrn):
                return inn, short_name, site, phone, email, 'exact+ogrn'
            return inn, short_name, site, phone, email, 'name_exact'

    # 2 нормализованное сходство
    if org_clean in clean_index:
        candidates = [tpl for tpl in clean_index[org_clean] if region_ok(tpl[0])]
        if ogrn_digits:
            for tpl in candidates:
                inn, short_name, index_ogrn, site, phone, email, full_raw, short_raw = tpl
                if index_ogrn and ogrn_digits == inn_digits_only(index_ogrn):
                    short_name = clean_field(short_name) or clean_field(full_raw)
                    return inn, short_name, site, phone, email, 'clean+ogrn'
        # fallback: выбрать первое подходящее
        if candidates:
            inn, short_name, index_ogrn, site, phone, email, full_raw, short_raw = candidates[0]
            short_name = clean_field(short_name) or clean_field(full_raw)
            return inn, short_name, site, phone, email, 'clean_exact'

    # 3 fuzzy 
    candidate_keys = set()
    for t in org_tok:
        candidate_keys |= token_to_keys.get(t, set())
    keys_to_check = candidate_keys if candidate_keys else set(clean_index.keys())
    best_key = None
    best_score = 0.0
    best_tpl = None
    for key in keys_to_check:
        key_tokens = set(key.split())
        score = jaccard(org_tok, key_tokens)
        cand_tpl = clean_index.get(key, [None])[0]
        if cand_tpl:
            _, cand_short, _, _, _, _, cand_full_raw, cand_short_raw = cand_tpl
            score += org_type_bonus(org, cand_full_raw, cand_short_raw)
        if score > best_score:
            best_score = score
            best_key = key
            best_tpl = cand_tpl

    if best_tpl and best_score >= 0.62:
        inn, short_name, index_ogrn, site, phone, email, full_raw, short_raw = best_tpl
        if region_ok(inn):
            short_name = clean_field(short_name) or clean_field(full_raw) or ''
            if ogrn_digits:
                for tpl in clean_index.get(best_key, []):
                    inn2, short2, ogrn2, site2, phone2, email2, fraw, sraw = tpl
                    if ogrn2 and inn_digits_only(ogrn2) == ogrn_digits and region_ok(inn2):
                        short2 = clean_field(short2) or clean_field(fraw) or ''
                        return inn2, short2, site2, phone2, email2, f'fuzzy+ogrn({best_score:.2f})'
            return inn, short_name, site, phone, email, f'fuzzy({best_score:.2f})'

    return '', '', '', '', '', 'not_found'

# ========== Основной процесс ==========
def main():
    logger.info("=== НАЧАЛО РАБОТЫ ===")

    logger.info("ШАГ 1: Строим индекс адресов из CSV...")
    addr_dict, house_index = build_addr_dict_from_csv()
    if not addr_dict:
        logger.error("Пустой индекс адресов — выходим")
        sys.exit(1)

    logger.info("ШАГ 2: Строим индекс ИНН...")
    exact_index, clean_index, ogrn_index, token_to_keys = build_inn_lookup()

    logger.info("ШАГ 3: Обрабатываем входной файл...")
    try:
        df_in = pd.read_excel(INPUT_XLSX, dtype=str).fillna('')
        df_in = df_in.loc[:, ~df_in.columns.str.contains('^Unnamed*')]
    except Exception as e:
        logger.error(f"Ошибка при чтении входного файла: {e}")
        sys.exit(1)

    addr_col = find_column(df_in, [r'\bадрес\b'])
    if not addr_col:
        logger.error("Не найдена колонка с адресами во входном файле")
        sys.exit(1)

    # Гарантируем столбцы результата
    for col in ['Наименование УК', 'ИНН', 'Официальный сайт в сети Интернет', 'Телефон', 'Адрес электронной почты']:
        if col not in df_in.columns:
            df_in[col] = ''

    stats = Counter()
    missed_rows = []

    total = len(df_in)
    logger.info(f"Строк в входном файле: {total} (адрес в колонке: '{addr_col}')")

    for idx, row in df_in.iterrows():
        stats['total'] += 1
        raw_addr = clean_field(row.get(addr_col, ''))

        org, method_addr, matched_key, ogrn = find_org_for_address(raw_addr, addr_dict, house_index)

        written_uk = ''
        written_inn = ''
        written_site = ''
        written_phone = ''
        written_email = ''
        inn_method = ''

        if org:
            inn, short_name, site, phone, email, inn_method = find_inn_for_org(
                org, ogrn, exact_index, clean_index, ogrn_index, token_to_keys
            )
            short_name = clean_field(short_name)
            if inn:
                # выбираем корректное отображаемое имя: короткое (если есть), иначе исходное полное
                if short_name:
                    written_uk = short_name
                else:
                    written_uk = clean_field(org)
                written_inn = inn_digits_only(inn)
                written_site = clean_field(site)
                written_phone = clean_field(phone)
                written_email = clean_field(email)

                df_in.at[idx, 'Наименование УК'] = written_uk
                df_in.at[idx, 'ИНН'] = written_inn
                df_in.at[idx, 'Официальный сайт в сети Интернет'] = written_site
                df_in.at[idx, 'Телефон'] = written_phone
                df_in.at[idx, 'Адрес электронной почты'] = written_email

                status = 'OK'
                stats['ok'] += 1
            else:
                written_uk = clean_field(org)
                df_in.at[idx, 'Наименование УК'] = written_uk
                status = 'NO_INN'
                stats['no_inn'] += 1
        else:
            status = 'NOT_FOUND'
            stats['not_found'] += 1

        if status in ('NOT_FOUND', 'NO_INN'):
            missed_rows.append({
                'row_index': int(idx),
                'raw_address': raw_addr,
                'normalized': normalize_address(raw_addr),
                'key': matched_key,
                'found_org_from_csv': clean_field(org),
                'method_addr': method_addr,
                'inn_method': inn_method,
                'written_uk': written_uk,
                'written_inn': written_inn,
                'status': status,
                'ogrn': inn_digits_only(ogrn) if ogrn else ''
            })

        if stats['total'] % PROGRESS_EVERY == 0:
            logger.info(f"Обработано {stats['total']}/{total} | OK={stats['ok']} NOT_FOUND={stats['not_found']} NO_INN={stats['no_inn']}")

    logger.info("=== ИТОГИ ===")
    logger.info(f"Всего: {stats['total']}, OK={stats['ok']}, NOT_FOUND={stats['not_found']}, NO_INN={stats['no_inn']}")

    logger.info(f"Сохранение результата в {OUTPUT_XLSX}...")
    try:
        df_in.to_excel(OUTPUT_XLSX, index=False)
    except Exception as e:
        logger.error(f"Ошибка при сохранении результата: {e}")

    if missed_rows:
        logger.info(f"Сохранение пропусков в {MISSED_CSV} ({len(missed_rows)} записей)...")
        try:
            pd.DataFrame(missed_rows).to_csv(MISSED_CSV, index=False, encoding='utf-8-sig')
        except Exception as e:
            logger.error(f"Ошибка при сохранении пропущенных адресов: {e}")
    else:
        logger.info("Пропущенных адресов не обнаружено")

    logger.info("=== ГОТОВО ===")

if __name__ == "__main__":
    main()
