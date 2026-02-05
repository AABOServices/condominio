import re
import pandas as pd
import sqlite3

DB_PATH = "condominio.db"

ARCHIVOS = [
    "012025.csv", "022025.csv", "032025.csv", "042025.csv", "052025.csv", "062025.csv",
    "072025.csv", "082025.csv", "092025.csv", "102025.csv", "112025.csv", "122025.csv"
]

USUARIO_CARGA = "SISTEMA_CARGA"


# ---------- Helpers ----------
def norm_cell(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def money_to_float(x) -> float:
    if pd.isna(x):
        return 0.0
    s = str(x).strip()

    neg = False
    if "(" in s and ")" in s:
        neg = True
        s = s.replace("(", "").replace(")", "")

    s = s.replace("$", "").replace(" ", "").replace("\u00a0", "")
    if s in ["-", "—", ""]:
        return 0.0

    s = s.replace(",", "")
    try:
        val = float(s)
        return -val if neg else val
    except:
        return 0.0


def parse_fecha(valor_fecha, archivo: str) -> str:
    # Intenta leer fecha real
    if not pd.isna(valor_fecha):
        s = norm_cell(valor_fecha)
        if s and s != "0":
            dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
            if pd.notna(dt):
                return dt.strftime("%Y-%m-%d")

    # Fallback: fecha primer día del mes según archivo MMYYYY.csv
    m = re.search(r"(\d{2})(\d{4})", archivo)
    if m:
        mm = int(m.group(1))
        yyyy = int(m.group(2))
        return f"{yyyy:04d}-{mm:02d}-01"

    return "2025-01-01"


def yyyymm(fecha_yyyy_mm_dd: str) -> str:
    return fecha_yyyy_mm_dd[:7]


def extraer_codigo_casa(descripcion: str) -> str:
    s = norm_cell(descripcion)
    if not s:
        return ""

    m = re.match(r"^(C\d{2})\b", s, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()

    m = re.match(r"^(\d{2})\b", s)
    if m:
        return f"C{m.group(1)}"

    return ""


def es_fila_pago_principal(descripcion: str) -> bool:
    s = norm_cell(descripcion).lower()
    if not s:
        return False

    # Excluir sublíneas Provisión/Décimos/Sueldo
    if "provisi" in s or "décim" in s or "decim" in s or "sueldo" in s:
        return False

    return bool(re.match(r"^(c\d{2}|\d{2})\b", s))


def calcular_fondos(monto: float):
    prov = round(monto * 0.0338, 2)
    deci = round(monto * 0.045, 2)
    suel = round(monto * 0.20, 2)
    oper = round(monto - prov - deci - suel, 2)
    return prov, deci, suel, oper


# ---------- Overwrite por Casa+Mes ----------
def borrar_mes_casa(cur, casa: str, mes_yyyy_mm: str):
    """
    Borra registros previos de importación automática (SISTEMA_CARGA)
    para esa casa y mes, y luego se reinsertan.
    """
    cur.execute(
        """
        DELETE FROM pagos
        WHERE casa = ?
          AND usuario = ?
          AND strftime('%Y-%m', fecha) = ?
        """,
        (casa, USUARIO_CARGA, mes_yyyy_mm)
    )


def importar_historico_overwrite():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO pagos (casa, fecha, monto, provision, decimos, sueldo, operativo, usuario)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    total_insertados = 0

    for archivo in ARCHIVOS:
        try:
            df = pd.read_csv(archivo, skiprows=2, encoding="latin1")
            cols = list(df.columns)

            col_tipo = cols[0]
            col_desc = cols[1]
            col_fecha = cols[3]
            col_entra = cols[4]

            # localizar bloque INGRESOS
            idx_ing = df.index[df[col_tipo].fillna("").astype(str).str.strip().eq("INGRESOS")]
            if len(idx_ing) == 0:
                print(f"⚠️ No se encontró sección INGRESOS en {archivo}")
                continue

            start = int(idx_ing[0])

            # fin bloque INGRESOS
            end = len(df)
            for i in range(start + 1, len(df)):
                v = norm_cell(df.loc[i, col_tipo])
                if v and v != "INGRESOS":
                    end = i
                    break

            bloque = df.iloc[start:end].copy()

            # Primero recolectamos filas “principal
