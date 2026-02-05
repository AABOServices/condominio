import os
import re
import sqlite3
import pandas as pd

DB_PATH = "condominio.db"

ARCHIVOS = [
    "012025.csv", "022025.csv", "032025.csv", "042025.csv", "052025.csv", "062025.csv",
    "072025.csv", "082025.csv", "092025.csv", "102025.csv", "112025.csv", "122025.csv"
]

# ------------------ Helpers ------------------
def norm_cell(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s


def money_to_float(x) -> float:
    """
    Convierte strings tipo '$279.57', '$-', '($224.65)', ' 279.50 ' -> float
    """
    if pd.isna(x):
        return 0.0
    s = str(x).strip()

    neg = False
    if "(" in s and ")" in s:
        neg = True
        s = s.replace("(", "").replace(")", "")

    s = s.replace("$", "").replace(",", "").replace("\u00a0", "").strip()
    if s in ["", "-", "—"]:
        return 0.0

    try:
        val = float(s)
        return -val if neg else val
    except Exception:
        return 0.0


def parse_fecha_excel(x, fallback_yyyy_mm: str) -> str:
    """
    Fecha de pago:
    - Si hay fecha en columna D, se parsea.
    - Si no hay (0 o vacío), se usa YYYY-MM-01 desde el archivo.
    """
    s = norm_cell(x)
    if s and s != "0":
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
    return f"{fallback_yyyy_mm}-01"


def archivo_a_yyyymm(archivo: str) -> str:
    """
    '072025.csv' -> '2025-07'
    """
    m = re.search(r"(\d{2})(\d{4})", archivo)
    if not m:
        return "2025-01"
    mm = int(m.group(1))
    yyyy = int(m.group(2))
    return f"{yyyy:04d}-{mm:02d}"


def is_fila_casa_principal(colB: str) -> bool:
    """
    Fila principal de casa: 'C01 Canelos - Pérez'
    Excluye: 'C01 Provisión', 'C01 Décimos...', 'C01 Sueldo...'
    """
    s = norm_cell(colB)
    if not s:
        return False

    if not re.match(r"^C\d{2}\b", s, flags=re.IGNORECASE):
        return False

    sl = s.lower()
    if "provisi" in sl or "décim" in sl or "decim" in sl or "sueldo" in sl:
        return False

    return True


def get_casa(colB: str) -> str:
    return norm_cell(colB).split()[0].upper()


def contiene_provision(colB: str) -> bool:
    return "provisi" in norm_cell(colB).lower()


def contiene_decimos(colB: str) -> bool:
    s = norm_cell(colB).lower()
    return ("décim" in s) or ("decim" in s)


def contiene_sueldo(colB: str) -> bool:
    return "sueldo" in norm_cell(colB).lower()


# ------------------ DB: recreación total ------------------
def recrear_db():
    # Borra DB anterior
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Nueva tabla con esquema correcto (cabeceras solicitadas)
    cur.execute("""
        CREATE TABLE pagos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            casa TEXT NOT NULL,
            propietario TEXT NOT NULL,
            monto_a_pagar REAL NOT NULL,
            fecha_pago TEXT NOT NULL,
            monto_pagado REAL NOT NULL,
            provision REAL NOT NULL,
            decimos REAL NOT NULL,
            sueldo REAL NOT NULL,
            saldo_pagar REAL NOT NULL
        )
    """)

    # Índice para consultas rápidas
    cur.execute("CREATE INDEX idx_pagos_casa_fecha ON pagos(casa, fecha_pago)")

    conn.commit()
    conn.close()


# ------------------ Importación ------------------
def importar_todos_los_meses():
    recrear_db()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO pagos
        (casa, propietario, monto_a_pagar, fecha_pago, monto_pagado, provision, decimos, sueldo, saldo_pagar)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    total_insertados = 0

    for archivo in ARCHIVOS:
        yyyymm = archivo_a_yyyymm(archivo)

        try:
            # Leemos como tabla cruda sin headers (para mapear por posición, como tu imagen)
            # A=0, B=1, C=2, D=3, E=4, ... L=11
            df = pd.read_csv(archivo, skiprows=2, encoding="latin1", header=None)

            i = 0
            insertados_arch = 0

            while i < len(df):
                colA = norm_cell(df.iloc[i, 0])   # columna A
                colB = norm_cell(df.iloc[i, 1])   # columna B (texto casa/linea)
                colC = df.iloc[i, 2]              # columna C (monto a pagar / provisión / etc)
                colD = df.iloc[i, 3]              # columna D (fecha)
                colE = df.iloc[i, 4]              # columna E (monto pagado)
                colL = df.iloc[i, 11] if df.shape[1] > 11 else 0  # columna L (saldo)

                # saltar hasta sección INGRESOS y filas válidas
                if colA == "INGRESOS" or not is_fila_casa_principal(colB):
                    i += 1
                    continue

                # ---- principal ----
                casa = get_casa(colB)
                propietario = colB
                monto_a_pagar = money_to_float(colC)
                fecha_pago = parse_fecha_excel(colD, yyyymm)
                monto_pagado = money_to_float(colE)
                saldo_pagar = money_to_float(colL)

                provision = 0.0
                decimos = 0.0
                sueldo = 0.0

                # ---- buscar las 3 filas siguientes del bloque ----
                j = i + 1
                while j < len(df):
                    b = norm_cell(df.iloc[j, 1])

                    # si empieza otra casa principal, se termina el bloque
                    if is_fila_casa_principal(b):
                        break

                    if contiene_provision(b):
                        provision = money_to_float(df.iloc[j, 2])  # columna C
                    elif contiene_decimos(b):
                        decimos = money_to_float(df.iloc[j, 2])    # columna C
                    elif contiene_sueldo(b):
                        sueldo = money_to_float(df.iloc[j, 2])     # columna C

                    j += 1

                # ---- insert ----
                cur.execute(
                    insert_sql,
                    (casa, propietario, monto_a_pagar, fecha_pago, monto_pagado, provision, decimos, sueldo, saldo_pagar)
                )
                insertados_arch += 1
                total_insertados += 1

                # avanzamos al siguiente bloque
                i = j

            conn.commit()
            print(f"✅ {archivo}: insertados={insertados_arch}")

        except FileNotFoundError:
            print(f"❌ No existe el archivo: {archivo}")
        except Exception as e:
            print(f"❌ {archivo}: error -> {e}")

    conn.close()
    print(f"\nRESUMEN: total_insertados={total_insertados}")


if __name__ == "__main__":
    importar_todos_los_meses()
