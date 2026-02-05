import pandas as pd
import sqlite3
import re

DB_PATH = "condominio.db"
USUARIO = "SISTEMA_CARGA"

ARCHIVOS = [
    "012025.csv","022025.csv","032025.csv","042025.csv","052025.csv","062025.csv",
    "072025.csv","082025.csv","092025.csv","102025.csv","112025.csv","122025.csv"
]

# ---------- DB ----------
def ensure_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pagos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            casa TEXT,
            propietario TEXT,
            monto_a_pagar REAL,
            fecha_pago TEXT,
            monto_pagado REAL,
            provision REAL,
            decimos REAL,
            sueldo REAL,
            saldo_pagar REAL,
            usuario TEXT
        )
    """)

    conn.commit()
    conn.close()


# ---------- Helpers ----------
def money(x):
    if pd.isna(x):
        return 0.0
    s = str(x).replace("$","").replace(",","").strip()
    if s in ["","-"]:
        return 0.0
    if "(" in s:
        return -float(s.replace("(","").replace(")",""))
    return float(s)

def is_casa_row(txt):
    return bool(re.match(r"^C\d{2}\b", txt))

def get_casa(txt):
    return txt.split()[0]

def get_fecha(x):
    try:
        return pd.to_datetime(x, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None


# ---------- Import ----------
def importar():
    ensure_table()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for archivo in ARCHIVOS:
        df = pd.read_csv(archivo, skiprows=2, encoding="latin1", header=None)

        # Limpiamos importación previa de ese mes
        cur.execute("DELETE FROM pagos WHERE usuario = ?", (USUARIO,))

        i = 0
        while i < len(df):
            colA = str(df.iloc[i,0]).strip()
            colB = str(df.iloc[i,1]).strip()

            if colA == "INGRESOS" or not is_casa_row(colB):
                i += 1
                continue

            # ---- Fila principal ----
            casa = get_casa(colB)
            propietario = colB
            monto_a_pagar = money(df.iloc[i,2])
            fecha_pago = get_fecha(df.iloc[i,3])
            monto_pagado = money(df.iloc[i,4])
            saldo_pagar = money(df.iloc[i,11])

            provision = decimos = sueldo = 0.0

            # ---- Filas dependientes ----
            j = i + 1
            while j < len(df):
                txt = str(df.iloc[j,1]).lower()
                if is_casa_row(txt):
                    break

                if "provisi" in txt:
                    provision = money(df.iloc[j,2])
                elif "décimo" in txt or "decimo" in txt:
                    decimos = money(df.iloc[j,2])
                elif "sueldo" in txt:
                    sueldo = money(df.iloc[j,2])

                j += 1

            # ---- Insert ----
            cur.execute("""
                INSERT INTO pagos
                (casa, propietario, monto_a_pagar, fecha_pago, monto_pagado,
                 provision, decimos, sueldo, saldo_pagar, usuario)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                casa, propietario, monto_a_pagar, fecha_pago, monto_pagado,
                provision, decimos, sueldo, saldo_pagar, USUARIO
            ))

            i = j

    conn.commit()
    conn.close()
    print("✅ Importación finalizada correctamente")


if __name__ == "__main__":
    importar()
