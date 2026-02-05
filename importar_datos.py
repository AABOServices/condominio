import sqlite3
import pandas as pd
import os

DB_PATH = "condominio.db"
CSV_PATH = "pagos_planos_2025.csv"


def recreate_pagos_table(conn: sqlite3.Connection):
    cur = conn.cursor()

    # Borra solo la tabla pagos (no toca usuarios)
    cur.execute("DROP TABLE IF EXISTS pagos")

    # Crea tabla con esquema final
    cur.execute("""
        CREATE TABLE pagos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            periodo TEXT,
            casa TEXT NOT NULL,
            propietario TEXT NOT NULL,
            monto_a_pagar REAL,
            fecha_pago TEXT,
            monto_pagado REAL,
            provision REAL,
            decimos REAL,
            sueldo REAL,
            saldo_pagar REAL
        )
    """)

    # Índices útiles
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pagos_casa_fecha ON pagos(casa, fecha_pago)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pagos_periodo ON pagos(periodo)")

    conn.commit()


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Verificación de columnas mínimas
    required = [
        "periodo", "casa", "propietario",
        "monto_a_pagar", "fecha_pago", "monto_pagado",
        "provision", "decimos", "sueldo", "saldo_pagar"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en el CSV: {missing}")

    # Normalización de tipos
    df["periodo"] = df["periodo"].astype(str).str.strip()
    df["casa"] = df["casa"].astype(str).str.strip().str.upper()
    df["propietario"] = df["propietario"].astype(str).str.strip()

    # Fecha a formato ISO (si viene vacía queda None)
    df["fecha_pago"] = pd.to_datetime(df["fecha_pago"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["fecha_pago"] = df["fecha_pago"].where(df["fecha_pago"].notna(), None)

    # Numéricos
    num_cols = ["monto_a_pagar", "monto_pagado", "provision", "decimos", "sueldo", "saldo_pagar"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).round(2)

    return df


def importar_desde_csv():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"No se encontró el archivo CSV: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, encoding="utf-8")
    df = normalize_df(df)

    conn = sqlite3.connect(DB_PATH)

    # Rehacer tabla pagos
    recreate_pagos_table(conn)

    # Insert masivo con to_sql (rápido y confiable)
    df.to_sql("pagos", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()

    print(f"✅ Importación OK: {len(df)} filas cargadas desde {CSV_PATH} a {DB_PATH}")


if __name__ == "__main__":
    importar_desde_csv()
