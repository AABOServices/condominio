import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = "condominio.db"

def ensure_table(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS propietarios (
            casa TEXT PRIMARY KEY,
            propietario TEXT NOT NULL,
            fecha_ultimo_registro TEXT,
            actualizado_en TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_propietarios_propietario ON propietarios(propietario)")
    conn.commit()

def rebuild_from_pagos(conn: sqlite3.Connection):
    """
    Regla:
    - Para cada casa, toma el propietario del registro más reciente (MAX(fecha_pago)).
    - Si fecha_pago es NULL, lo deja al final (no debería ocurrir si tu CSV está bien).
    """
    df = pd.read_sql_query("""
        SELECT casa, propietario, fecha_pago
        FROM pagos
        WHERE casa IS NOT NULL AND TRIM(casa) <> ''
    """, conn)

    if df.empty:
        print("⚠️ No hay datos en 'pagos'. No se puede construir propietarios.")
        return 0

    df["fecha_pago"] = pd.to_datetime(df["fecha_pago"], errors="coerce")
    df["casa"] = df["casa"].astype(str).str.strip().str.upper()
    df["propietario"] = df["propietario"].astype(str).str.strip()

    # Tomar el último registro por casa
    df = df.sort_values(["casa", "fecha_pago"], ascending=[True, True])
    idx = df.groupby("casa")["fecha_pago"].idxmax()
    df_last = df.loc[idx, ["casa", "propietario", "fecha_pago"]].copy()

    df_last["fecha_ultimo_registro"] = df_last["fecha_pago"].dt.strftime("%Y-%m-%d")
    df_last.drop(columns=["fecha_pago"], inplace=True)

    # Reemplazar tabla (maestro) completamente
    cur = conn.cursor()
    cur.execute("DELETE FROM propietarios")

    actualizado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.executemany("""
        INSERT INTO propietarios (casa, propietario, fecha_ultimo_registro, actualizado_en)
        VALUES (?, ?, ?, ?)
    """, [
        (r["casa"], r["propietario"], r["fecha_ultimo_registro"], actualizado_en)
        for _, r in df_last.iterrows()
    ])

    conn.commit()
    print(f"✅ Maestro propietarios actualizado: {len(df_last)} casas")
    return len(df_last)

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)
    rebuild_from_pagos(conn)
    conn.close()

if __name__ == "__main__":
    main()
