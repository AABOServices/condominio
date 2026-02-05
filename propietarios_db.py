import sqlite3
from datetime import datetime

DB_PATH = "condominio.db"

CASAS = [f"C{i:02d}" for i in range(1, 11)]  # C01..C10


def create_propietarios_table(conn: sqlite3.Connection):
    cur = conn.cursor()

    # Tabla principal de propietarios (1 fila por casa)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS propietarios (
            casa TEXT PRIMARY KEY,                 -- C01..C10

            foto_path TEXT,                        -- ruta local o URL si luego manejas uploads
            nombre TEXT,
            cedula TEXT,
            telefono_fijo TEXT,
            celular TEXT,
            area REAL,                             -- m2 o unidad que manejen
            alicuota_pct REAL,                     -- % alícuota
            email TEXT,

            tiene_arrendatario INTEGER DEFAULT 0,  -- 0/1 (radio button en UI)
            no_autos INTEGER DEFAULT 0,            -- número de autos

            placa1 TEXT,
            placa2 TEXT,
            placa3 TEXT,
            placa4 TEXT,
            placa5 TEXT,
            placa6 TEXT,

            asistente_hogar INTEGER DEFAULT 0,     -- 0/1
            asistente_nombre TEXT,                 -- solo si asistente_hogar=1

            actualizado_en TEXT
        )
    """)

    # Índices útiles
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prop_nombre ON propietarios(nombre)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prop_cedula ON propietarios(cedula)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prop_email ON propietarios(email)")

    conn.commit()


def seed_casas(conn: sqlite3.Connection):
    """
    Crea las 10 casas como registros base (vacíos), sin sobrescribir
    si ya existen.
    """
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for casa in CASAS:
        cur.execute("""
            INSERT OR IGNORE INTO propietarios (casa, actualizado_en)
            VALUES (?, ?)
        """, (casa, now))

    conn.commit()


def main(recreate: bool = False):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if recreate:
        cur.execute("DROP TABLE IF EXISTS propietarios")
        conn.commit()

    create_propietarios_table(conn)
    seed_casas(conn)

    conn.close()
    print("✅ Tabla propietarios lista y 10 casas precargadas (C01..C10).")


if __name__ == "__main__":
    # Cambia a True si quieres borrar y recrear la tabla desde cero
    main(recreate=False)
