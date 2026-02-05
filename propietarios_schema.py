import sqlite3
from datetime import datetime

DB_PATH = "condominio.db"
CASAS = [f"C{i:02d}" for i in range(1, 11)]  # C01..C10


def recreate_propietarios_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. Borrar tabla si existe
    cur.execute("DROP TABLE IF EXISTS propietarios")

    # 2. Crear tabla propietarios
    cur.execute("""
        CREATE TABLE propietarios (
            casa TEXT PRIMARY KEY,                -- C01..C10

            foto_path TEXT,                       -- Ruta o URL de la foto
            nombre TEXT,
            cedula TEXT,
            telefono_fijo TEXT,
            celular TEXT,

            area REAL,                            -- Área en m2
            alicuota_pct REAL,                    -- % Alícuota
            email TEXT,

            tiene_arrendatario INTEGER DEFAULT 0, -- 0 = No, 1 = Sí
            no_autos INTEGER DEFAULT 0,           

            placa1 TEXT,
            placa2 TEXT,
            placa3 TEXT,
            placa4 TEXT,
            placa5 TEXT,
            placa6 TEXT,

            asistente_hogar INTEGER DEFAULT 0,    -- 0 = No, 1 = Sí
            asistente_nombre TEXT,

            actualizado_en TEXT                  -- Timestamp
        )
    """)

    # 3. Índices útiles
    cur.execute("CREATE INDEX idx_propietarios_nombre ON propietarios(nombre)")
    cur.execute("CREATE INDEX idx_propietarios_cedula ON propietarios(cedula)")
    cur.execute("CREATE INDEX idx_propietarios_email ON propietarios(email)")

    # 4. Precargar las 10 casas vacías
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for casa in CASAS:
        cur.execute("""
            INSERT INTO propietarios (casa, actualizado_en)
            VALUES (?, ?)
        """, (casa, now))

    conn.commit()
    conn.close()

    print("✅ Tabla 'propietarios' creada correctamente.")
    print("✅ Casas precargadas: C01 a C10.")


if __name__ == "__main__":
    recreate_propietarios_table()
