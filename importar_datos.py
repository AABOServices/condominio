import re
import pandas as pd
import sqlite3
from datetime import datetime

DB_PATH = "condominio.db"

ARCHIVOS = [
    "012025.csv", "022025.csv", "032025.csv", "042025.csv", "052025.csv", "062025.csv",
    "072025.csv", "082025.csv", "092025.csv", "102025.csv", "112025.csv", "122025.csv"
]

# ---------- Helpers ----------
def money_to_float(x) -> float:
    """
    Convierte strings tipo: " $279.50 ", "$-   ", "$(224.65)", " $(1,883.57) " -> float
    """
    if pd.isna(x):
        return 0.0
    s = str(x).strip()

    # Normaliza paréntesis como negativo: (123.45) o $(123.45)
    neg = False
    if "(" in s and ")" in s:
        neg = True
        s = s.replace("(", "").replace(")", "")

    # Quita símbolos y espacios
    s = s.replace("$", "").replace(" ", "").replace("\u00a0", "")

    # Si es guion o vacío
    if s in ["-", "—", "", "0", "0.0"]:
        return 0.0

    # Quita separadores de miles (coma)
    s = s.replace(",", "")

    try:
        val = float(s)
        return -val if neg else val
    except:
        return 0.0


def parse_fecha(valor_fecha, archivo: str) -> str:
    """
    Toma FECHA del CSV; si viene vacía o 0, usa 1er día del mes inferido del nombre (MMYYYY.csv).
    """
    # 1) Si viene fecha válida tipo "2-Jul-2025"
    if not pd.isna(valor_fecha):
        s = str(valor_fecha).strip()
        if s and s != "0":
            try:
                dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
                if pd.notna(dt):
                    return dt.strftime("%Y-%m-%d")
            except:
                pass

    # 2) Default por nombre de archivo: "072025.csv" -> 2025-07-01
    m = re.search(r"(\d{2})(\d{4})", archivo)
    if m:
        mm = int(m.group(1))
        yyyy = int(m.group(2))
        return f"{yyyy:04d}-{mm:02d}-01"

    # 3) Fallback
    return "2025-01-01"


def extraer_codigo_casa(descripcion: str) -> str:
    """
    De "C01 Canelos - Pérez" -> "C01"
    De "03 Nieding - Toscano" -> "C03"
    """
    if descripcion is None:
        return ""

    s = str(descripcion).strip()

    # Caso "C01 ..."
    m = re.match(r"^(C\d{2})\b", s, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()

    # Caso "03 ..." (sin C)
    m = re.match(r"^(\d{2})\b", s)
    if m:
        return f"C{m.group(1)}"

    return ""


def es_fila_pago_principal(descripcion: str) -> bool:
    """
    True solo para filas que representan el pago principal del mes (no Provisión/Décimos/Sueldo).
    """
    if descripcion is None:
        return False
    s = str(descripcion).strip().lower()

    # Excluir sublíneas
    if "provisi" in s or "décim" in s or "decim" in s or "sueldo" in s:
        return False

    # Debe empezar por Cxx o xx
    return bool(re.match(r"^(c\d{2}|\d{2})\b", s))


# ---------- Importación ----------
def importar_historico():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO pagos (casa, fecha, monto, provision, decimos, sueldo, operativo, usuario)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    for archivo in ARCHIVOS:
        try:
            # Nota: encoding latin1 suele ir mejor con estos CSV exportados
            df = pd.read_csv(archivo, skiprows=2, encoding="latin1")

            # Detecta columnas reales
            cols = list(df.columns)

            # Normalmente quedan así (por tu archivo real):
            # [0]=Unnamed: 0  -> "INGRESOS" / vacío
            # [1]=DESCRIPCIÓN -> "C01 Canelos - Pérez"
            # [2]=G_OFICIAL   -> "$279.57"
            # [3]=FECHA       -> "2-Jul-2025"
            # [4]=ENTRA       -> "$279.50"
            col_tipo = cols[0]
            col_desc = cols[1]
            col_fecha = cols[3]
            col_entra = cols[4]

            # Filtramos filas de la sección INGRESOS:
            # En tu CSV, la primera fila de ingresos tiene col_tipo="INGRESOS"
            # y las siguientes suelen venir con col_tipo vacío/NaN pero siguen siendo ingresos.
            # Estrategia: ubicar el bloque desde "INGRESOS" hasta que cambie a otra sección (p.ej. GASTOS VARIABLES)
            idx_ing = df.index[df[col_tipo].astype(str).str.strip().eq("INGRESOS")]
            if len(idx_ing) == 0:
                print(f"⚠️ No se encontró sección INGRESOS en {archivo}")
                continue

            start = idx_ing[0]

            # Busca fin del bloque: primera fila después que tenga un texto fuerte en col_tipo distinto de "" y distinto de "INGRESOS"
            end = len(df)
            for i in range(start + 1, len(df)):
                v = str(df.loc[i, col_tipo]).strip()
                if v and v != "INGRESOS":
                    end = i
                    break

            bloque = df.iloc[start:end].copy()

            cargados = 0

            for _, row in bloque.iterrows():
                tipo = str(row.get(col_tipo, "")).strip()
                desc = row.get(col_desc, "")

                # Solo filas que son pagos principales (no "C01 Provisión", etc.)
                if not es_fila_pago_principal(desc):
                    continue

                casa = extraer_codigo_casa(desc)
                if not casa:
                    continue

                fecha = parse_fecha(row.get(col_fecha, None), archivo)
                monto = money_to_float(row.get(col_entra, 0))

                # Si no pagó ese mes (ENTRA = 0), no insertamos
                if monto <= 0:
                    continue

                # Distribución (misma que en tu app)
                prov = round(monto * 0.0338, 2)
                deci = round(monto * 0.045, 2)
                suel = round(monto * 0.20, 2)
                oper = round(monto - prov - deci - suel, 2)

                # (Opcional) Antiduplicados básico:
                # Evita insertar si ya existe mismo casa+fecha+monto+usuario
                existe = cur.execute(
                    "SELECT 1 FROM pagos WHERE casa=? AND fecha=? AND monto=? AND usuario=? LIMIT 1",
                    (casa, fecha, monto, "SISTEMA_CARGA")
                ).fetchone()

                if existe:
                    continue

                cur.execute(insert_sql, (casa, fecha, monto, prov, deci, suel, oper, "SISTEMA_CARGA"))
                cargados += 1

            conn.commit()
            print(f"✅ {archivo}: {cargados} pagos principales cargados.")

        except FileNotFoundError:
            print(f"❌ No existe el archivo: {archivo}")
        except Exception as e:
            print(f"❌ Error en {archivo}: {e}")

    conn.close()


if __name__ == "__main__":
    importar_historico()
