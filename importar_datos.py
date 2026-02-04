import re
import pandas as pd
import sqlite3

DB_PATH = "condominio.db"

ARCHIVOS = [
    "012025.csv", "022025.csv", "032025.csv", "042025.csv", "052025.csv", "062025.csv",
    "072025.csv", "082025.csv", "092025.csv", "102025.csv", "112025.csv", "122025.csv"
]

# ---------- Helpers ----------
def money_to_float(x) -> float:
    if pd.isna(x):
        return 0.0
    s = str(x).strip()

    neg = False
    if "(" in s and ")" in s:
        neg = True
        s = s.replace("(", "").replace(")", "")

    s = s.replace("$", "").replace(" ", "").replace("\u00a0", "")
    if s in ["-", "—", "", "0", "0.0"]:
        return 0.0

    s = s.replace(",", "")
    try:
        val = float(s)
        return -val if neg else val
    except:
        return 0.0


def parse_fecha(valor_fecha, archivo: str) -> str:
    """
    Devuelve fecha YYYY-MM-DD.
    Si la fila no trae fecha, toma YYYY-MM-01 según el nombre del archivo MMYYYY.csv
    """
    if not pd.isna(valor_fecha):
        s = str(valor_fecha).strip()
        if s and s != "0":
            dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
            if pd.notna(dt):
                return dt.strftime("%Y-%m-%d")

    m = re.search(r"(\d{2})(\d{4})", archivo)
    if m:
        mm = int(m.group(1))
        yyyy = int(m.group(2))
        return f"{yyyy:04d}-{mm:02d}-01"

    return "2025-01-01"


def yyyymm(fecha_yyyy_mm_dd: str) -> str:
    # "2025-07-02" -> "2025-07"
    return fecha_yyyy_mm_dd[:7]


def extraer_codigo_casa(descripcion: str) -> str:
    if descripcion is None:
        return ""
    s = str(descripcion).strip()

    m = re.match(r"^(C\d{2})\b", s, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()

    m = re.match(r"^(\d{2})\b", s)
    if m:
        return f"C{m.group(1)}"

    return ""


def es_fila_pago_principal(descripcion: str) -> bool:
    if descripcion is None:
        return False
    s = str(descripcion).strip().lower()

    # excluir sublíneas de desglose
    if "provisi" in s or "décim" in s or "decim" in s or "sueldo" in s:
        return False

    return bool(re.match(r"^(c\d{2}|\d{2})\b", s))


# ---------- Anti-duplicado por Casa+Mes ----------
def existe_pago_casa_mes(cur, casa: str, fecha: str) -> bool:
    """
    Si ya existe un pago en ese mes para esa casa, no insertamos.
    Comparo por strftime('%Y-%m', fecha) en SQLite.
    """
    mes = yyyymm(fecha)
    row = cur.execute(
        """
        SELECT 1
        FROM pagos
        WHERE casa = ?
          AND strftime('%Y-%m', fecha) = ?
        LIMIT 1
        """,
        (casa, mes)
    ).fetchone()
    return row is not None


def importar_historico():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO pagos (casa, fecha, monto, provision, decimos, sueldo, operativo, usuario)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    total_insertados = 0
    total_saltados = 0

    for archivo in ARCHIVOS:
        try:
            df = pd.read_csv(archivo, skiprows=2, encoding="latin1")
            cols = list(df.columns)

            # columnas según tu formato real
            col_tipo = cols[0]
            col_desc = cols[1]
            col_fecha = cols[3]
            col_entra = cols[4]

            # localizar bloque INGRESOS
            idx_ing = df.index[df[col_tipo].astype(str).str.strip().eq("INGRESOS")]
            if len(idx_ing) == 0:
                print(f"⚠️ No se encontró sección INGRESOS en {archivo}")
                continue

            start = idx_ing[0]
            end = len(df)
            for i in range(start + 1, len(df)):
                v = str(df.loc[i, col_tipo]).strip()
                if v and v != "INGRESOS":
                    end = i
                    break

            bloque = df.iloc[start:end].copy()

            insertados_arch = 0
            saltados_arch = 0

            for _, row in bloque.iterrows():
                desc = row.get(col_desc, "")

                if not es_fila_pago_principal(desc):
                    continue

                casa = extraer_codigo_casa(desc)
                if not casa:
                    continue

                fecha = parse_fecha(row.get(col_fecha, None), archivo)
                monto = money_to_float(row.get(col_entra, 0))

                if monto <= 0:
                    continue

                # Anti-duplicado por casa+mes
                if existe_pago_casa_mes(cur, casa, fecha):
                    saltados_arch += 1
                    continue

                prov = round(monto * 0.0338, 2)
                deci = round(monto * 0.045, 2)
                suel = round(monto * 0.20, 2)
                oper = round(monto - prov - deci - suel, 2)

                cur.execute(insert_sql, (casa, fecha, monto, prov, deci, suel, oper, "SISTEMA_CARGA"))
                insertados_arch += 1

            conn.commit()
            total_insertados += insertados_arch
            total_saltados += saltados_arch
            print(f"✅ {archivo}: insertados={insertados_arch} | saltados(duplicados casa+mes)={saltados_arch}")

        except FileNotFoundError:
            print(f"❌ No existe el archivo: {archivo}")
        except Exception as e:
            print(f"❌ Error en {archivo}: {e}")

    conn.close()
    print(f"\nRESUMEN: insertados={total_insertados} | saltados={total_saltados}")


if __name__ == "__main__":
    importar_historico()
