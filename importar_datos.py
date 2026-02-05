import re
import pandas as pd
import sqlite3

DB_PATH = "condominio.db"
USUARIO_CARGA = "SISTEMA_CARGA"

ARCHIVOS = [
    "012025.csv", "022025.csv", "032025.csv", "042025.csv", "052025.csv", "062025.csv",
    "072025.csv", "082025.csv", "092025.csv", "102025.csv", "112025.csv", "122025.csv"
]


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
    except Exception:
        return 0.0


def parse_fecha(valor_fecha, archivo: str) -> str:
    # 1) fecha real
    if not pd.isna(valor_fecha):
        s = norm_cell(valor_fecha)
        if s and s != "0":
            dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
            if pd.notna(dt):
                return dt.strftime("%Y-%m-%d")

    # 2) fallback por nombre MMYYYY.csv
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

    # Excluir sublíneas
    if "provisi" in s or "décim" in s or "decim" in s or "sueldo" in s:
        return False

    return bool(re.match(r"^(c\d{2}|\d{2})\b", s))


def calcular_fondos(monto: float):
    prov = round(monto * 0.0338, 2)
    deci = round(monto * 0.045, 2)
    suel = round(monto * 0.20, 2)
    oper = round(monto - prov - deci - suel, 2)
    return prov, deci, suel, oper


def borrar_mes_casa(cur, casa: str, mes_yyyy_mm: str):
    """
    Overwrite: borra registros importados automáticamente (SISTEMA_CARGA)
    de esa casa y ese mes; luego se inserta el nuevo.
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

    total_archivos = 0
    total_reescritos = 0

    for archivo in ARCHIVOS:
        try:
            df = pd.read_csv(archivo, skiprows=2, encoding="latin1")
            cols = list(df.columns)

            col_tipo = cols[0]
            col_desc = cols[1]
            col_fecha = cols[3]
            col_entra = cols[4]

            # ubicar inicio bloque INGRESOS
            idx_ing = df.index[df[col_tipo].fillna("").astype(str).str.strip().eq("INGRESOS")]
            if len(idx_ing) == 0:
                print(f"⚠️ {archivo}: no se encontró sección INGRESOS")
                continue

            start = int(idx_ing[0])

            # ubicar fin bloque INGRESOS (evita 'nan' como texto)
            end = len(df)
            for i in range(start + 1, len(df)):
                v = norm_cell(df.loc[i, col_tipo])
                if v and v != "INGRESOS":
                    end = i
                    break

            bloque = df.iloc[start:end].copy()

            filas = []
            for _, row in bloque.iterrows():
                desc = row.get(col_desc, "")
                if not es_fila_pago_principal(desc):
                    continue

                casa = extraer_codigo_casa(desc)
                if not casa:
                    continue

                fecha = parse_fecha(row.get(col_fecha, None), archivo)
                mes = yyyymm(fecha)
                monto = money_to_float(row.get(col_entra, 0))  # puede ser 0.0

                filas.append((casa, fecha, mes, monto))

            if not filas:
                print(f"⚠️ {archivo}: no se detectaron casas para importar")
                continue

            # Overwrite por casa+mes
            reescritos_arch = 0
            for casa, fecha, mes, monto in filas:
                borrar_mes_casa(cur, casa, mes)
                prov, deci, suel, oper = calcular_fondos(monto)
                cur.execute(insert_sql, (casa, fecha, monto, prov, deci, suel, oper, USUARIO_CARGA))
                reescritos_arch += 1

            conn.commit()
            total_archivos += 1
            total_reescritos += reescritos_arch
            print(f"✅ {archivo}: reescritos={reescritos_arch}")

        except FileNotFoundError:
            print(f"❌ No existe el archivo: {archivo}")
        except Exception as e:
            print(f"❌ {archivo}: error -> {e}")

    conn.close()
    print(f"\nRESUMEN: archivos_procesados={total_archivos} | reescritos_total={total_reescritos}")


if __name__ == "__main__":
    importar_historico_overwrite()
