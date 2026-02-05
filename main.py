import os
import re
import sqlite3
import hashlib
import subprocess
import sys
from datetime import datetime

import pandas as pd
import streamlit as st
import altair as alt

DB_PATH = "condominio.db"
UPLOAD_DIR = "uploads"
CASAS = [f"C{i:02d}" for i in range(1, 11)]
PLATE_RE = re.compile(r"^[A-Z]{3}\d{4}$")


# ------------------ DB: usuarios + propietarios ------------------
def ensure_users():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            user TEXT PRIMARY KEY,
            pw TEXT,
            rol TEXT
        )
    """)
    pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
    cur.execute("INSERT OR IGNORE INTO usuarios VALUES ('admin', ?, 'ADMINISTRADOR')", (pw_hash,))
    conn.commit()
    conn.close()


def ensure_propietarios_table():
    """
    Crea tabla propietarios (si no existe) y precarga C01..C10 (si no existen).
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS propietarios (
            casa TEXT PRIMARY KEY,

            foto_path TEXT,
            nombre TEXT,
            cedula TEXT,
            telefono_fijo TEXT,
            celular TEXT,

            area REAL,
            alicuota_pct REAL,
            email TEXT,

            tiene_arrendatario INTEGER DEFAULT 0,
            no_autos INTEGER DEFAULT 0,

            placa1 TEXT,
            placa2 TEXT,
            placa3 TEXT,
            placa4 TEXT,
            placa5 TEXT,
            placa6 TEXT,

            asistente_hogar INTEGER DEFAULT 0,
            asistente_nombre TEXT,

            actualizado_en TEXT
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_prop_nombre ON propietarios(nombre)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prop_cedula ON propietarios(cedula)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prop_email ON propietarios(email)")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for casa in CASAS:
        cur.execute("INSERT OR IGNORE INTO propietarios (casa, actualizado_en) VALUES (?, ?)", (casa, now))

    conn.commit()
    conn.close()


def validar_login(user: str, pw: str) -> bool:
    pw_hash = hashlib.sha256(pw.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT rol FROM usuarios WHERE user=? AND pw=?", (user, pw_hash)).fetchone()
    conn.close()
    if row:
        st.session_state.rol = row[0]
        return True
    return False


def run_script(script_name: str):
    result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


# ------------------ Datos: pagos ------------------
def cargar_df_pagos():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM pagos", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()

    if df.empty:
        return df

    needed = ["casa", "propietario", "fecha_pago", "monto_pagado", "saldo_pagar"]
    for c in needed:
        if c not in df.columns:
            df[c] = None

    df["casa"] = df["casa"].fillna("").astype(str).str.strip().str.upper()
    df["propietario"] = df["propietario"].fillna("").astype(str).str.strip()
    df["fecha_pago"] = pd.to_datetime(df["fecha_pago"], errors="coerce")

    df["monto_pagado"] = pd.to_numeric(df["monto_pagado"], errors="coerce").fillna(0.0)
    df["saldo_pagar"] = pd.to_numeric(df["saldo_pagar"], errors="coerce").fillna(0.0)
    df["periodo_mes"] = df["fecha_pago"].dt.to_period("M").astype(str)

    return df


# ------------------ Datos: propietarios CRUD ------------------
def get_propietario(casa: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM propietarios WHERE casa=?", (casa,)).fetchone()
    cols = [d[0] for d in cur.execute("PRAGMA table_info(propietarios)").fetchall()]  # (cid, name, type,...)
    conn.close()

    if not row:
        return {"casa": casa}

    # PRAGMA table_info devuelve tuples (cid,name,...) -> queremos name:
    conn = sqlite3.connect(DB_PATH)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(propietarios)").fetchall()]
    conn.close()

    return dict(zip(cols, row))


def upsert_propietario(data: dict):
    """
    Inserta/actualiza por casa.
    """
    data = dict(data)
    data["actualizado_en"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cols = [
        "casa", "foto_path", "nombre", "cedula", "telefono_fijo", "celular",
        "area", "alicuota_pct", "email",
        "tiene_arrendatario", "no_autos",
        "placa1", "placa2", "placa3", "placa4", "placa5", "placa6",
        "asistente_hogar", "asistente_nombre",
        "actualizado_en"
    ]

    # Normaliza ausentes
    for c in cols:
        if c not in data:
            data[c] = None

    placeholders = ",".join(["?"] * len(cols))
    update_set = ",".join([f"{c}=excluded.{c}" for c in cols if c != "casa"])

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO propietarios ({",".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(casa) DO UPDATE SET
        {update_set}
    """, [data[c] for c in cols])
    conn.commit()
    conn.close()


def save_uploaded_photo(casa: str, uploaded_file) -> str:
    """
    Guarda foto en uploads/ y devuelve path relativo.
    """
    if uploaded_file is None:
        return ""

    os.makedirs(UPLOAD_DIR, exist_ok=True)

    # extensión
    filename = uploaded_file.name
    ext = os.path.splitext(filename)[1].lower() or ".jpg"
    safe_name = f"{casa}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
    path = os.path.join(UPLOAD_DIR, safe_name)

    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    return path


def parse_placas(text: str):
    """
    Acepta placas separadas por coma, espacio o salto de línea.
    Devuelve lista única, en mayúsculas, máximo 6.
    """
    if not text:
        return []
    raw = re.split(r"[,\s]+", text.strip().upper())
    raw = [x for x in raw if x]
    # únicas preservando orden
    seen = set()
    out = []
    for p in raw:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out[:6]


def validate_placas(placas):
    bad = [p for p in placas if not PLATE_RE.match(p)]
    return bad


# ------------------ UI ------------------
st.set_page_config(page_title="Condominio 2025", layout="wide")
ensure_users()
ensure_propietarios_table()

st.title("🏢 CONDOMINIOS NANTU")

if "conectado" not in st.session_state:
    st.session_state.conectado = False
if "user" not in st.session_state:
    st.session_state.user = ""
if "rol" not in st.session_state:
    st.session_state.rol = ""

# ---- Login ----
if not st.session_state.conectado:
    st.sidebar.subheader("🔐 Acceso")
    user = st.sidebar.text_input("Usuario")
    pw = st.sidebar.text_input("Contraseña", type="password")
    if st.sidebar.button("Entrar"):
        if validar_login(user, pw):
            st.session_state.conectado = True
            st.session_state.user = user
            st.rerun()
        else:
            st.sidebar.error("Error de acceso (usuario/clave incorrectos)")
    st.info("Ingresa con tus credenciales para visualizar dashboard, históricos y administración.")
    st.stop()

st.sidebar.success(f"Conectado como: {st.session_state.user}")
st.sidebar.write(f"Rol: **{st.session_state.rol}**")

# Menú (Propietarios disponible para todos, pero edición solo Admin)
menu = st.sidebar.radio("Menú", ["Dashboard", "Histórico", "Propietarios", "Administrador"])

# ------------------ Data pagos + filtros pagos ------------------
df = cargar_df_pagos()

st.sidebar.divider()
st.sidebar.subheader("🎛️ Filtros (Pagos)")

if df.empty:
    st.sidebar.info("No hay datos. Ejecuta la carga desde 'Administrador'.")
    df_f = df
    casa_sel = "Todas"
    prop_filter = ""
    f_ini = None
    f_fin = None
else:
    casas = ["Todas"] + sorted([c for c in df["casa"].unique().tolist() if c])
    casa_sel = st.sidebar.selectbox("Casa", casas)
    prop_filter = st.sidebar.text_input("Propietario (contiene)", value="")

    min_fecha = df["fecha_pago"].min()
    max_fecha = df["fecha_pago"].max()
    col1, col2 = st.sidebar.columns(2)
    with col1:
        f_ini = st.date_input("Desde", value=min_fecha.date() if pd.notna(min_fecha) else None)
    with col2:
        f_fin = st.date_input("Hasta", value=max_fecha.date() if pd.notna(max_fecha) else None)

    df_f = df.copy()
    if casa_sel != "Todas":
        df_f = df_f[df_f["casa"] == casa_sel]
    if prop_filter.strip():
        df_f = df_f[df_f["propietario"].str.contains(prop_filter.strip(), case=False, na=False)]
    if f_ini and f_fin:
        df_f = df_f[(df_f["fecha_pago"].dt.date >= f_ini) & (df_f["fecha_pago"].dt.date <= f_fin)]


# ------------------ DASHBOARD ------------------
if menu == "Dashboard":
    st.subheader("📊 Dashboard por Casa (Pagado Σ vs Saldo último período)")

    if df_f.empty:
        st.info("No hay datos para mostrar con los filtros seleccionados.")
    else:
        total_pagado = df_f["monto_pagado"].sum()

        # Pagado Σ por casa
        pagado_by_casa = df_f.groupby("casa", as_index=False)["monto_pagado"].sum()
        pagado_by_casa.rename(columns={"monto_pagado": "pagado_sum"}, inplace=True)

        # Último saldo por casa según fecha máxima en el filtro
        df_last = df_f.dropna(subset=["fecha_pago"]).sort_values(["casa", "fecha_pago"])
        idx = df_last.groupby("casa")["fecha_pago"].idxmax()
        last_rows = df_last.loc[idx, ["casa", "fecha_pago", "saldo_pagar"]].copy()

        last_rows["saldo_ultimo_neg"] = last_rows["saldo_pagar"].apply(lambda x: x if x < 0 else 0.0)
        last_rows["saldo_ultimo_pos"] = last_rows["saldo_pagar"].apply(lambda x: x if x > 0 else 0.0)

        agg = pd.merge(
            pagado_by_casa,
            last_rows[["casa", "fecha_pago", "saldo_ultimo_neg", "saldo_ultimo_pos"]],
            on="casa",
            how="left"
        )
        agg["saldo_ultimo_neg"] = agg["saldo_ultimo_neg"].fillna(0.0)
        agg["saldo_ultimo_pos"] = agg["saldo_ultimo_pos"].fillna(0.0)

        total_saldo_neg = agg["saldo_ultimo_neg"].sum()
        total_saldo_pos = agg["saldo_ultimo_pos"].sum()

        last_date = df_f["fecha_pago"].max()
        last_period = last_date.to_period("M").strftime("%Y-%m") if pd.notna(last_date) else None
        st.caption(f"Último período detectado (según filtros): **{last_period if last_period else 'N/D'}**")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Monto Pagado (Σ)", f"${total_pagado:,.2f}")
        c2.metric("Saldo último período (Σ negativos)", f"${total_saldo_neg:,.2f}")
        c3.metric("Saldo último período (Σ positivos)", f"${total_saldo_pos:,.2f}")
        c4.metric("Registros (filtrados)", f"{len(df_f)}")

        st.divider()

        rows = []
        for _, r in agg.iterrows():
            rows.append({"casa": r["casa"], "categoria": "Pagado (Σ)", "valor": float(r["pagado_sum"])})
            rows.append({"casa": r["casa"], "categoria": "Saldo negativo (último, <0)", "valor": float(r["saldo_ultimo_neg"])})
            rows.append({"casa": r["casa"], "categoria": "Saldo positivo (último, >0)", "valor": float(r["saldo_ultimo_pos"])})

        chart_df = pd.DataFrame(rows)

        color_scale = alt.Scale(
            domain=["Pagado (Σ)", "Saldo negativo (último, <0)", "Saldo positivo (último, >0)"],
            range=["#2e7d32", "#ef6c00", "#6a1b9a"]  # verde / anaranjado / morado
        )

        bars = (
            alt.Chart(chart_df)
            .mark_bar()
            .encode(
                x=alt.X("casa:N", title="Casa", sort="ascending"),
                y=alt.Y("valor:Q", title="Monto (Σ) | Negativos hacia abajo", axis=alt.Axis(format=",.2f")),
                color=alt.Color("categoria:N", scale=color_scale, legend=alt.Legend(title="Serie")),
                tooltip=[
                    alt.Tooltip("casa:N", title="Casa"),
                    alt.Tooltip("categoria:N", title="Serie"),
                    alt.Tooltip("valor:Q", title="Valor", format=",.2f"),
                ],
            )
            .properties(height=420)
        )

        zero_line = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule().encode(y="y:Q")
        st.altair_chart(bars + zero_line, use_container_width=True)

        st.divider()
        st.subheader("📄 Detalle (tabla filtrada)")
        st.dataframe(df_f.sort_values(["casa", "fecha_pago"], ascending=[True, False]), use_container_width=True)


# ------------------ HISTÓRICO ------------------
elif menu == "Histórico":
    st.subheader("📚 Histórico (filtrado)")

    if df_f.empty:
        st.info("No hay datos para los filtros seleccionados.")
    else:
        st.caption(f"Registros filtrados: {len(df_f)}")
        st.dataframe(df_f.sort_values(["casa", "fecha_pago"], ascending=[True, False]), use_container_width=True)

        csv_bytes = df_f.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Descargar CSV (filtrado)",
            data=csv_bytes,
            file_name="historico_filtrado.csv",
            mime="text/csv"
        )


# ------------------ PROPIETARIOS: ver + (admin) editar ------------------
elif menu == "Propietarios":
    st.subheader("🏠 Propietarios por Casa")

    casa = st.selectbox("Casa", CASAS)

    # carga actual
    data = get_propietario(casa)

    # Mostrar foto actual si existe
    foto_path = data.get("foto_path") or ""
    if foto_path and os.path.exists(foto_path):
        st.image(foto_path, caption=f"Foto actual {casa}", width=220)
    elif foto_path:
        st.info(f"Foto registrada pero no encontrada en disco: {foto_path}")

    st.divider()

    # Solo admin puede editar
    is_admin = (st.session_state.rol == "ADMINISTRADOR")

    if not is_admin:
        st.info("Solo un ADMINISTRADOR puede editar. Aquí puedes visualizar la información.")
        st.json({k: v for k, v in data.items() if k not in ["id"]})
        st.stop()

    st.subheader("📝 Formulario de registro / edición")

    with st.form(f"form_prop_{casa}", clear_on_submit=False):
        colA, colB = st.columns(2)

        with colA:
            nombre = st.text_input("Nombre", value=data.get("nombre") or "")
            cedula = st.text_input("Cédula", value=data.get("cedula") or "")
            telefono_fijo = st.text_input("Teléfono fijo", value=data.get("telefono_fijo") or "")
            celular = st.text_input("Celular", value=data.get("celular") or "")
            email = st.text_input("Mail", value=data.get("email") or "")

        with colB:
            area = st.number_input("Área", min_value=0.0, value=float(data.get("area") or 0.0))
            alicuota = st.number_input("% Alícuota", min_value=0.0, value=float(data.get("alicuota_pct") or 0.0))
            no_autos = st.number_input("No. Autos", min_value=0, value=int(data.get("no_autos") or 0), step=1)

            tiene_arrendatario = st.radio(
                "¿Tiene arrendatario?",
                options=["No", "Sí"],
                index=1 if int(data.get("tiene_arrendatario") or 0) == 1 else 0,
                horizontal=True
            )

            asistente_hogar = st.radio(
                "¿Asistente de hogar?",
                options=["No", "Sí"],
                index=1 if int(data.get("asistente_hogar") or 0) == 1 else 0,
                horizontal=True
            )

            asistente_nombre = ""
            if asistente_hogar == "Sí":
                asistente_nombre = st.text_input("Nombre asistente", value=data.get("asistente_nombre") or "")

        st.divider()

        uploaded = st.file_uploader("Foto (opcional)", type=["png", "jpg", "jpeg", "webp"])

        # placas: mostramos las existentes como texto plano editable
        placas_exist = [
            data.get("placa1") or "", data.get("placa2") or "", data.get("placa3") or "",
            data.get("placa4") or "", data.get("placa5") or "", data.get("placa6") or ""
        ]
        placas_exist = [p for p in placas_exist if p]
        placas_text = st.text_area(
            "Placas (hasta 6). Formato AAA1234. Sepáralas por coma, espacio o línea.",
            value=" ".join(placas_exist),
            height=90
        )

        guardar = st.form_submit_button("💾 Guardar cambios")

    if guardar:
        placas = parse_placas(placas_text)
        bad = validate_placas(placas)

        if bad:
            st.error(f"Placas inválidas (deben ser AAA1234): {', '.join(bad)}")
            st.stop()

        # Foto: si subieron una nueva, la guardamos
        new_foto_path = foto_path
        if uploaded is not None:
            new_foto_path = save_uploaded_photo(casa, uploaded)

        # Asignar placas a columnas
        placas_cols = {f"placa{i+1}": (placas[i] if i < len(placas) else None) for i in range(6)}

        payload = {
            "casa": casa,
            "foto_path": new_foto_path,
            "nombre": nombre.strip() or None,
            "cedula": cedula.strip() or None,
            "telefono_fijo": telefono_fijo.strip() or None,
            "celular": celular.strip() or None,
            "area": float(area),
            "alicuota_pct": float(alicuota),
            "email": email.strip() or None,
            "tiene_arrendatario": 1 if tiene_arrendatario == "Sí" else 0,
            "no_autos": int(no_autos),
            "asistente_hogar": 1 if asistente_hogar == "Sí" else 0,
            "asistente_nombre": (asistente_nombre.strip() or None) if asistente_hogar == "Sí" else None,
            **placas_cols,
        }

        upsert_propietario(payload)
        st.success(f"✅ Propietario de {casa} guardado correctamente.")
        st.rerun()


# ------------------ ADMINISTRADOR ------------------
elif menu == "Administrador":
    st.subheader("🛠 Administrador")

    st.warning("Acciones de carga/actualización de base de datos.")

    colA, colB = st.columns(2)
    with colA:
        if st.button("🚀 Ejecutar Carga Pagos (importar_datos.py)"):
            rc, out, err = run_script("importar_datos.py")
            st.write("**Return code:**", rc)
            st.code(out if out else "(stdout vacío)", language="text")
            st.code(err if err else "(stderr vacío)", language="text")
            if rc == 0:
                st.success("✅ Pagos cargados correctamente.")
            else:
                st.error("❌ Error en la carga de pagos. Revisa stderr.")

    with colB:
        if st.button("✅ Inicializar/Verificar tabla propietarios"):
            ensure_propietarios_table()
            st.success("Tabla propietarios verificada y casas precargadas (C01..C10).")

    st.divider()
    st.subheader("Vista rápida")

    df2 = cargar_df_pagos()
    st.write(f"Registros en pagos: **{len(df2)}**")
    if not df2.empty:
        st.dataframe(df2.sort_values("fecha_pago", ascending=False).head(20), use_container_width=True)

    # Vista rápida propietarios
    conn = sqlite3.connect(DB_PATH)
    try:
        dfp = pd.read_sql_query("SELECT casa, nombre, cedula, celular, email, actualizado_en FROM propietarios", conn)
    except Exception:
        dfp = pd.DataFrame()
    conn.close()

    st.write(f"Registros en propietarios: **{len(dfp)}**")
    if not dfp.empty:
        st.dataframe(dfp.sort_values("casa"), use_container_width=True)


# ------------------ LOGOUT ------------------
st.sidebar.divider()
if st.sidebar.button("Cerrar sesión"):
    st.session_state.conectado = False
    st.session_state.user = ""
    st.session_state.rol = ""
    st.rerun()
