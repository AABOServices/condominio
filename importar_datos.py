import pandas as pd
import sqlite3

def importar_historico():
    conn = sqlite3.connect('condominio.db')
    
    # Lista de archivos que subiste a GitHub
    archivos = ['012025.csv', '022025.csv', '032025.csv','042025.csv', '052025.csv', '062025.csv','072025.csv', '082025.csv', '092025.csv', '102025.csv', '112025.csv', '122025.csv',] # Agrega todos los nombres aquí
    
    for archivo in archivos:
        try:
            # Leemos el CSV (ajustamos según el formato de tus archivos)
            df = pd.read_csv(archivo, skiprows=2) # Saltamos encabezados vacíos
            
            for index, row in df.iterrows():
                casa = row['CASA'] # Nombre de la columna en tu archivo
                monto = float(row['VALOR']) 
                fecha = "2025-01-01" # Fecha estimada según el archivo
                
                # Calculamos la distribución automática
                prov = round(monto * 0.0338, 2)
                deci = round(monto * 0.045, 2)
                suel = round(monto * 0.20, 2)
                oper = round(monto - prov - deci - suel, 2)
                
                # Insertamos en la base de datos
                conn.execute('''INSERT INTO pagos (casa, fecha, monto, provision, decimos, sueldo, operativo, usuario) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                             (casa, fecha, monto, prov, deci, suel, oper, 'SISTEMA_CARGA'))
            
            print(f"✅ Archivo {archivo} cargado con éxito.")
        except Exception as e:
            print(f"❌ Error en {archivo}: {e}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    importar_historico()
