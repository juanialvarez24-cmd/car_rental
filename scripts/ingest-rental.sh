#!/bin/bash
# Usage: chmod +x ingest-rental.sh && ./ingest-rental.sh
export HADOOP_HOME=/home/hadoop/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# ===============================================
# 1. VARIABLES DE CONFIGURACION
# ===============================================

FILES=(
        https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv,rental_data.csv
        https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv,geo_ref.csv
)

DESTINO_TEMPORAL="/tmp/ingest_rental"

DESTINO_HDFS="/home/hadoop/landing/rental"

# ===============================================
# 2. CONFIGURANDO ENTORNO
# ===============================================

echo "Limpiando y preparando directorio local: ${DESTINO_TEMPORAL}"
rm -rf "${DESTINO_TEMPORAL}"
mkdir -p "${DESTINO_TEMPORAL}"
echo "Directorio local listo."

echo "Limpiando directorio de destino en HDFS (Eliminando ${DESTINO_HDFS})..."
hdfs dfs -rm -r -skipTrash "${DESTINO_HDFS}"
echo "Verificando/Creando directorio de destino en HDFS: ${DESTINO_HDFS}"
hdfs dfs -mkdir -p "${DESTINO_HDFS}"
echo "Directorio HDFS (Landing Zone) listo."

# ===============================================
# 3. PROCESO DE INGESTA
# ===============================================

for ENTRY in "${FILES[@]}"; do
    FILE_URL=$(echo "${ENTRY}" | cut -d',' -f1)
    FILE_NAME=$(echo "${ENTRY}" | cut -d',' -f2)
    LOCAL_FILE="${DESTINO_TEMPORAL}/${FILE_NAME}"

    echo "--- Procesando archivo: ${FILE_NAME} ---"

        echo "1. Descargando desde: ${FILE_URL}..."
        wget --no-check-certificate -q --timeout=120 "${FILE_URL}" -O "${LOCAL_FILE}"

        if [ $? -ne 0 ]; then
                echo "ERROR: Fallo la descarga de ${FILE_NAME}. Saliendo."
                rm -f "${LOCAL_FILE}"
                exit 1
        fi
        echo "   Descarga completada en ${LOCAL_FILE}."

        echo "2. Ingestando ${FILE_NAME} a HDFS en ${DESTINO_HDFS}..."
        hdfs dfs -put -f "${LOCAL_FILE}" "${DESTINO_HDFS}"

        echo "   Ingesta completada."

        echo "3. Limpiando archivo local para liberar espacio..."
        rm "${LOCAL_FILE}"
        echo "   Limpieza local de ${FILE_NAME} terminada."
done

echo ""
echo "======================================================="
echo "EXITO: Ingesta de todos los archivos completada."
echo "Archivos disponibles en HDFS en: ${DESTINO_HDFS}"
echo "======================================================="

# ===============================================
# 4. VERIFICACION FINAL
# ===============================================

echo "---  Verificando archivos finales en HDFS ---"
hdfs dfs -ls "${DESTINO_HDFS}"
