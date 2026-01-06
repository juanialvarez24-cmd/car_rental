#!/bin/bash
# Usage: chmod +x ingest-rental.sh && ./ingest-rental.sh
export HADOOP_HOME=/home/hadoop/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# ===============================================
# 1. CONFIGURATION VARIABLES
# ===============================================

FILES=(
        https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv,rental_data.csv
        https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv,geo_ref.csv
)

TEMP_DESTINATION="/tmp/ingest_rental"

HDFS_DESTINATION="/home/hadoop/landing/rental"

# ===============================================
# 2. ENVIRONMENT SETUP
# ===============================================

echo "Cleaning and preparing local directory: ${TEMP_DESTINATION}"
rm -rf "${TEMP_DESTINATION}"
mkdir -p "${TEMP_DESTINATION}"
echo "Local directory ready."

echo "Cleaning destination directory in HDFS (Deleting ${HDFS_DESTINATION})..."
hdfs dfs -rm -r -skipTrash "${HDFS_DESTINATION}"
echo "Verifying/Creating destination directory in HDFS: ${HDFS_DESTINATION}"
hdfs dfs -mkdir -p "${HDFS_DESTINATION}"
echo "HDFS Landing Zone ready."

# ===============================================
# 3. INGESTION PROCESS
# ===============================================

for ENTRY in "${FILES[@]}"; do
    FILE_URL=$(echo "${ENTRY}" | cut -d',' -f1)
    FILE_NAME=$(echo "${ENTRY}" | cut -d',' -f2)
    LOCAL_FILE="${TEMP_DESTINATION}/${FILE_NAME}"

    echo "--- Processing file: ${FILE_NAME} ---"

        echo "1. Downloading from: ${FILE_URL}..."
        wget --no-check-certificate -q --timeout=120 "${FILE_URL}" -O "${LOCAL_FILE}"

        if [ $? -ne 0 ]; then
                echo "ERROR: Failed to download ${FILE_NAME}. Exiting."
                rm -f "${LOCAL_FILE}"
                exit 1
        fi
        echo "Download completed: ${LOCAL_FILE}."

        echo "2. Ingesting ${FILE_NAME} into HDFS at ${HDFS_DESTINATION}..."
        hdfs dfs -put -f "${LOCAL_FILE}" "${HDFS_DESTINATION}"

        echo "Ingestion completed."

        echo "3. Cleaning up local file to free space..."
        rm "${LOCAL_FILE}"
        echo "Local cleanup for ${FILE_NAME} finished"
done

echo ""
echo "======================================================="
echo "SUCCESS: Data ingestion for all files completed."
echo "Files available in HDFS at: ${HDFS_DESTINATION}"
echo "======================================================="

# ===============================================
# 4. FINAL VERIFICATION
# ===============================================

echo "--- Verifying final files in HDFS ---"
hdfs dfs -ls "${HDFS_DESTINATION}"
