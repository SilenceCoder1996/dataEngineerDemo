# ETL Pipeline with Airflow, PostgreSQL, and MinIO

This demo demonstrates an ETL (Extract, Transform, Load) pipeline implemented using **Apache Airflow**, **PostgreSQL**, and **MinIO** (a local S3 simulator). It processes an IMDb movie dataset and uploads it to PostgreSQL, all while leveraging MinIO for cloud-like object storage.

## 🛠️ Technologies Used

- **Airflow**: Workflow automation and scheduling system to orchestrate the ETL pipeline.
- **PostgreSQL**: Relational database for storing processed data.
- **MinIO**: Object storage server, simulating AWS S3, for storing raw and processed files.
- **Docker**: Containerization to run all services locally with Docker Compose.
- **Python**: For ETL scripting and data processing.

## 📦 Project Structure

- `dags/`: Contains the Airflow DAG definition for the ETL process.
- `data/`: Directory where raw data (IMDB dataset) and output files are stored.
- `utils/`: Helper scripts for extracting, transforming, and loading data.
- `docker-compose.yml`: Configures the services (Airflow, PostgreSQL, MinIO) for running the pipeline.


+-------------+     +-------------+     +-------------+
|  Extract    | --> |  Transform  | --> |   Load      |
|  (CSV)      |     |  (Cleaning, |     |  (PostgreSQL|
|             |     |   Formatting|     |   or MinIO) |
+-------------+     +-------------+     +-------------+
