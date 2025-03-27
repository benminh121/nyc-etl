# 🚕 NYC_TAXI Data Pipeline 🚕
The project aims to demonstrate how to establish a system where you can extract, transform, and load data into a local data lake and query the data using a SQL engine.

## 🌟 System Architecture
<p align="center">
<img src="./img/diagram.svg" width=100% height=100%>

<p align="center">
    System Architecture
</p>

## 🚀 Getting Started
1. **Clone the repository**:
    ```bash
    git clone https://github.com/benminh121/nyc-etl
    ```

2. **Start all infrastructures**:
    ```bash
    make run_all
    ```
    This command will download the necessary Docker images, create containers, and start the services in detached mode.

3. **Setup environment**:
  - Installing UV
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
  - Installing Python
  ```bash
  uv python install 3.13
  ```
  - Sync the project's dependencies with the environment
  ```bash
  uv sync
  ```

4. **Access the Services**:
    - MinIO is accessible at `http://localhost:9001`.
    - Airflow is accessible at `http://localhost:8080`.
    - StarRocks is accessible at `http://localhost:9030`

5. **Download JAR files for Spark**:
- aws-java-sdk-bundle-1.12.262.jar
- hadoop-aws-3.3.4.jar
- iceberg-spark-runtime-3.5_2.12-1.8.1.jar
- url-connection-client-2.31.1.jar