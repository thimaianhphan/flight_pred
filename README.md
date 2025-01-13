# Flight Delay Prediction Using Machine Learning on Azure Databricks

This project predicts flight arrival delays using a large-scale dataset scraped from the USA Bureau of Transportation Statistics. The dataset spans **600k rows × 12 months × 8 years** and is processed using **Dask** and **multithreading** for efficiency. The cleaned data is stored in **Parquet format** on **Azure Blob Storage** and used to train machine learning models (GBT) on **Azure Databricks**.

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Dataset](#dataset)
3. [Technical Stack](#technical-stack)
4. [Project Steps](#project-steps)
5. [Results](#results)
6. [How to Run the Project](#how-to-run-the-project)

---

## Project Overview
The goal of this project is to predict flight arrival delays using historical flight data. The project involves:
- **Scraping** flight data from the USA Bureau of Transportation Statistics using **BeautifulSoup**.
- **Preprocessing** and cleaning the data using **Dask** and **multithreading**.
- Storing the cleaned data in **Parquet format** on **Azure Blob Storage**.
- Building and training machine learning models (GBT and XGBoost) on **Azure Databricks**.
- Evaluating model performance using metrics like **RMSE**.

---

## Dataset
The dataset contains flight data from the USA Bureau of Transportation Statistics, spanning **600k rows × 12 months × 8 years**. Key features include:
- **Flight details**: Carrier code, flight number, origin, destination, etc.
- **Time-based features**: Departure time, arrival time, day of week, month, etc.
- **Delay information**: Departure delay, arrival delay, weather delay, etc.

The raw data is scraped, cleaned, and stored in **Parquet format** for efficient querying and processing.

---

## Technical Stack
- **Programming Languages**: Python, SQL
- **Libraries/Frameworks**: PySpark, Spark MLlib, XGBoost, Dask, MLflow, BeautifulSoup
- **Cloud Platforms**: Azure Databricks, Azure Blob Storage
- **Data Formats**: Parquet, ASC 
- **Performance Optimization**: Multithreading, Dask
- **Version Control**: Git

---

## Project Steps
1. **Data Collection**:
   - Scraped flight data using **BeautifulSoup**.
   - Combined data from multiple years and months into a single dataset.

2. **Data Preprocessing**:
   - Cleaned the dataset by handling missing values, outliers, and inconsistencies.
   - Used **Dask** and **multithreading** to optimize preprocessing performance.
   - Converted the cleaned data into **Parquet format** and uploaded it to **Azure Blob Storage**.

3. **Feature Engineering**:
   - Created new features such as:
     - Time-based features (e.g., hour of day, day of week).
     - Interaction features (e.g., total delay, delay ratios).

4. **Model Training**:
   - Built a machine learning pipeline using **PySpark** and **GBT**.
   - Trained and evaluated models using metrics like **RMSE**.

5. **Deployment**:
   - Deployed the pipeline on **Azure Databricks** for scalable model training.
   - Tracked experiments using **MLflow** for reproducibility.

---

## Results
- Achieved an **RMSE of 7.52** for flight delay prediction.
- Reduced preprocessing time by **62%** using **Dask** and **multithreading**.
- Successfully deployed the pipeline on **Azure Databricks** for scalable processing.

---

## How to Run the Project
1. **Set Up Environment**:
   - Install required libraries:
     ```bash
     pip install pyspark xgboost dask mlflow beautifulsoup4 azure-storage-blob
     ```
   - Set up an **Azure Databricks** cluster and connect it to **Azure Blob Storage**.

2. **Run the Preprocessing Scripts**:
   - Navigate to the `preprocess` folder and run the following script to scrape the data and push it to Azure Blob Storage:
        ```bash
        python preprocess/main.py
        ```

3. **Run the Notebooks**:
   - Open the notebooks in **Azure Databricks** or **Jupyter**.
   - Execute the cells in the following order:
     1. **Data Preprocessing Notebook**: Cleans and preprocesses the data.
     2. **Model Training Notebook**: Trains the machine learning models.
     3. **Evaluation Notebook**: Evaluates model performance.

4. **Access Data**:
   - Ensure the dataset is accessible from **Azure Blob Storage** or the local file system.
