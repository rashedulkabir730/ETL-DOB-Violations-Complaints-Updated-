# ETL-DOB-Violations-Complaints-Updated-

<img width="813" alt="Screenshot 2024-10-08 at 6 00 32 PM" src="https://github.com/user-attachments/assets/54475e3b-1edd-4425-a2e4-b39d34de314b">


Project Overview:

I revamped a prior project to address performance issues in a Streamlit web app by integrating data engineering practices, specifically leveraging AWS services and Apache Airflow for orchestration.

Data Extraction: Using Python, I extracted datasets from two distinct APIs provided by NYC Open Data—one containing complaints data and the other containing violations data. Due to the large dataset size, I implemented chunking methods to handle the data efficiently and loaded the raw data into two separate S3 buckets for storage and future processing.

Data Transformation: Utilizing Pandas for data transformation, I performed a variety of operations, such as data type conversions, column clean-up (e.g., dropping unnecessary fields), column concatenation, and splitting. The transformed datasets were then uploaded to separate S3 buckets to streamline access for downstream tasks, including visualization.

Data Loading: Once transformation was complete, the processed data was loaded into new S3 buckets, ensuring a clean, structured dataset was available for further analysis and visualization.

Further Data Cleaning: I integrated an additional CSV dataset containing complaint descriptions, which required further transformation to align with the primary datasets. This step, along with additional column clean-ups, will be incorporated into future iterations of the DAG to fully automate the process within Apache Airflow.

Visualization: Using Streamlit, I developed a dynamic dashboard for visualizing complaints and violations over time, filtered by address. The application included bar charts and displayed relevant descriptions for both complaints and violations, providing users with a comprehensive view of the data.

Key tools and technologies used in this project include AWS S3, EC2, Apache Airflow, Python, Pandas, and Streamlit, focusing on the ETL pipeline, data transformation, and automated workflows. This project showcased my ability to implement scalable data engineering solutions for real-world datasets and optimize application performance through cloud-based infrastructure
