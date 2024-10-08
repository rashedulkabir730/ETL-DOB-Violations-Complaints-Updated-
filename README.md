# ETL-DOB-Violations-Complaints-Updated-

<img width="813" alt="Screenshot 2024-10-08 at 6 00 32 PM" src="https://github.com/user-attachments/assets/54475e3b-1edd-4425-a2e4-b39d34de314b">

Project Overview:

I enhanced a previous project by addressing performance bottlenecks in a Streamlit web app through the integration of data engineering best practices, leveraging AWS services, Apache Airflow for orchestration, and caching mechanisms to optimize application performance.

Data Extraction: Using Python, I extracted datasets from two NYC Open Data APIs—one for complaints data and the other for violations data. To efficiently handle the large volume of data, I utilized chunking techniques and stored the raw data in separate AWS S3 buckets.

Data Transformation: I used Pandas for data transformation, which involved data type conversions, dropping unnecessary columns, concatenating and splitting columns. The transformed data was loaded into new S3 buckets, ensuring structured and clean datasets were ready for subsequent processing and analysis.

Data Loading: The processed and cleaned datasets were uploaded to additional S3 buckets to support downstream tasks, including data visualization and analysis.
Further Data Cleaning: I incorporated a separate CSV file containing complaint descriptions into the existing datasets, cleaning and transforming the data to align with the primary datasets. In future iterations, this additional transformation step will be automated within the DAG managed by Apache Airflow.

Visualization & Performance Optimization: I developed an interactive dashboard using Streamlit to visualize complaints and violations over time, filtered by address. The visualizations included bar charts and descriptions of complaints and violations. To improve the performance of the app, I implemented caching mechanisms, reducing load times significantly by storing results of repeated computations and database queries. This optimization enabled faster rendering of visualizations when users interacted with the app.

Technologies used include AWS S3, EC2, Apache Airflow, Python, Pandas, Streamlit, and caching techniques. This project demonstrates my ability to build and optimize a scalable ETL pipeline, automate workflows using Airflow, and improve application performance through caching and cloud infrastructure.

