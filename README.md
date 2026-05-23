This project addresses the critical issue of false alarms and processing delays in intensive care cardiac monitoring. Previous systems suffered from:
1- High latency (slow processing time)
2- Elevated false alarm rates
3- Heavy reliance on resources and constant internet connectivity.

Our solution employs Hoeffding Tree theory and Incremental Learning (using Welford's algorithm) combined with Big Data engineering principles such as distributed stream processing and fault tolerance.

How it works:
The Producer broadcasts patient data (32 or 100 patients) to multiple processing cores via Apache Kafka. If a core fails, the system immediately stops sending data to it to preserve patient isolation and redistributes those patients to the remaining healthy cores. On the other side, Apache Spark Structured Streaming consumes the data and performs real-time arrhythmia detection with adaptive thresholds.

We also included a stress-testing module that allows adjusting the number of active cores (1, 2, 4, 8) and visualizes the performance metrics (throughput, latency, false alarm rate) as engineering charts. Required libraries are automatically downloaded when the script runs.

Important: The project requires a running Apache Kafka server; it will not operate without it.

Key Results:

False alarm rate: 26.2% (below the AAMI EC57 standard of 28%)

Accuracy: 96%

Latency: 225ms

Fault tolerance: 100% processing continuity during core failures.

Regards,
Eng. Mohammed Rafiq Mohsen
Healthcare Systems Development Specialist