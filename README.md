This project aims to find a solution to the problem of medical monitoring of cardiac electrical signals in intensive care units. The main problems encountered in previous projects were: 1- The length of treatment time
2- The high incidence of false alarms
3- The urgent need for significant resources and internet connectivity. This project is based on the Hofding tree and incremental learning. The Wilfred algorithm was used in incremental learning, drawing on several engineering concepts in the field of big data.

The CAP and ACID principles ensure a smooth project experience, preventing any issues or delays.

The setup is as follows: The Procedure broadcasts data to the cores. If a core is not functioning, broadcasting to it is stopped to protect patient data. Patients are then distributed to the remaining cores within the system. The Spark handles the formal processing.

Another Procedure and Spark were added as a test to adjust the number of active cores and visualize the results directly as an engineering diagram. However, some libraries need to be downloaded; the code will automatically download them within the command line.

Furthermore, the project requires an Apache Kafka server for high-quality operation, as it will not function without it.

Regards, Eng. Mohammed Rafiq Mohsen, Specialist in Healthcare Systems Development