# AnomalyDetection

    For every event, the algorithm looks back by the defined window (2 hours) and calculates the percentage deviation from the average count for the window. 
    If the percentage deviation is greater than the specified threshold (75%), the data-point is considered an anomaly. If a given date has more 4 anomalous datapoints,
    the "anomaly_status" is set to "anomaly detected" or else is set to "no anomalies".



*Step Zero: Prerequisites*

This project assumes

    You have a sbt installed and running.

*Step One:*

    Run in project folder
`sbt run`

*Result:*
    
    A csv with random name will be created under 
`<project_root>/AnomalyOutput/`



