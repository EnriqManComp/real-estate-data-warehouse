TASKS:

- [x] Create virtual env
- [x] Add .gitignore exceptions 
- [x] Download Airflow docker-compose.yaml file.
- [x] Add pgAdmin to docker-compose.yaml file.
- [x] Run docker compose using ```docker compose up -d```
- [x] Add requirements file.
- [x] Add src folder to airflow volumen
- [x] Run DDL pipeline
- [x] Normalize field names.
- [x] One-time run dag > fetch_data_api on 2018/7/5.
- [x] Design daily run dag.
- [x] Request runtime execution of each task using flower api
- [x] Store time consumed in a db. 
- [x] Design the DE dashboard.
- [ ] Create the following test cases:
    - [ ] Check API get +1000 data.
    - [ ] Check one day insert.
    - [ ] Check different records insert.
    - [ ] Check duplicates records in the same date
    - [ ] Check updates in scd type 1, when same id has been inserted.
    - [ ] Check tracking in scd type 2, when a new update has been required.
    - [ ] Check expected length over different runs.
- [ ] Run for two year interval 2018-07-05 to 2018-08-05.
- [ ] Design the executive & analytic dashboards.
- [ ] Run for the remain time interval to current day.
- [ ] Check the final results on dashboards.
- [ ] Write the summary of the project: technologies used, achievement, use cases in real-world scenario, data modeling approach.
- [ ] Add to porfolio.


