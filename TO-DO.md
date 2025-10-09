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
- [x] Create the following unit test cases:
    - [x] Test if the response is empty data.
    - [x] Test the pagination of the responses from the API.
    - [x] Test the push to the database.
    - [x] Test the creation of stage_table.
- [x] Create the following integration test cases:
    - [x] Test the column length and renames of the dataset fields.
    - [x] Test if the response is non-empty that the db is non empty.
- [x] Run for one year interval 2018-07-05 to 2019-07-05.
- [ ] Design a second DAG to deliver custom data to data analyst team from data warehouse. Creates the following data model with star schema:
    - [x] Fact Table: key_id, date, serial_number, median_sales, avg_sales_ratio
    - [ ] Agent: serial_number, num_properties, YoY_e_profit, principal_town, second_town
    - [ ] Agent-property (Details from previous record): serial_number, property_id, how_long_owned, value_change_ratio, assessed_value_change_ratio, sale_amount_change_ratio.
    - [ ] Property: property_id, property_type, residencial_type, non_use_code, assesor_remarks, opm_remarks.    
    - [ ] Geolocation: property_id, location, town, state, country, address
    - [ ] Sales: serial_number, property_id, assessed_value, sale_amount, sales_ratio, YoY_change, MoM_change, is_first_entry
    - [ ] Relationships:
        - Fact table * : 1 Agent-property
        - Agent-property * : 1 Property
        - Fact table * : * Agent
        - Fact table * : 1 Geolocation
        - Fact table * : 1 Sales
- [ ] Design the executive & analytics dashboards where answer question like:
    - [ ] General questions:
        - [ ] YoY analysis property value: Avg of the properties value changes by town.
        - [ ] Value over time: Avg of the properties values over time by town.
        - [ ] Geographic location of the properties. Segments by property value 

    - [ ] Questions about serial number (Pick the 10th most profitable (w1 * number of properties + w2 * expected profits) agencies):  
        - [ ] Percentage of number of owned properties.
        - [ ] Percentage of expected profit.
        - [ ] Map > Distribution of the properties by town.  
        
    - Questions about property-sales (Filter by agency number):
        - [ ] Expected profits by property.
        - [ ] Gained value over time
        

- [ ] Run for the remain time interval to current day.
- [ ] Check the final results on dashboards.
- [ ] Write the summary of the project: technologies used, achievement, use cases in real-world scenario, data modeling approach.
- [x] Add to porfolio.


