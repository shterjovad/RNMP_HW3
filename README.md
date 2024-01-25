## RUN THE SETUP
docker-compose up -d


## Training Model
please open the notebook file from browser
http://localhost:8888 and work on the notebook file


## prediction service
prediction service starts running with the docker-compose and writes to the topic.

to produce the online.csv to the topic, please run the following command
```bash
python3 producer.py
```