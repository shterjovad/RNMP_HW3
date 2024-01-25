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

## Project overview

i use 3 models to train during offline phase and yet:

1.Random Forest
2.Logistic Regression
3.Gradient Boost Trees

For each i use ParamGridBuilder for tuning maxDepth 5-10, maxIter 10-10, and 10-20 accordingly and afterwards i use CrossValidator.

I am evaluating each of the models using the metric f1 score.

There are slightly changes in the results of the models and i chose to work with the best f1 of them-Linear Regression with 0.8112380324329342.

Producer script reads record by record from online.csv and sends it to Kafka topic health_data in json format.

During the online phase my script prediction_job.py now Spark is reading directly from Kafka from the earliest offset of health_data topic.

After defining the columns in a schema and then droping them the ones that do not add a value to the model, i read the data in json format parsing back to the schema.

After loading the model, just like during the training phase, i use the same function VectorAssembler and using transform i do the same transformations as before.

Then i make predictions, extract the individual features from them and write back the results in a structured streaming manner.
