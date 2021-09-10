# speech-to-text-data-collection


Speech recognition technology allows for hands-free control of smartphones, speakers, and even vehicles in a wide variety of languages. Companies have moved towards the goal of
enabling machines to understand and respond to more and more of our verbalized commands. There are many matured speech recognition systems available, such as Google Assistant,
Amazon Alexa, and Apple’s Siri. However, all of those voice assistants work for limited languages only. The purpose of this week’s challenge is to build a data engineering
pipeline that allows recording millions of Amharic and Swahili speakers reading digital texts in-app and web platforms. By the end of this project, we should produce a tool that
can be deployed to process posting and receiving text and audio files from and into a data lake, apply transformation in a distributed manner, and load it into a warehouse in a
suitable format to train a speech-t0-text model

## Technologies used 
- 'Apache Kafka': To sequentially log streaming data into specific topics
Apache Airflow: To create,ocherstrate and monitor data workflows
S3 Buckets: For storing transformed streaming data

