This repository includes data transformation library, which is used throughout the experimenting phase, and the ML experiments themselves.

To start, one needs to create venv and install all the necessary libraries:

`conda create --name <env_name> --file requirements.txt`

After that, one needs to ensure, that the necessary data is in place (in PostgreSQL). After that the configuration should be provided in the root folder in a file called `config.ini`. Example for its strcture is show in `example_config.ini` file.

File for the classic ML models experiment can be found here: `ml/labevents_enhanced_study_classification.ipynb`

File for the transformers experiment (BERT and Temporal Fusion Transformer) can be found here: `ml/transformers_time_series_classification.ipynb`

File for the LSTM experiments can be found here: `ml/LSTM_with_original_chartevents.ipynb`