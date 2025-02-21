# buzzline-06-ruiz

## This project will work with 1,000 people data in csv and gather data for a visualizaitons with bar charts for sex as x and y categories, average age, sex counts,and top 10 occupations by sex.


## Task 1. Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first. 
Python 3.11 is required. 

## Task 2. Copy This Example Project and Rename

Once the tools are installed, copy/fork this project into your GitHub account
and create your own version of this project to run and experiment with.
Name it `buzzline-06-yourname` where yourname is something unique to you.
Follow the instructions in [FORK-THIS-REPO.md](https://github.com/denisecase/buzzline-01-case/blob/main/docs/FORK-THIS-REPO.md).
    

## Task 3. Manage Local Project Virtual Environment

Follow the instructions in [MANAGE-VENV.md](https://github.com/denisecase/buzzline-01-case/blob/main/docs/MANAGE-VENV.md) to:
1. Create your .venv
2. Activate .venv
3. Install the required dependencies using requirements.txt.

## Task 4. Start Zookeeper and Kafka (2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
2. Start Kafka ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

## Fist Terminal with WSL- Zookeeper
1. cd ~/kafka
chmod +x zookeeper-server-start.sh
bin/zookeeper-server-start.sh config/zookeeper.properties

## Second Terminal with WSL- Kafka
1. cd ~/kafka
chmod +x kafka-server-start.sh
bin/kafka-server-start.sh config/server.properties

## Third Terminal-Run Producer
1. py -3.11 -m venv .venv
.venv\Scripts\activate
2. py -m producers.csv_producer_ruiz

## Fourth Terminal-Run Consumer
1. py -3.11 -m venv .venv
.venv\Scripts\activate
2. py -m consumers.csv_consumer_ruiz
3. my terminal said alreay satisfied, if needed add to the virtual environmnet pip install matplotlib.
4. If gives error for no pyhon exe, can use the following: python.exe -m pip install --upgrade pip

## Results will be in Terminal and project_log.log.
## I have saved charts in the data folder of the 3 expected bar charts for average age by sex, sex counts, and Top 10 jobs by sex.
## Please note when running the consumer the charts do take a bit to come up.I notice you close one and the other will pop up. Then close the seond chart, and the third will come up.


## Later Work Sessions
When resuming work on this project:
1. Open the folder in VS Code. 
2. Start the Zookeeper service.
3. Start the Kafka service.
4. Activate your local project virtual environment (.env).

## Save Space
To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later. 
Managing Python virtual environments is a valuable skill. 

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.
