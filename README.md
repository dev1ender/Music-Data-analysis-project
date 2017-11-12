# Music Data analysis project
## Intro:
Project explanation is provided in project pdf file.what we are going to do in this project.I was using cloudera vm for this project.

## Directory structre
#### data 
It contain two subdirectory one is web which contain data in xml file format and other is mod subdirectory which contain data in text format which are going to be analyzed.
#### lib
It contain piggybank jar file used while using pig 
#### logs 
This folder will be created automatically when you will run the project it will used to store the logs generated. also the batch id.
#### lookupfiles
This folder contain all the look up files which will help to enrich the data that is missing
#### scripts
This folder conatin all scripts file which are going to run when executing the project.

## Special NOTE:
###### Tip 1:
This is project is made in cloudera vm so adjust all the path according to your system before running the project.
###### Tip 2:
If you are not using cloudera vm then please modify the the start-dameons file at the end add commands to run hadoop and spark cluster and all the other services requiered in the project.
###### Tip 3:
This is project will some of the directory automatically  which will have intermediate results of the project so do not misbehave with them.
###### Tip 4:
spark in cloudera vm only accepts file from the hdfs file structre so before running the project adjust the file paths in data_analysis.scala file to local file system if you are using other system or if you are using cloudera use flume to move the intermideate directory files to hdfs that will be genrated in the process.

## How to run the project

#### STEP 1
change permision of the scripts folder so that it can be easly accessable everywhere, by using the following command `chmod 744 <path>`
and running the mysql service `sudo service mysqld start`

#### STEP 2
now simplly run the wrapper file it will automattically run all the files by using the command `sh <path>wapper.sh`

