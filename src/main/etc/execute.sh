#!/usr/bin/env bash

echo "start task 1.2"
yarn jar ~/cloud-capstone/target/cloud-capstone-jar-with-dependencies.jar org.buldakov.batch.task1_2.JobRunner /capstone/ontime_input/* /capstone/task12
echo "done!"

echo "start task 1.3"
yarn jar ~/cloud-capstone/target/cloud-capstone-jar-with-dependencies.jar org.buldakov.batch.task1_3.JobRunner /capstone/ontime_input/* /capstone/task13
echo "done!"

echo "start task 2.1"
yarn jar ~/cloud-capstone/target/cloud-capstone-jar-with-dependencies.jar org.buldakov.batch.task2_1.JobRunner /capstone/ontime_input/*
echo "done!"

echo "start task 2.3"
yarn jar ~/cloud-capstone/target/cloud-capstone-jar-with-dependencies.jar org.buldakov.batch.task2_3.JobRunner /capstone/ontime_input/*
echo "done!"

echo "start task 2.4"
yarn jar ~/cloud-capstone/target/cloud-capstone-jar-with-dependencies.jar org.buldakov.batch.task2_4.JobRunner /capstone/ontime_input/*
echo "done!"

echo "start task 3.1"
yarn jar ~/cloud-capstone/target/cloud-capstone-jar-with-dependencies.jar org.buldakov.batch.task3_1.JobRunner /capstone/ontime_input/*
echo "done!"

echo "start task 3.2"
yarn jar ~/cloud-capstone/target/cloud-capstone-jar-with-dependencies.jar org.buldakov.batch.task3_2.JobRunner /capstone/ontime_input/Cleaned_On_Time_On_Time_Performance_2008*
echo "done!"