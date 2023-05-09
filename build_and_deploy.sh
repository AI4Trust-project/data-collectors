#!/bin/bash
cd hate_speech_detector
minikube image build -t hatespeech:latest .
kubectl apply -f hate_speech_detector.yaml
cd ..
cd unifier
minikube image build -t youtubeschema:latest .
kubectl apply -f unifier.yaml
cd ..
cd dynamicrouting
minikube image build -t dynamic-router:latest .
kubectl apply -f dynamicrouter.yaml
cd ..
cd thumbnaildownloader
minikube image build -t thumbnaildownloader:latest .
kubectl apply -f thumbnaildownloader.yaml
cd ..
cd pulsar-sink
minikube image build -t cloud-sink:latest .
kubectl apply -f cloud-sink.yaml
cd ..
cd youtube_collector
minikube image build -t collector:latest .
kubectl apply -f youtube_collector.yaml
