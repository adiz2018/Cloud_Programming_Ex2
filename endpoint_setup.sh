#!/bin/bash
sudo apt update
sudo apt install python3-pip -y
sudo apt install python3-flask -y
sudo git clone https://github.com/adiz2018/Cloud_Programming_Ex2.git
cd Cloud_Programming_Ex2
sudo pip3 install -r requirements.txt
# run app
python3 ./endpoint.py {0} {1}
