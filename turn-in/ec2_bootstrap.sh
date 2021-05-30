#!/bin/bash
cd
apt-get update
yes | apt-get install awscli
yes | apt-get install python3-pip
export AWS_ACCESS_KEY_ID=ASIASKM4G45MCNSYPAGD
export AWS_SECRET_ACCESS_KEY=O2nzu4GgPdREiODwaNuEFB6ORXZ2npzvWvghORvx
export AWS_SESSION_TOKEN=FwoGZXIvYXdzEOn//////////wEaDAraPHoTa5LUE1km+SKqAZnrVm0fMHG5gE/V7imcQ6A5Y+bkGaeKkfw7X1MrETCJP33cHSy4rw87FUBRSQ86glso95HB/HBnnQ/IqWw8CE3HROCYPNIgsqBJPcc3T8JMsw3lkh7st/5LdAkmw3InrXsugJdDBLsiLwyeiC7sLUIstw8erNhxf2bRmZ5iPzDeBG96sMEoBNTccaVK7lUqRpGYxJSvxx1kviSw9Uki5llGwa5AGzEgKGKDKLigy4UGMi31I2XrWwkU9wl83/nVfz+VnF1pKIBLcInQZsQGqBkSyX1zzDo4bWlH1Nm3Up0=
aws s3 cp s3://ds102-dsc01-scratch/requirements.txt requirements.txt
pip install -r requirements.txt
pip3 install --upgrade awscli
aws s3 cp s3://ds102-dsc01-scratch/feature_prep.py feature_prep.py
aws s3 cp s3://ds102-dsc01-scratch/orig_read_helper.csv orig_read_helper.csv
aws s3 cp s3://ds102-dsc01-scratch/orig orig --recursive
python3 feature_prep.py orig
shutdown -h now