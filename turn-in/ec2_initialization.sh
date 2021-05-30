#!/bin/bash
export AWS_ACCESS_KEY_ID=ASIASKM4G45MCNSYPAGD
export AWS_SECRET_ACCESS_KEY=O2nzu4GgPdREiODwaNuEFB6ORXZ2npzvWvghORvx
export AWS_SESSION_TOKEN=FwoGZXIvYXdzEOn//////////wEaDAraPHoTa5LUE1km+SKqAZnrVm0fMHG5gE/V7imcQ6A5Y+bkGaeKkfw7X1MrETCJP33cHSy4rw87FUBRSQ86glso95HB/HBnnQ/IqWw8CE3HROCYPNIgsqBJPcc3T8JMsw3lkh7st/5LdAkmw3InrXsugJdDBLsiLwyeiC7sLUIstw8erNhxf2bRmZ5iPzDeBG96sMEoBNTccaVK7lUqRpGYxJSvxx1kviSw9Uki5llGwa5AGzEgKGKDKLigy4UGMi31I2XrWwkU9wl83/nVfz+VnF1pKIBLcInQZsQGqBkSyX1zzDo4bWlH1Nm3Up0=

aws ec2 run-instances \
	--image-id ami-03d5c68bab01f3496 \
	--count 1 \
	--instance-type t2.xlarge \
	--key-name keypair \
	--security-group-ids sg-0d3f517cfb5a49814 \
	--subnet-id subnet-19ae2144 \
	--region us-west-2 \
	--placement "AvailabilityZone=us-west-2c" \
	--tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=feature_prep}]' \
	--user-data file://ec2_bootstrap.sh\
	--instance-initiated-shutdown-behavior terminate
