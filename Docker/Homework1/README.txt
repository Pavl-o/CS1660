1. I was unable to access Jypyter through localhost on my machine no matter how many different ports and browsers I tried so I included the screenshot of me running the two commands on the cluster and it successfully starting jypyter in hopes of partial credit. I really did not know what else to try as the instructions were very limited.

2. I wasn't sure how to include the commands I used to launch my HelloWorld program as that shell session expired but here they are:

$ docker pull pvl0/hllwrld
$ docker tag pvl0/hllwrld gcr.io/skilful-scarab-327604/pvl0/hllwrld:latest
$ docker push gcr.io/skilful-scarab-327604/pvl0/hllwrld:latest