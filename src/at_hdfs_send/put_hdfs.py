# -*- coding: utf-8 -*-
import hdfs
import os

#get all files in repository
repo="/home/fitec/code/data_send/data/"
files_send = os.listdir(repo)
print(files_send)

##connect to hdfs cluster, with address 
client=hdfs.InsecureClient("http://172.17.0.1:50070")

#list of reporsitory
print(client.list("/"))
 
#this function send file to hdfs 
def sendtohdfs(file_to_send):
    with open(repo+file_to_send) as reader, client.write('/data/'+file_to_send,overwrite=True) as writer:
        for line in reader:
            writer.write(line)



# send all files in repository to hdfs
for elt in files_send:
    sendtohdfs(elt)




#lecture du fichier, pour tester la presence et le contenu, vous pouvez le supprimer

with client.read("/data/Teletravail_regulier_en_2017_domicile_travail.csv") as reader:
    print(reader.read())




