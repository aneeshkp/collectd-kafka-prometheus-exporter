from prometheus_client import start_http_server,CollectorRegistry
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
import json
import requests
import sys
import time
import collections
import random
from kafka import KafkaConsumer

class KafkaCollector(object):


   def __init__(self):
       self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',enable_auto_commit=True,auto_offset_reset='earliest',consumer_timeout_ms=1000)
       self.consumer.subscribe(['collectd-topic-x'])

   @staticmethod
   def _serialize_identifier(index, v):
       """Based of FORMAT_VL from collectd/src/daemon/common.h.
       The biggest difference is that we don't prepend the host and append the
       index of the value, and don't use slash.
       """
       return (v["plugin"] + ("-" + v["plugin_instance"]
                           if v["plugin_instance"] else "")
               + "@"
               + v["type"] + ("-" + v["type_instance"]
                           if v["type_instance"] else "")
               + "-" + str(index))

   def collect2(self):
	yield GaugeMetricFamily('my_gauge', 'Help text', value=7)
        c = CounterMetricFamily('my_counter_total', 'Help text', labels=['foo'])
        c.add_metric(['bar'], random.random())
        c.add_metric(['baz'], random.random())
        yield c

   def collect(self):
     consumer = KafkaConsumer(bootstrap_servers='localhost:9092',enable_auto_commit=True,auto_offset_reset='earliest',consumer_timeout_ms=1000)
     consumer.subscribe(['collectd-topic-x'])
     #consumer.seek(0,2)
     consumer.max_buffer_size=0
     print "calling collect"
     count=1
     for message in consumer:
         print("####################################\n")
         print message

         parsed_json=json.loads(message.value)
         host_id = "kafka:" + parsed_json[0]["host"].replace("/", "_")
         host=parsed_json[0]["host"]
         dataset=parsed_json[0]
         value_list=parsed_json[0]
         for index,item in enumerate(dataset["values"]):
           name=self.get_metric_family_name(value_list,dataset["dsnames"][index],dataset["dstypes"][index])
           metric=self.metric_family_create(name,dataset,value_list,dataset["dstypes"][index])
           data=self.getdata(dataset,value_list,index)
           metric.add_metric(data["labels"],data["values"])
           yield metric

         #metric= self.getmetrics(parsed_json[0],parsed_json[0])
         #metrics.append(metric)
         #print metric.type
         #break
         #count+=1
         #if count>=10:
         #   print count
         #   break






   # metric_family_name creates a metric family's name from a data source. This is
   # done in the same way as done by the "collectd_exporter" for best possible
   # compatibility. In essence, the plugin, type and data source name go in the
   # metric family name, while hostname, plugin instance and type instance go into
   # the labels of a metric.
   def get_metric_family_name(self, value_list,dsname,type):
    fields=[]


    if value_list["plugin"]!=value_list["type"]:
      fields.append(value_list["plugin"])

    fields.append(value_list["type"])

    if dsname !="value":
      fields.append(dsname)

    fields.append(value_list["host"])


    if type == "counter" or type =="derive":
       fields.append("total")

    return '_'.join(fields)

   def metric_family_get(self,dataset,value_list,dsname,dstype):
      metric_name=self.get_metric_family_name(value_list,dsname,dstype)

      #if metric_name==None
      return self.metric_family_create(metric_name,dataset,value_list,dstype)


   def metric_family_create(self,name,dataset,value_list,type):
      metricFamily=None

      if type == "gauge":
         metricFamily=GaugeMetricFamily(name,'')
      else:
         metricFamily=CounterMetricFamily(name,'')
      return metricFamily

   def getmetrics(self,dataset,value_list):
     metrics_type=()
     metrics={}
     name=self.get_metric_family_name(value_list,dataset["dsnames"][0],dataset["dstypes"][0])
     metric=self.metric_family_create(name,dataset,value_list,dataset["dstypes"][0])
     for index,item in enumerate(dataset["values"]):
        data=self.getdata(dataset,value_list,index)
        #print "dta=%s\n",data["labels"]
        # metric.add_metric(['foo'],index)
        metric.add_metric(data["labels"],data["values"])

     return metric
    # for index,item in enumerate(dataset["values"]):
    #    name=self.metric_family_get(dataset,value_list,dataset["dsnames"][index],dataset["dstypes"])
    #    if name not in metrics_type:
    #        metrics_type=metrics_type +(name,)
    #        family=self.metric_family_create(name,dataset,value_list,index)
    #        metrics[name]=family
    #        data=self.getdata(dataset,value_list,index)
    #        family.add_metric(data["labels"],data["values"])
    #    else:
    #        data=self.getdata(dataset,value_list,index)
    #        metrics[name].add_metric(data["labels"],data["values"])
    #       #for i in data:
    #        #   for j in i['values']:
    #              #      metrics[name].set(i['labels'], j)
    # return metrics

   def getdata(self,dataset,value_list,index):
	labels=[value_list["type"].encode("ascii","replace"),value_list["time"],value_list["interval"],
		value_list["plugin_instance"].encode("ascii","replace"),
                value_list["host"].encode("ascii","replace"),value_list["dsnames"][index].encode("ascii","replace") ]
        data={}
        #labels=[value_list["dsnames"][index].encode("ascii","replace")]
        data["labels"]=labels
        data["values"]=dataset["values"][index]
        return data


   def getdata2(self,dataset,value_list,index):
     data=()
     labels=[]
     if len(value_list["type_instance"])>0:
        if len(value_list["plugin_instance"])==0:
           labels[value_list["plugin"]]=value_list["type_instance"]


     labels["type"]=value_list["type"]
     if len(value_list["plugin_instance"])>0:
        labels[value_list["plugin"]]=value_list["plugin_instance"]
        labels["instance"]=value_list["host"]


     labels["dsname"]=value_list["dsnames"][index]

     #label.append("values"]=dataset["values"][index]
     data1={}
     data1["labels"]=labels
     data1["values"]=random.random() #dataset["values"][index]
     return data1


if __name__ == '__main__':
    start_http_server(int(sys.argv[1]))

    #MYREGISTRY = CollectorRegistry(auto_describe=False)
    REGISTRY.register(KafkaCollector())
    while True: time.sleep(1)
