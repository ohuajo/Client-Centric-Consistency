import grpc
import example_pb2
import example_pb2_grpc
import ast
import json
import os, signal
from concurrent import futures
import subprocess
import re
from itertools import chain
from operator import itemgetter
import shutil
import multiprocessing
import numpy as np

class Customer:
    def __init__(self, id, dest, events, writeset):
        # unique ID of the Customer
        self.id = id
        # unique ID of the destination branch
        self.dest = dest
        # events from the input
        self.events = events
        # Writeset with which branch will use in enforcing client-centric consistency
        self.writeset = writeset

    # TODO: students are expected to create the Customer stub
    def createStub(self):
        trans_spec = 50047 + self.dest
        channel = grpc.insecure_channel('localhost:' + str(trans_spec))
        # create a stub (client)
        self.stub = example_pb2_grpc.RPCStub(channel)
    
    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        # create a valid request message
        validrequest = str([self.id,self.events, self.writeset])
        string = example_pb2.ExampleRequest(inmessage=validrequest)
        # make the call
        response = self.stub.MsgDelivery(string)
        return response.outmessage

# Function for multiprocessing client's request.
def setupRequest(x):
    customerrun = Customer(x[0], x[1], x[2], x[3])
    customerrun.createStub()
    return customerrun.executeEvents()

        
if __name__ == "__main__":
    with open("input.json") as example_file:
        example_data = json.load(example_file)
        id_list = []
        request_list = []
        for i in range(len(example_data)):
            if example_data[i]['type'] == 'customer':
                writesetid = 0
                for x in example_data[i]['events']:
                    id_list.append(int(x["dest"]))
                    request1 = [example_data[i]['id'], int(x["dest"]), x, writesetid]
                    request_list.append(request1)
                    writesetid += 1
        p = multiprocessing.Pool(processes=20)
        data = p.map(setupRequest, request_list)
        p.close()
        resultFromBranch = ast.literal_eval(data[-1])
        resultFromBranch[0].pop("writeset")                    
        # Creating of output file
        with open("output.json", "w") as file_object:
            file_object.write(str(resultFromBranch))
        # Closing of approriate server after writing to output file.
        serverList = np.unique(id_list)
        for i in serverList:
            pidX1 = 50047 + i
            pidX2 = str(pidX1)+'/tcp'
            pidX3 = subprocess.run(['fuser', '-k', pidX2], capture_output=True)