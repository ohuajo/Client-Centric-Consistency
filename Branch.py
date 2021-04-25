import example_pb2
import example_pb2_grpc
import ast
from concurrent import futures
import os
import json
import grpc
import time
import multiprocessing
import shutil

class Branch(example_pb2_grpc.RPCServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of id of other branches
        self.others = self.branches
        self.others.pop((self.id-1))
        # A list to carry the message recieved from Customer
        self.events = list()
        # customer id
        self.cust = 0
        # Writeset for enforcing client-centric consistency
        self.writeset = 0




    # This method accepts the request from Customer and generates what has to be added to the existing balance.
    # The variable known as self.money carries this amount that will be added to the balance.
    # If the request is a deposit, then self.money is assigned the money amount to be deposited.
    # If the request is a withrawal, self.money is assigned a negative value of the money amounth to be withdrawn.
    # Else self.money is assigned 0. 
    # Note that for easy of implementaion, query is regarded also as a deposit of $0
    def eventRequestExecute (self, x):
        self.events = (ast.literal_eval(x))
        self.cust = int(self.events[0])
        if self.events[1]["interface"] == 'withdraw':
            self.money = 0 - int(self.events[1]["money"])   
        elif self.events[1]["interface"] == 'deposit':
            self.money = int(self.events[1]["money"])
        else:
            self.money = 0
        self.writeset = self.events[2]
        self.workingWriteset = self.events[2]



    # This method handles the events in a branch that recieves a propagated message
    # The propagated message is the amount carried by variable self.money from the propagating branch as well as the writeset of 
    # This variable is the amount which could be positive (deposit) or negative (withdrawal) is added to the balance of the receiving branch
    # Also the writeset is increased by 1 to allow the relevant branch execute writset based event.
    # This result in a propagation of writes and writesets 
    def ClockUpdate(self, request, context):
        rcast = ast.literal_eval(request.propout)
        self.balance += int(rcast[0])
        self.writeset  = int(rcast[1]) + 1
        return example_pb2.ExamplePropIn(propin=str("done"))
    

    # This method handles the propagation of self.money (amount to be deposited or withdrawn) to other branches aside self.  
    # The propagating Branch send a message, awaits for the other Branches to recieve the messages and acknowledge reciept.
    # Then it updates it's own branch's balance.
    def propagateRequest(self):
        for x in self.others:
            sendit = str([self.money, self.writeset])
            trans_spec = 50047 + x
            clockit = sendit
            with grpc.insecure_channel('localhost:' + str(trans_spec)) as channel:
                stub = example_pb2_grpc.RPCStub(channel)
                response = stub.ClockUpdate(example_pb2.ExamplePropOut(propout=clockit))
        self.writeset += 1
        self.balance += self.money
         
        

    # This method puts it all together. It ensures that request from the client is appropriately executed 
    # the right message returned back to the Customer
    # This method also enforces client centric consistencies using the writeset, while lool and time.sleep
    # If the writeset and the incoming message writeset from the customer is not the same, the branch waits for 0.1 seconds and check again. 
    # Once the message writeset is same with the branch writeset, the execution will now proceed. In essence the first writeset to be excuted and propagated is 0. 
    # Other event of the same process will wait until the increment is made to 1 and then writeset 1 can be able to execute. 
    # This continues until executions are done.
    def MsgDelivery(self,request, context):
        checkevent = (ast.literal_eval(request.inmessage))
        while int(checkevent[2]) != int(self.writeset):
            time.sleep(0.1)
        response = example_pb2.ExampleReply()
        self.eventRequestExecute(request.inmessage)
        self.propagateRequest()
        response.outmessage  = str([{"id": self.cust, "balance": self.balance, "writeset": self.workingWriteset}])
        return response

# This function ensures the servers are actively listening to the appropriate ports.
def creatServer(id, balance, branches):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    example_pb2_grpc.add_RPCServicer_to_server(Branch(id, balance, branches), server)
    trans_spec = 50047 + int(id)
    server.add_insecure_port('[::]:'+ str(trans_spec))
    server.start()
    server.wait_for_termination()



if __name__ == "__main__":
    lock = multiprocessing.Lock()
    # Reading of input file
    with open("input.json") as example_file:
        example_data = json.load(example_file)
        branches = []
        for i in range(len(example_data)):
            if example_data[i]['type'] == 'bank':
                branches.append(int(example_data[i]['id']))
        # this ensures multiprocessing is executed.
        processes = []
        for i in range(len(example_data)):
            if example_data[i]['type'] == 'bank':
                p1 = multiprocessing.Process(target=creatServer, args=(int(example_data[i]['id']), int(example_data[i]['balance']), branches))
                processes.append(p1)
                p1.start()
        for p1 in processes:
            p1.join()