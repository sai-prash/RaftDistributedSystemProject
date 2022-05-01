import json
import socket
import traceback
import time
import ast

# Wait following seconds below sending the controller request
time.sleep(5)

# Read Message Template
msg = json.load(open("Message.json"))

#Initialize Request to get the leader info:
sender = "Controller"
port = 5555
target="Node1"
msg['sender_name'] = sender
msg['request'] = "LEADER_INFO"
print(f"Request Created : {msg}")

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))



# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    responsedata,server=skt.recvfrom(1024)
    print(responsedata)
    response=responsedata.decode("UTF-8")
    response_dict=ast.literal_eval(response)
    print(response_dict)
    leader=response_dict["Value"]
    print(f"leader node is :{leader}")
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


time.sleep(2)
#Initialize Request to convert leader to follower:
sender = "Controller"
target = leader
port = 5555

# Request
msg['sender_name'] = sender
msg['request'] = "CONVERT_FOLLOWER"
print(f"Request Created : {msg}")

# # Socket Creation and Binding
# skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
# skt.bind((sender, port))

# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")



time.sleep(5)
#Initialize Request to timeout a node:
sender = "Controller"
target = leader
port = 5555

# Request
msg['sender_name'] = sender
msg['request'] = "TIMEOUT"
print(f"Request Created : {msg}")

# # Socket Creation and Binding
# skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
# skt.bind((sender, port))

# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


time.sleep(5)
target="Node1"
msg['sender_name'] = sender
msg['request'] = "LEADER_INFO"
print(f"Request Created : {msg}")

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))


# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


time.sleep(5)
#Initialize Request to timeout a node:
sender = "Controller"
target = leader
port = 5555

# Request
msg['sender_name'] = sender
msg['request'] = "SHUTDOWN"
print(f"Request Created : {msg}")

# # Socket Creation and Binding
# skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
# skt.bind((sender, port))

# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")
